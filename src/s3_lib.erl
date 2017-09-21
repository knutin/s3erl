%%
%% Blocking stateless library functions for working with Amazon S3.
%%
-module(s3_lib).

%% API
-export([get/4, put/6, delete/3, list/5]).
-export([signed_url/4, signed_url/5]).

-include_lib("xmerl/include/xmerl.hrl").
-include("../include/s3.hrl").

%%
%% API
%%

-spec get(#config{}, bucket(), key(), [header()]) ->
                 {ok, body()} | {error, any()}.
get(Config, Bucket, Key, Headers) ->
    do_get(Config, Bucket, Key, Headers).

-spec put(#config{}, bucket(), key(), body(), contenttype(), [header()]) ->
    {ok, etag()} | {error, any()}.
put(Config, Bucket, Key, Value, ContentType, Headers) ->
    MD5 = md5_hash_string(Value),
    NewHeaders = [{"Content-Type", ContentType},
                  {"Content-MD5", MD5}
                  | Headers],
    do_put(Config, Bucket, Key, Value, NewHeaders).

delete(Config, Bucket, Key) ->
    do_delete(Config, Bucket, Key).

list(Config, Bucket, Prefix, MaxKeys, Marker) ->
    Key = ["?", "prefix=", Prefix, "&", "max-keys=", MaxKeys, "&marker=", Marker],
    case request(Config, get, Bucket, lists:flatten(Key), [], <<>>) of
        {ok, _Headers, Body} ->
            {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Body)),
            Keys = lists:map(fun (#xmlText{value = K}) -> list_to_binary(K) end,
                             xmerl_xpath:string(
                               "/ListBucketResult/Contents/Key/text()", XmlDoc)),

            {ok, Keys};
        {ok, not_found} ->
            {ok, not_found};
        {error, _} = Error ->
            Error
    end.

signed_url(Config, Bucket, Key, Expires) ->
    signed_url(Config, Bucket, Key, get, Expires).

signed_url(Config, Bucket, Key, Method, Expires) ->
    Signature = sign(Config#config.secret_access_key,
                     stringToSign(Method, "", integer_to_list(Expires),
                                  Bucket, Key, "")),
    Url = build_full_url(Config#config.endpoint, Bucket, Key),
    SignedUrl = [Url, "?", "AWSAccessKeyId=", Config#config.access_key, "&",
                 "Expires=", integer_to_list(Expires), "&", "Signature=",
                 http_uri:encode(binary_to_list(Signature))],
    lists:flatten(SignedUrl).


%%
%% INTERNAL HELPERS
%%

do_put(Config, Bucket, Key, Value, Headers) ->
    case request(Config, put, Bucket, Key, Headers, Value) of
        {ok, RespHeaders, Body} ->
            case lists:keyfind("Etag", 1, RespHeaders) of
                {"Etag", Etag} ->
                    %% for objects
                    {ok, Etag};
                false when Key == "" andalso Value == "" ->
                    %% for bucket
                    ok;
                false when Value == "" orelse Value == <<>> ->
                    %% for bucket-to-bucket copy
                    {ok, parseCopyXml(Body)}
            end;
        {ok, not_found} -> %% eg. bucket doesn't exist.
            {ok, not_found};
        {error, _} = Error ->
            Error
    end.

do_get(Config, Bucket, Key, Headers) ->
    case request(Config, get, Bucket, Key, Headers, <<>>) of
        {ok, ResponseHeaders, Body} ->
            if Config#config.return_headers ->
                    {ok, ResponseHeaders, Body};
               true ->
                    {ok, Body}
            end;
        {ok, not_found} ->
            {ok, not_found};
        Error ->
            Error
    end.

do_delete(Config, Bucket, Key) ->
    request(Config, delete, Bucket, Key, [], <<>>).


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

build_host(Bucket) ->
    [Bucket, ".s3.amazonaws.com"].

build_host(Endpoint, Bucket) ->
    [Endpoint, "/", Bucket].

build_url(undefined, Bucket, Path) ->
    lists:flatten(["http://", build_host(Bucket), "/", Path]);
build_url(Endpoint, _Bucket, Path) ->
    lists:flatten(["http://", Endpoint, "/", Path]).

build_full_url(undefined, Bucket, Path) ->
    lists:flatten(["http://", build_host(Bucket), "/", Path]);
build_full_url(Endpoint, Bucket, Path) ->
    lists:flatten(["http://", build_host(Endpoint, Bucket), "/", Path]).

request(Config, Method, Bucket, Path, Headers, Body) ->
    Date = httpd_util:rfc1123_date(),
    Url = build_url(Config#config.endpoint, Bucket, Path),
    ContentMD5 = case lists:keyfind("Content-MD5", 1, Headers) of
                     {_, MD5} ->
                         MD5;
                     false ->
                         ""
                 end,
    Signature = sign(Config#config.secret_access_key,
                     stringToSign(Method, ContentMD5,
                                  Date, Bucket, Path, Headers)),

    Auth = ["AWS ", Config#config.access_key, ":", Signature],
    FullHeaders = [{"Authorization", Auth},
                   {"Host", build_host(Bucket)},
                   {"Date", Date},
                   {"Connection", "keep-alive"}
                   | Headers],
    Options = [{max_connections, Config#config.max_concurrency}],
    do_request(Url, Method, FullHeaders, Body, Config#config.timeout, Options).

do_request(Url, Method, Headers, Body, Timeout, Options) ->
    case lhttpc:request(Url, Method, Headers, Body, Timeout, Options) of
        {ok, {{200, _}, ResponseHeaders, ResponseBody}} ->
            {ok, ResponseHeaders, ResponseBody};
        {ok, {{204, "No Content" ++ _}, _, _}} ->
            {ok, not_found};
        {ok, {{307, "Temporary Redirect" ++ _}, ResponseHeaders, _ResponseBody}} ->
            {"Location", Location} = lists:keyfind("Location", 1, ResponseHeaders),
            do_request(Location, Method, Headers, Body, Timeout, Options);
        {ok, {{404, "Not Found" ++ _}, _, _}} ->
            {ok, not_found};
        {ok, {Code, _ResponseHeaders, <<>>}} ->
            {error, Code};
        {ok, {_Code, _ResponseHeaders, _ResponseBody} = ErrorResp} ->
            handle_error_response(ErrorResp);
        {error, Reason} ->
            {error, Reason}
    end.

handle_error_response({Code, _ResponseHeaders, ResponseBody}) ->
    {ErrText, Explanation} = parseErrorXml(ResponseBody),
    case {Code, ErrText} of
        {{400, _}, "BadDigest"} ->
            {error, bad_digest};
        _ ->
            {error, {ErrText, Explanation}}
    end.

parseErrorXml(Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Xml)),
    [#xmlText{value=ErrorCode}] = xmerl_xpath:string("/Error/Code/text()", XmlDoc),
    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("/Error/Message/text()",
                                                        XmlDoc),
    {ErrorCode, ErrorMessage}.


parseCopyXml(Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Xml)),
    %% xmerl doesn't parse &quot; escape character very well
    case xmerl_xpath:string("/CopyObjectResult/ETag/text()", XmlDoc) of
        [#xmlText{value=Etag}, #xmlText{value="\""}] -> Etag ++ "\"";
        [#xmlText{value=Etag}] -> Etag
    end.


%%
%% Signing
%%

is_amz_header(<<"x-amz-", _/binary>>) -> true; %% this is not working.
is_amz_header("x-amz-"++ _)           -> true;
is_amz_header(_)                      -> false.

canonicalizedAmzHeaders("") -> "";
canonicalizedAmzHeaders(AllHeaders) ->
    AmzHeaders = [{string:to_lower(K),V} || {K,V} <- AllHeaders, is_amz_header(K)],
    Strings = lists:map(
                fun s3util:join/1,
                s3util:collapse(
                  lists:keysort(1, AmzHeaders) ) ),
    s3util:string_join(lists:map( fun (S) -> S ++ "\n" end, Strings), "").

canonicalizedResource("", "")       -> "/";
canonicalizedResource(Bucket, "")   -> ["/", Bucket, "/"];
canonicalizedResource(Bucket, Path) when is_list(Path) ->
    canonicalizedResource(Bucket, list_to_binary(Path));
canonicalizedResource(Bucket, Path) ->
    case binary:split(Path, <<"?">>) of
        [URL, _SubResource] ->
            %% TODO: Possible include the sub resource if it should be
            %% included
            ["/", Bucket, "/", URL];
        [URL] ->
            ["/", Bucket, "/", URL]
    end.

stringToSign(Verb, ContentMD5 = "", Date, Bucket = "", Path,
             OriginalHeaders = "") ->
    VerbString = string:to_upper(atom_to_list(Verb)),
    Parts = [VerbString, ContentMD5, "", Date,
             canonicalizedAmzHeaders(OriginalHeaders)],
    [s3util:string_join(Parts, "\n"), canonicalizedResource(Bucket, Path)];
stringToSign(Verb, ContentMD5, Date, Bucket, Path, OriginalHeaders) ->
    VerbString = string:to_upper(atom_to_list(Verb)),
    ContentType = proplists:get_value("Content-Type", OriginalHeaders, ""),
    Parts = [VerbString, ContentMD5, ContentType, Date,
             canonicalizedAmzHeaders(OriginalHeaders)],
    [s3util:string_join(Parts, "\n"), canonicalizedResource(Bucket, Path)].

sign(Key,Data) ->
    base64:encode(crypto:hmac(sha, Key, lists:flatten(Data))).

%%
%% MD5
%%

md5_hash_string(Data) ->
    base64:encode_to_string(erlang:md5(Data)).
