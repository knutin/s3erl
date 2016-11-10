%% @doc gen_server wrapping calls to s3. Contains configuration in
%% state, isolates caller from library failure, controls concurrency,
%% manages retries.
-module(s3_server).
-behaviour(gen_server).

-include("../include/s3.hrl").


%% API
-export([start_link/1, reload_config/1, get_config/0, get_stats/0, stop/0,
         get_request_cost/0]).
-export([default_max_concurrency_cb/1,
         default_retry_cb/2,
         default_post_request_cb/3]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(counters, {puts = 0, gets = 0, deletes = 0}).
-record(state, {config, workers, counters}).


%%
%% API
%%

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

reload_config(Config) ->
    gen_server:call(?MODULE, {reload_config, Config}).

get_stats() ->
    gen_server:call(?MODULE, get_stats).

get_request_cost() ->
    {ok, Stats} = get_stats(),
    GetCost = proplists:get_value(gets, Stats) / 1000000,
    PutCost = proplists:get_value(gets, Stats) / 100000,
    [{gets, GetCost}, {puts, PutCost}, {total, GetCost + PutCost}].

stop() ->
    gen_server:call(?MODULE, stop).

get_config() ->
    gen_server:call(?MODULE, get_config).


%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    process_flag(trap_exit, true),

    {ok, #state{config = create_config(Config), workers = [],
                counters = #counters{}}}.

handle_call({request, Req}, From, #state{config = C} = State)
  when length(State#state.workers) < C#config.max_concurrency ->
    WorkerPid =
        spawn_link(fun() ->
                           gen_server:reply(From, handle_request(Req, C))
                   end),
    NewState = State#state{workers = [{WorkerPid, From} | State#state.workers],
                           counters = update_counters(Req, State#state.counters)},
    {noreply, NewState};

handle_call({request, _}, _From, #state{config = C} = State)
  when length(State#state.workers) >= C#config.max_concurrency ->
    (C#config.max_concurrency_cb)(C#config.max_concurrency),
    {reply, {error, max_concurrency}, State};

handle_call(get_num_workers, _From, #state{workers = Workers} = State) ->
    {reply, length(Workers), State};

handle_call(get_stats, _From, #state{workers = Workers, counters = C} = State) ->
    Stats = [{puts, C#counters.puts},
             {gets, C#counters.gets},
             {deletes, C#counters.deletes},
             {num_workers, length(Workers)}],
    {reply, {ok, Stats}, State};

handle_call({reload_config, Config}, _From, State) ->
    {reply, ok, State#state{config = create_config(Config)}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(get_config, _From, #state{config = C} = State) ->
    {reply, {ok, C}, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    case lists:keysearch(Pid, 1, State#state.workers) of
        {value, {Pid, From}} ->
            case Reason of
                normal ->
                    ok;
                _ ->
                    error_logger:info_msg(
                      "s3 worker ~p exit because ~p. sending reply to ~p~n",
                      [Pid, Reason, From]),
                    gen_server:reply(From, {worker_exit, Reason})
            end,
            NewWorkers = lists:keydelete(Pid, 1, State#state.workers),
            {noreply, State#state{workers = NewWorkers}};
        false ->
            error_logger:info_msg("ignored down message~n"),
            {noreply, State}
    end;

handle_info(Info, State) ->
    error_logger:info_msg("ignored: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

v(Key, Data) ->
    proplists:get_value(Key, Data).

v(Key, Data, Default) ->
    proplists:get_value(Key, Data, Default).

create_config(Config) ->
    AccessKey        = v(access_key, Config),
    SecretAccessKey  = v(secret_access_key, Config),
    Endpoint         = v(endpoint, Config),

    Timeout          = v(timeout, Config, 1500),
    RetryCallback    = v(retry_callback, Config,
                         fun ?MODULE:default_retry_cb/2),
    MaxRetries       = v(max_retries, Config, 3),
    RetryDelay       = v(retry_delay, Config, 500),
    MaxConcurrency   = v(max_concurrency, Config, 50),
    MaxConcurrencyCB = v(max_concurrency_callback, Config,
                         fun ?MODULE:default_max_concurrency_cb/1),
    PostRequestCB    = v(post_request_callback, Config,
                         fun ?MODULE:default_post_request_cb/3),
    ReturnHeaders    = v(return_headers, Config, false),

    #config{access_key         = AccessKey,
            secret_access_key  = SecretAccessKey,
            endpoint           = Endpoint,
            timeout            = Timeout,
            retry_callback     = RetryCallback,
            max_retries        = MaxRetries,
            retry_delay        = RetryDelay,
            max_concurrency    = MaxConcurrency,
            max_concurrency_cb = MaxConcurrencyCB,
            post_request_cb    = PostRequestCB,
            return_headers     = ReturnHeaders}.

%% @doc: Executes the given request, will retry if request failed
handle_request(Req, C) ->
    StartTs = os:timestamp(),
    handle_request(Req, C, StartTs, 0).

handle_request(Req, C, StartTs, Attempts) ->
    case catch execute_request(Req, C) of
        %% Continue trying if we have connection related errors
        {error, Reason} when Attempts < C#config.max_retries andalso
                             (Reason =:= connect_timeout orelse
                              Reason =:= connection_closed orelse
                              Reason =:= timeout) ->
            catch (C#config.retry_callback)(Reason, Attempts),
            timer:sleep(C#config.retry_delay),
            handle_request(Req, C, StartTs, Attempts + 1);

        {'EXIT', {econnrefused, _}} when Attempts < C#config.max_retries ->
            catch (C#config.retry_callback)(econnrefused, Attempts),
            timer:sleep(C#config.retry_delay),
            handle_request(Req, C, StartTs, Attempts + 1);

        {error, Error} when Attempts < C#config.max_retries ->
            Retry = case Error of
                        {"InternalError", _} -> true;
                        {503, "Service Unavailable"} -> true;
                        {"SlowDown", _} -> true;
                        _ -> false
                    end,
            case Retry of
                true ->
                    catch (C#config.retry_callback)(internal_error, Attempts),
                    timer:sleep(C#config.retry_delay),
                    handle_request(Req, C, StartTs, Attempts + 1);
                false ->
                    EndTs = os:timestamp(),
                    catch (C#config.post_request_cb)(Req, {error, Error},
                                                     timer:now_diff(EndTs, StartTs)),
                    {error, Error}
            end;

        Res ->
            EndTs = os:timestamp(),
            catch (C#config.post_request_cb)(Req, Res, timer:now_diff(EndTs, StartTs)),
            Res
    end.

execute_request({get, Bucket, Key, Headers}, C) ->
    s3_lib:get(C, Bucket, Key, Headers);
execute_request({put, Bucket, Key, Value, ContentType, Headers}, C) ->
    s3_lib:put(C, Bucket, Key, Value, ContentType, Headers);
execute_request({delete, Bucket, Key}, C) ->
    s3_lib:delete(C, Bucket, Key);
execute_request({list, Bucket, Prefix, MaxKeys, Marker}, C) ->
    s3_lib:list(C, Bucket, Prefix, MaxKeys, Marker);
execute_request({signed_url, Bucket, Key, Expires}, C) ->
    s3_lib:signed_url(C, Bucket, Key, Expires);
execute_request({signed_url, Bucket, Key, Method, Expires}, C) ->
    s3_lib:signed_url(C, Bucket, Key, Method, Expires).



request_method({get, _, _, _})        -> get;
request_method({put, _, _, _, _, _})  -> put;
request_method({delete, _, _})        -> delete;
request_method({list, _, _, _, _})    -> get;
request_method({signed_url, _, _, _}) -> ignore.


update_counters(Req, Cs) ->
    case request_method(Req) of
        get    -> Cs#counters{gets = Cs#counters.gets + 1};
        put    -> Cs#counters{puts = Cs#counters.puts + 1};
        delete -> Cs#counters{deletes = Cs#counters.deletes + 1};
        ignore -> Cs
    end.

default_max_concurrency_cb(_)    -> ok.
default_retry_cb(_, _)           -> ok.
default_post_request_cb(_, _, _) -> ok.
