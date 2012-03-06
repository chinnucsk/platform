-module(etl1).

-author("hejin-2011-03-28").

-behaviour(gen_server).

%% Network Interface callback functions
-export([start_link/1, start_link/2,
        register_callback/1, register_callback/2,
        set_tl1/1,
        get_tl1/0, get_tl1_req/0,
        input/2, input/3,
        input_group/2, input_group/3,
        input_asyn/2, input_asyn/3
     ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).


-include("elog.hrl").
-include("tl1.hrl").

-define(RETRIES, 2).

-define(REQ_TIMEOUT, 60000).

-define(CALL_TIMEOUT, 300000).

-import(extbif, [to_list/1, to_binary/1, to_integer/1]).

-record(request,  {id, type, ems, data, ref, timeout, time, from}).

-record(state, {tl1_tcp, req_id=0, req_type, callback=[]}).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
start_link(Tl1Options) ->
    ?INFO("start etl1....~p",[Tl1Options]),
	gen_server:start_link({local, ?MODULE},?MODULE, [Tl1Options], []).

start_link(Name, Tl1Options) ->
    ?INFO("start etl1....~p",[Tl1Options]),
	gen_server:start_link({local, Name},?MODULE, [Tl1Options], []).

register_callback(Callback) ->
    gen_server:call(?MODULE, {callback, Callback}).

register_callback(Pid, Callback) ->
    gen_server:call(Pid, {callback, Callback}).

get_tl1() ->
    gen_server:call(?MODULE, get_tl1, ?CALL_TIMEOUT).


get_tl1_req() ->
    [{tl1_timeout, ets:info(tl1_request_timeout, size)}, {tl1_table, ets:info(tl1_request_table, size)}].


set_tl1(Tl1Info) ->
    ?WARNING("set tl1 info....~p",[Tl1Info]),
    gen_server:call(?MODULE, {set_tl1, Tl1Info}, ?CALL_TIMEOUT).

%%Cmd = LST-ONUSTATE::OLTID=${oltid},PORTID=${portid}:CTAG::;
input(Type, Cmd) ->
    input(Type, Cmd, ?REQ_TIMEOUT).


input(Type, Cmd, Timeout) when is_tuple(Type)->
    case gen_server:call(?MODULE, {sync_input, self(), Type, Cmd, Timeout}, ?CALL_TIMEOUT) of
        {ok, {_CompCode, Data}, _} ->
            %Data :{ok , [Values]} | {ok, [[{en, En},{endesc, Endesc}]]}
            %send after : {error, {tl1_cmd_error, [{en, En},{endesc, Endesc},{reason, Reason}]}}
            Data;
        {error, Reason} ->
            %send before: {error, {invalid_request, Req}} | {error, no_ctag} | {error, {no_type, Type}}
            %             {error, {'EXIT',Reason }} | {error, {conn_failed, ConnState, Host, Port}}
            %sending    : {error, {tcp_send_error, Reason}} | {error, {tcp_send_exception, Error}}
            %send after : {error, {tl1_timeout, Data}}
            {error, Reason};
        Error ->
            %{'EXIT',{badarith,_}}
            {error, Error}
	end;
input(Type, Cmd, Timeout) ->
    input({Type, ""}, Cmd, Timeout).

input_group(Type, Cmd) ->
    input_group(Type, Cmd, ?REQ_TIMEOUT).

input_group(Type, Cmd, Timeout) ->
	case  input(Type, Cmd, Timeout) of
        {ok, [Data1|_]} ->
            ?INFO("get cmd:~p, data:~p",[Cmd, Data1]),
            {ok, Data1};
        Other ->
            Other
    end.

input_asyn(Type, Cmd) ->
    gen_server:get_cast(?MODULE, {asyn_input, Type, Cmd}).

input_asyn(Pid, Type, Cmd) ->
    gen_server:get_cast(Pid, {asyn_input, Type, Cmd}).

%%%-------------------------------------------------------------------
%%% Callback functions from gen_server
%%%-------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%%--------------------------------------------------------------------
init([Tl1Options]) ->
        ?INFO("start etl1....~p",[Tl1Options]),
    case (catch do_init(Tl1Options)) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Pids} ->
            ?INFO("get tl1_tcp :~p", [Pids]),
            {ok, #state{tl1_tcp = Pids}}
    end.

do_init(Tl1Options) ->
    process_flag(trap_exit, true),
    ets:new(tl1_request_table, [set, named_table, protected, {keypos, #request.id}]),
    ets:new(tl1_request_timeout, [set, named_table, protected, {keypos, #request.id}]),
    Pids = connect_tl1(Tl1Options),
    {ok, Pids}.

connect_tl1(Tl1Infos) ->
    Pids = lists:map(fun(Tl1Info) ->
        do_connect(Tl1Info)
    end, Tl1Infos),
    lists:flatten(Pids).

do_connect(Tl1Info) ->
    ?INFO("get tl1 info:~p", [Tl1Info]),
    Type = proplists:get_value(manu, Tl1Info),
    CityId = proplists:get_value(cityid, Tl1Info, <<"">>),
    case  do_connect2(Tl1Info) of
        {ok, Pid} ->
            {{to_list(Type), to_list(CityId)}, Pid};
        {error, Error} ->
            ?ERROR("get tcp error: ~p, ~p", [Error, Tl1Info]),
            []
     end.

do_connect2(Tl1Info) ->
    case proplists:get_value(id, Tl1Info, false) of
        false -> 
            etl1_tcp:start_link(self(), Tl1Info);
        Id ->
            etl1_tcp:start_link(self(), list_to_atom("etl1_tcp_" ++ to_list(Id)), Tl1Info)
    end.

%%--------------------------------------------------------------------
%% Func: handle_call/3
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_call({callback, {Name, Pid}}, _From, #state{callback = Pids} = State) ->
    NewCall = lists:keystore(Name, 1, Pids, {Name, Pid}),
    {reply, ok, State#state{callback = NewCall}};

handle_call(get_tl1, _From, #state{tl1_tcp = Pids} = State) ->
    Result = lists:map(fun({_Type, Pid}) ->
        {ok, TcpState} = etl1_tcp:get_status(Pid),
        TcpState
    end, Pids),
    {reply, {ok, Result}, State};

handle_call({set_tl1, Tl1Info}, _From, #state{tl1_tcp = Pids} = State) ->
    NewPid = do_connect(Tl1Info),
    NewState = State#state{tl1_tcp = [NewPid|Pids]},
    {reply, ok, NewState};

handle_call({sync_input, Send, Type, Cmd, Timeout}, From, #state{tl1_tcp = Pids} = State) ->
    ?INFO("handle_call,Pid:~p,from:~p, Cmd,~p", [{Send, node(Send)}, From, Cmd]),
    case get_tl1_tcp(Type, Pids) of
        [] ->
            ?ERROR("error:type:~p, state:~p",[Type,State]),
            {reply, {error, {no_type, Type}}, State};
        [Pid] ->
            case (catch handle_sync_input(Pid, Cmd, Timeout, From, State)) of
                {ok, NewState} ->
                    {noreply, NewState};
                Error ->
                    ?ERROR("error:~p, state:~p",[Error, State]),
                    {reply, Error, State}
            end
     end;

handle_call(Req, _From, State) ->
    ?WARNING("unexpect request: ~p", [Req]),
    {reply, {error, {invalid_request, Req}}, State}.

%%--------------------------------------------------------------------
%% Func: handle_cast/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_cast({asyn_input, Type, Cmd}, #state{tl1_tcp = Pids} = State) ->
    ?INFO("handle_cast, Cmd,~p", [Cmd]),
    case get_tl1_tcp(Type, Pids) of
        [] ->
            ?ERROR("error:type:~p, state:~p",[Type,State]),
            {noreply, State};
        [Pid] ->
            handle_asyn_input(Pid, Cmd, Type, State)
    end;

handle_cast(Msg, State) ->
    ?WARNING("unexpected message: ~n~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Func: handle_info/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_info({sync_timeout, ReqId, From}, State) ->
    ?WARNING("received sync_timeout [~w] message", [ReqId]),
    case ets:lookup(tl1_request_table, ReqId) of
        [#request{from = From, data = Data} = Req] ->
            gen_server:reply(From, {error, {tl1_timeout, [ReqId, Data]}}),
            ets:insert(tl1_request_timeout, Req),
            ets:delete(tl1_request_table, ReqId);
        _ ->
            ?ERROR("cannot lookup reqid:~p", [ReqId])
    end,
    {noreply, State};

handle_info({tl1_error, Pct, Reason}, State) ->
    handle_tl1_error(Pct, Reason, State),
    {noreply, State};

handle_info({tl1_tcp, Pct}, State) ->
    handle_recv_tcp(Pct, State),
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, #state{tl1_tcp = Pids} = State) ->
    Type = [T || {T, P} <- Pids, P == Pid],
	?ERROR("unormal exit message received: ~p, ~p", [Type, Reason]),
	{noreply, State};

handle_info(Info, State) ->
    ?WARNING("unexpected info: ~n~p", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Func: terminate/2
%% Purpose: Shutdown the server
%% Returns: any (ignored by gen_server)
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%----------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%%----------------------------------------------------------------------
code_change(_Vsn, State, _Extra) ->
    {ok, State}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
%% send
handle_sync_input(Pid, Cmd, Timeout, From, #state{req_id = ReqId} = State) ->
    NextReqId = get_next_reqid(ReqId),
    ?INFO("input reqid:~p, cmd:~p, state:~p",[NextReqId, Cmd, State]),
    Session = #pct{request_id = NextReqId,
               type = 'input',
               complete_code = 'REQ',
               data = Cmd},
    etl1_tcp:send_tcp(Pid, {send_req, Session, Cmd}),
    Msg    = {sync_timeout, NextReqId, From},
    Ref    = erlang:send_after(Timeout, self(), Msg),
    Req    = #request{id = NextReqId,
              type    = iuput,
              data    = Cmd,
              ref     = Ref,
              time    = extbif:timestamp(),
              timeout = Timeout,
              from    = From},
    ets:insert(tl1_request_table, Req),
    {ok, State#state{req_id = NextReqId}}.

handle_asyn_input(Pid, Cmd, Ems, #state{req_id = ReqId} = State) ->
    NextReqId = get_next_reqid(ReqId),
    ?INFO("input reqid:~p, cmd:~p, state:~p",[NextReqId, Cmd, State]),
    Session = #pct{request_id = NextReqId,
               type = 'asyn_input',
               complete_code = 'REQ',
               data = Cmd},
    etl1_tcp:send_tcp(Pid, {send_req, Session, Cmd}),
    Req    = #request{id = NextReqId,
              type    = asyn_input,
              ems     = Ems,
              data    = Cmd,
              time    = extbif:timestamp()},
    ets:insert(tl1_request_table, Req),
    {noreply, State#state{req_id = NextReqId}}.


%% send error
handle_tl1_error(#pct{request_id = ReqId} = Pct, Reason, #state{callback = Callback} = _State) ->
    case ets:lookup(tl1_request_table, ReqId) of
	[#request{type = 'input', ref = _Ref, from = From}] ->
	    gen_server:reply(From, {error, Reason}),
	    ets:delete(tl1_request_table, ReqId),
	    ok;
	[#request{type = 'asyn_input',data = Cmd, time = Time}] ->
        Now = extbif:timestamp(),
        ?INFO("recv tcp reqid:~p, time:~p, cmd :~p",[ReqId, Now - Time, Cmd]),
        lists:map(fun({_Name, Pid}) ->
            Pid ! {asyn_data, {error, Reason}}
        end, Callback);
	_ ->
		?ERROR("unexpected tl1, reqid:~p, ~n error: ~p",[Pct, Reason])
    end.


%% receive
handle_recv_tcp(#pct{request_id = ReqId, type = 'output', complete_code = CompCode, data = Data} = _Pct,  
    #state{callback = Callback} = _State) ->
%    ?INFO("recv tcp reqid:~p, code:~p, data:~p",[ReqId, CompCode, Data]),
    case ets:lookup(tl1_request_table, to_integer(ReqId)) of
        [#request{type = 'input', ref = Ref, data = Cmd, timeout = Timeout, from = From}] ->
            Remaining = case (catch cancel_timer(Ref)) of
                Rem when is_integer(Rem) -> Rem;
                _ -> 0
            end,
            OutputData = {CompCode, Data},
            Reply = {ok, OutputData, {ReqId, Timeout - Remaining}},
            ?INFO("recv tcp reqid:~p, from:~p, cmd :~p",[{ReqId, (Timeout - Remaining)/1000}, From,Cmd]),
            %TODO Terminator判断是否结束，然后回复，需要reqid是否一致，下一个包是否有head，目的多次信息收集，一次返回
            gen_server:reply(From, Reply),
            ets:delete(tl1_request_table, ReqId),
            ok;
       [#request{type = 'asyn_input',ems = {Type, Cityid}, data = Cmd, time = Time}] ->
           Now = extbif:timestamp(),
           ?INFO("recv tcp reqid:~p, time:~p, cmd :~p",[ReqId, Now - Time, Cmd]),
            lists:map(fun({_Name, Pid}) ->
                Reply = {asyn_data, Data, {Type, Cityid}},
                Pid ! Reply
            end, Callback);
	_ ->
        case ets:lookup(tl1_request_timeout, to_integer(ReqId)) of
            [#request{data = Cmd, time = Time}] ->
                Now = extbif:timestamp(),
                ?ERROR("cannot find reqid:~p, time:~p, cmd:~p", [ReqId, Now - Time, Cmd]),
                ets:delete(tl1_request_timeout, ReqId);
             _ ->
                ?ERROR("cannot find reqid:~p", [ReqId])
        end
	end;
handle_recv_tcp(Pct, _State) ->
    ?ERROR("received crap  type:~p, Pct :~p", [Pct]),
    ok.

%% second fun
cancel_timer(undefined) ->
    ok;
cancel_timer(Ref) ->
    (catch erlang:cancel_timer(Ref)).

get_next_reqid(ReqId) ->
    if ReqId == 1000 * 1000 ->
           0;
         true ->
            ReqId + 1
        end.

get_tl1_tcp({Type, City}, Pids) ->
    GetPids = [Pid || {{T, C}, Pid} <- Pids, {T, C} == {to_list(Type), to_list(City)}],
    case length(GetPids) > 1 of
        false ->
            GetPids;
        true ->
            [lists:nth(random:uniform(length(GetPids)), GetPids)]
    end.
