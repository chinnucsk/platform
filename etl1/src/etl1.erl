-module(etl1).

-author("hejin-03-28").

-behaviour(gen_server).

%% Network Interface callback functions
-export([start_link/1,
        set_tl1/1,
        get_tl1/0,
        input/2, input/3,
        input_group/2, input_group/3
     ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).


-include("elog.hrl").
-include("tl1.hrl").

-define(RETRIES, 2).

-define(REQ_TIMEOUT, 26000).

-define(CALL_TIMEOUT, 30000).

-import(extbif, [to_list/1, to_binary/1]).

-record(request,  {id, type, data, ref, from}).

-record(state, {tl1_tcp, req_id=0}).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
start_link(Tl1Options) ->
    ?INFO("start etl1....~p",[Tl1Options]),
	gen_server:start_link({local, ?MODULE},?MODULE, [Tl1Options], []).

get_tl1() ->
    gen_server:call(?MODULE, get_tl1, ?CALL_TIMEOUT).


set_tl1(Tl1Info) ->
    ?INFO("set tl1 info....~p",[Tl1Info]),
    gen_server:call(?MODULE, {set_tl1, Tl1Info}, ?CALL_TIMEOUT).

%%Cmd = LST-ONUSTATE::OLTID=${oltid},PORTID=${portid}:CTAG::;
input(Type, Cmd) ->
    input(Type, Cmd, ?REQ_TIMEOUT).


input(Type, Cmd, Timeout) when is_tuple(Type)->
    case gen_server:call(?MODULE, {sync_input, self(), Type, Cmd, Timeout}, ?CALL_TIMEOUT) of
        {ok, {CompCode, {error, Reason}}, _} ->
            %send after : {error,  " EN=DDNS ENDESC=port may not support this operation: -Port-4,;"}
            {error, {cmd_error, Cmd, CompCode ++ Reason}};
        {ok, {_CompCode, Data}, _} ->
            %Data :{ok ,Value} |
            Data;
        {ok, Error, _} ->
            {error, Error};
        {error, Reason} ->
            %send before: {error, {invalid_request, Req}} | {error, no_ctag} | {error, too_big} | {error, {'EXIT',Reason }} | {error, {conn_failed, ConnState, Host, Port}}
            %sending    : {error, {tcp_send_error, Reason}} | {error, {tcp_send_exception, Error}}
            %send after : {error, {tl1_timeout, Data}}
            {error, Reason};
        Error ->
            % {no_type, Type} | {many_type, Type} | {'EXIT',{badarith,_}}
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
    Name = list_to_atom(lists:concat(["etl1_tcp_", to_list(Type), "_", to_list(CityId)])),
    case etl1_tcp:start_link(Name, Tl1Info) of
        {ok, Pid} ->
            {{to_list(Type), to_list(CityId)}, Pid};
        {error, Error} ->
            ?ERROR("get tcp error: ~p, ~p", [Error, Tl1Info]),
            []
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
handle_call({set_tl1, Tl1Info}, _From, #state{tl1_tcp = Pids} = State) ->
    NewPid = do_connect(Tl1Info),
    NewState = State#state{tl1_tcp = [NewPid|Pids]},
    {reply, ok, NewState};

handle_call({sync_input, Send, Type, Cmd, Timeout}, From, #state{tl1_tcp = Pids} = State) ->
    ?INFO("handle_call,Pid:~p,from:~p, Cmd,~p", [{Send, node(Send)}, From, Cmd]),
    case get_tl1_tcp(Type, Pids) of
        [Pid] ->
            case (catch handle_sync_input(Pid, Cmd, Timeout, From, State)) of
                {ok, NewState} ->
                    ?INFO("has input, state:~p",[NewState]),
                    {noreply, NewState};
                Error ->
                    ?ERROR("error:~p, state:~p",[Error, State]),
                    {reply, Error, State}
            end;
        [] ->
            ?ERROR("error:disconn,type:~p, state:~p",[Type,State]),
            {reply, {no_type, Type}, State};
        _ ->
            ?ERROR("tl1 connect too much,type:~p, state:~p",[Type,State]),
            {reply, {many_type, Type}, State}
     end;

handle_call(get_tl1, _From, State) ->
    {reply, {ok, State}, State};

handle_call(Req, _From, State) ->
    ?WARNING("unexpect request: ~p", [Req]),
    {reply, {error, {invalid_request, Req}}, State}.

%%--------------------------------------------------------------------
%% Func: handle_cast/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
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
    %?WARNING("received sync_timeout [~w] message", [ReqId]),
    case ets:lookup(tl1_request_table, ReqId) of
	[#request{from = From, data = Data} = _Req0] ->
	    gen_server:reply(From, {error, {tl1_timeout, Data}}),
	    ets:delete(tl1_request_table, ReqId);
	_ ->
        ?WARNING("cannot lookup request: ~p", [ReqId])
    end,
    {noreply, State};

handle_info({tl1_error, Pct, Reason}, State) ->
    handle_tl1_error(Pct, Reason),
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
    ?INFO("cmd, state:~p,~p",[Cmd, State]),
    NextReqId = 
        if ReqId == 1000 * 1000 ->
           0;
         true ->
            ReqId + 1
        end,
    Session = #pct{type = 'input',
               request_id = NextReqId,
               complete_code = 'REQ',
               data = Cmd},
    etl1_tcp:send_tcp(Pid, {send_req, Session, Cmd}),
    Msg    = {sync_timeout, NextReqId, From},
    Ref    = erlang:send_after(Timeout, self(), Msg),
    Req    = #request{id = NextReqId,
              type    = iuput,
              data    = Cmd,
              ref     = Ref,
              from    = From},
    ets:insert(tl1_request_table, Req),
    {ok, State#state{req_id = NextReqId}}.

%% send error
handle_tl1_error(#pct{request_id = ReqId} = _Pct, Reason) ->
    case ets:lookup(tl1_request_table, ReqId) of
	[#request{ref = _Ref, from = From}] ->
	    Reply = {error, Reason},
	    gen_server:reply(From, Reply),
	    ets:delete(tl1_request_table, ReqId),
	    ok;
	_ ->
		?ERROR("unexpected tl1 error: ~p, ~p",[ReqId, Reason])
    end.

%% receive
handle_recv_tcp(#pct{type = 'output', request_id = ReqId, complete_code = CompCode, data = Data} = _Pct,  _State) ->
    ?INFO("recv tcp reqid:~p, code:~p, data:~p",[ReqId, CompCode, Data]),
    case ets:lookup(tl1_request_table, ReqId) of
	[#request{ref = Ref, from = From}] ->
	    Remaining = case (catch cancel_timer(Ref)) of
		    Rem when is_integer(Rem) -> Rem;
		    _ -> 0
		end,
	    OutputData = {CompCode, Data},
	    Reply = {ok, OutputData, Remaining},
        ?INFO("tcp reply:~p, from:~p",[Reply, From]),
        %TODO Terminator判断是否结束，然后回复，需要reqid是否一致，下一个包是否有head，目的多次信息收集，一次返回
	    gen_server:reply(From, Reply),
	    ets:delete(tl1_request_table, ReqId),
	    ok;
	_ ->
        ok
	end;
handle_recv_tcp(CrapPdu, _State) ->
    ?ERROR("received crap (snmp) Pdu from ~w:~w =>~p", [CrapPdu]),
    ok.

%% sencond fun
cancel_timer(undefined) ->
    ok;
cancel_timer(Ref) ->
    (catch erlang:cancel_timer(Ref)).

get_tl1_tcp({Type, City}, Pids) ->
    [Pid || {{T, C}, Pid} <- Pids, {T, C} == {to_list(Type), to_list(City)}].
