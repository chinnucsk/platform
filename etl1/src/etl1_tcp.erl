-module(etl1_tcp).

-author("hejin-2011-03-24").

-behaviour(gen_server).

%% Network Interface callback functions
-export([start_link/2, 
        get_status/1,
        send_tcp/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("elog.hrl").

-define(USERNAME, "root").
-define(PASSWORD, "public").

-define(TCP_OPTIONS, [binary, {packet, 0}, {active, true}, {reuseaddr, true}, {send_timeout, 6000}]).

-define(TIMEOUT, 12000).

-record(state, {host, port, username, password, socket, conn_state, rest = <<>>, data = []}).

-include("tl1.hrl").

-import(dataset, [get_value/2, get_value/3]).

-import(extbif, [to_list/1]).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
start_link(Name, NetIfOpts) ->
    ?INFO("start etl1_tcp....~p,~p",[Name, NetIfOpts]),
	gen_server:start_link({local, Name},?MODULE, [NetIfOpts], []).

get_status(Pid) ->
    gen_server:call(Pid, get_status).

send_tcp(Pid, Ptc)  ->
    gen_server:cast(Pid, Ptc).

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
init([Args]) ->
    case (catch do_init(Args)) of
	{error, Reason} ->
	    {stop, Reason};
	{ok, State} ->
	    {ok, State}
    end.

do_init(Args) ->
    process_flag(trap_exit, true),
    %% -- Socket --
    Host = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    Username = proplists:get_value(username, Args, ?USERNAME),
    Password = proplists:get_value(password, Args, ?PASSWORD),
    {ok, Socket, ConnState} = connect(Host, Port, Username, Password),
    %%-- We are done ---
    {ok, #state{host = Host, port = Port, username = Username, password = Password, socket = Socket, conn_state = ConnState}}.

connect(Host, Port, Username, Password) when is_binary(Host) ->
    connect(binary_to_list(Host), Port, Username, Password);
connect(Host, Port, Username, Password) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTIONS, ?TIMEOUT) of
    {ok, Socket} ->
        ?INFO("connect succ...~p,~p",[Host, Port]),
        login(Socket, Username, Password),
        ?INFO("login succ...~p,~p",[Username, Password]),
        erlang:send_after(5 * 60 * 1000, self(), shakehand),
        {ok, Socket, connected};
    {error, Reason} ->
        ?WARNING("tcp connect failure: ~p", [Reason]),
%        retry_connect(),
        {ok, null, disconnect}
    end.

login(Socket, Username, Password) when is_binary(Username)->
    login(Socket, binary_to_list(Username), Password);
login(Socket, Username, Password) when is_binary(Password)->
    login(Socket, Username, binary_to_list(Password));
login(Socket, Username, Password) ->
    ?INFO("begin to login,~p,~p,~p", [Socket, Username, Password]),
    Cmd = lists:concat(["LOGIN:::CTAG::", "UN=", to_list(Username), ",PWD=", Password, ";"]),
    tcp_send(Socket, Cmd).

%%--------------------------------------------------------------------
%% Func: handle_call/3
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_call(get_status, _From, State) ->
    {reply, {ok, State}, State};

handle_call(stop, _From, State) ->
    ?INFO("received stop request", []),
    {stop, normal, State};

handle_call(Req, _From, State) ->
    ?WARNING("unexpect request: ~p", [Req]),
    {reply, {error, {invalid_request, Req}}, State}.

%%--------------------------------------------------------------------
%% Func: handle_cast/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_cast({send_req, Pct, Cmd}, #state{conn_state = connected} = State) ->
    handle_send_tcp(Pct, Cmd, State),
    {noreply, State};

handle_cast({send_req, Pct, _Cmd}, #state{conn_state = ConnState, host = Host, port = Port} = State) ->
    etl1 ! {tl1_error, Pct, {conn_failed, ConnState, Host, Port}},
    {noreply, State};

handle_cast(Msg, State) ->
    ?WARNING("unexpected message: ~n~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Func: handle_info/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_info({tcp, Sock, Bytes}, #state{socket = Sock, rest = Rest, data = Data} = State) ->
    ?INFO("received tcp ~p ", [Bytes]),
    {NewData, NewRest} = case binary:last(Bytes) of
        $; ->
            handle_recv_msg(list_to_binary([Rest, Bytes]), State),
            {[], <<>>};
        $> ->
            NowData = handle_recv_wait(list_to_binary([Rest, Bytes]), State),
            ?INFO("get now data :~p",[NowData]),
            {NowData, <<>>};
        _ ->
            {Data, list_to_binary([Rest, Bytes])}
       end,
    {noreply, State#state{rest = NewRest, data = NewData}};

handle_info({tcp_closed, Socket}, #state{socket = Socket, host = Host, port = Port} = State) ->
    ?ERROR("tcp close: ~p,~p,~p", [Host, Port, Socket]),
    retry_connect(),
    {noreply, State#state{socket = null, conn_state = disconnect}};


handle_info(shakehand, #state{socket = Socket, conn_state = connected} = State) ->
    tcp_send(Socket, "SHAKEHAND:::CTAG::;"),
    ?INFO("send shakehand...~p",[State]),
    erlang:send_after(5 * 60 * 1000, self(), shakehand),
    {noreply, State};

handle_info(shakehand, State) ->
    ?INFO("ignore shakehand...~p",[State]),
    {noreply, State};

handle_info({timeout, retry_connect},  #state{host = Host, port = Port, username = Username, password = Password} = State) ->
    {ok, Socket, ConnState} = connect(Host, Port, Username, Password),
    {noreply, State#state{socket = Socket, conn_state = ConnState}};

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
retry_connect() ->
    erlang:send_after(30000, self(), {timeout, retry_connect}).

%% send
handle_send_tcp(Pct, MsgData, #state{socket = Sock}) ->
    case (catch etl1_mpd:generate_msg(Pct, MsgData)) of
	{ok, Msg} ->
	    ?DEBUG("handle_send_req -> message generated", []),
	    tcp_send(Pct, Sock, Msg);
	{discarded, Reason} ->
        send_failed(Pct, Reason);
     ERROR ->
        send_failed(Pct, ERROR)
    end.

tcp_send(Sock, Msg) ->
    case (catch gen_tcp:send(Sock, Msg)) of
	ok ->
	    ?INFO("sent cmd  to :~p", [Msg]),
	    ok;
	Error ->
	    ?ERROR("failed sending message to ~n   ~p",[Error])
    end.

tcp_send(Pct, Sock, Msg) ->
    case (catch gen_tcp:send(Sock, Msg)) of
	ok ->
	    ?INFO("sent cmd  to :~p", [Msg]),
	    ok;
	{error, Reason} ->
	    ?ERROR("failed sending message to ~p",[Reason]),
        send_failed(Pct, {tcp_send_error, Reason});
	Error ->
	    ?ERROR("failed sending message to ~n   ~p",[Error]),
        send_failed(Pct, {tcp_send_exception, Error})
    end.

send_failed(Pct, ERROR) ->
    etc1 ! {tl1_error, Pct, ERROR}.

%% receive
handle_recv_wait(Bytes, _State) ->
    case (catch etl1_mpd:process_msg(Bytes)) of
	{ok, Pct} when is_record(Pct, pct) ->
        case Pct#pct.data of
            {ok, Data} ->
                Data;
            {error, _Reason} ->
                []
        end ;
	Error ->
	    ?ERROR("processing of received message failed: ~n ~p", [Error]),
	    []
    end.

handle_recv_msg(Bytes, _State) when is_binary(Bytes) and (size(Bytes) == 0) ->
    ?WARNING("snmp error: ~p", [empty_message]),
    ok;
handle_recv_msg(Bytes, #state{data = Data}) ->
    case (catch etl1_mpd:process_msg(Bytes)) of
	%% BMK BMK BMK
	%% Do we really need message size here??
	{ok, #pct{type = 'autonomous'} = Pct} ->
	    ?WARNING("unexpected autonomous message:~p", [Pct]),
        %if port of revice autonomous is the same to input, can TODO
        noreply;
        
%	{ok, _Vsn, #pdu{type = 'acknowledgment'} = Pdu, _MS, _ACM} ->

	{ok, Pct} when is_record(Pct, pct) ->
        NewData = case Pct#pct.data of
            {ok, Data2} ->
                {ok, Data ++ Data2};
            {error, _Reason} ->
                {error, _Reason}
        end,
        ?INFO("send tl1 data :~p", [NewData]),
        etl1 ! {tl1_tcp, Pct#pct{data = NewData}};
    
	Error ->
	    ?ERROR("processing of received message failed: ~n ~p", [Error]),
	    ok
    end.
