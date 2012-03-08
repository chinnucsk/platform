-module(etl1_tcp).

-author("hejin-2011-03-24").

-behaviour(gen_server2).

%% Network Interface callback functions
-export([start_link/2, start_link/3,
        get_status/1,
        shakehand/1,
        send_req/3,
        send_tcp/1]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        prioritise_info/2,
        code_change/3,
        terminate/2]).

-include("elog.hrl").

-define(TCP_OPTIONS, [binary, {packet, 0}, {active, true}, {reuseaddr, true}, {send_timeout, 6000}]).

-define(TIMEOUT, 12000).

-define(USERNAME, "root").
-define(PASSWORD, "public").
-define(MAX_CONN, 100).

-record(state, {server, host, port, username, password, max_conn,
        socket, tl1_table, conn_num, conn_state, login_state, rest, data}).

-include("tl1.hrl").

-import(dataset, [get_value/2, get_value/3]).

-import(extbif, [to_list/1]).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
start_link(Server, NetIfOpts) ->
    ?WARNING("start etl1_tcp....~p",[NetIfOpts]),
	gen_server2:start_link(?MODULE, [Server, NetIfOpts], []).

start_link(Server, Name, NetIfOpts) ->
    ?WARNING("start etl1_tcp....~p,~p",[Name, NetIfOpts]),
	gen_server2:start_link({local, Name}, ?MODULE, [Server, NetIfOpts], []).

login_state(Pid, LoginState) ->
    gen_server2:cast(Pid, {login_state, LoginState}).

get_status(Pid) ->
    gen_server2:call(Pid, get_status, 6000).

shakehand(Pid) ->
    gen_server2:cast(Pid, shakehand).

send_req(Pid, Session, Cmd) ->
    gen_server2:cast(Pid, {send_req, Session, Cmd}).

send_tcp(Pid, Cmd) ->
    gen_server2:cast(Pid, {send, Cmd}).

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
init([Server, Args]) ->
    case (catch do_init(Server, Args)) of
	{error, Reason} ->
	    {stop, Reason};
	{ok, State} ->
	    {ok, State}
    end.

do_init(Server, Args) ->
    process_flag(trap_exit, true),
    Tl1Table = ets:new(tl1_table, [ordered_set, {keypos, #pct.request_id}]),
    %% -- Socket --
    Host = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    Username = proplists:get_value(username, Args, ?USERNAME),
    Password = proplists:get_value(password, Args, ?PASSWORD),
    MaxConn = proplists:get_value(max_conn, Args, ?MAX_CONN),
    {ok, Socket, ConnState} = connect(Host, Port, Username, Password),
    %%-- We are done ---
    {ok, #state{server = Server, host = Host, port = Port, username = Username, password = Password, max_conn = MaxConn, 
        socket = Socket, tl1_table = Tl1Table, conn_num = 0, conn_state = ConnState, rest = <<>>, data = []}}.

connect(Host, Port, Username, Password) when is_binary(Host) ->
    connect(binary_to_list(Host), Port, Username, Password);
connect(Host, Port, Username, Password) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTIONS, ?TIMEOUT) of
    {ok, Socket} ->
        ?INFO("connect succ...~p,~p",[Host, Port]),
        login(Socket, Username, Password),
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
    Cmd = lists:concat(["LOGIN:::login::", "UN=", to_list(Username), ",PWD=", Password, ";"]),
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
handle_call(get_status, _From, #state{tl1_table = Tl1Table, conn_num = ConnNum, conn_state = connected} = State) ->
    {reply, {ok, [{count, ets:info(Tl1Table, size) + ConnNum}, State]}, State};

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
handle_cast({send, Cmd}, #state{conn_state = connected, socket = Sock} = State) ->
    tcp_send(Sock, Cmd),
    {noreply, State};

handle_cast({send, _Cmd}, #state{server = Server, conn_state = ConnState, host = Host, port = Port} = State) ->
    Server ! {tl1_error, self(), {conn_failed, ConnState, Host, Port}},
    {noreply, State};

handle_cast({send_req, Pct, _Cmd}, #state{tl1_table = Tl1Table,conn_num = ConnNum,
    conn_state = connected} = State) when ConnNum > ?MAX_CONN ->
    ets:insert(Tl1Table, Pct),
    {noreply, State};

handle_cast({send_req, Pct, Cmd}, #state{conn_state = connected} = State) ->
    NewConnNum = handle_send_tcp(Pct, Cmd, State),
    {noreply, State#state{conn_num = NewConnNum}};

handle_cast({send_req, Pct, _Cmd}, #state{server = Server, conn_state = ConnState, host = Host, port = Port} = State) ->
    Server ! {tl1_error, Pct, {conn_failed, ConnState, Host, Port}},
    {noreply, State};

handle_cast({login_state, LoginState}, State) ->
    ?INFO("login state ...~p, ~p", [LoginState, self()]),
    {noreply, State#state{login_state = LoginState}};

handle_cast(Msg, State) ->
    ?WARNING("unexpected message: ~n~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Func: handle_info/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_info({tcp, Sock, Bytes}, #state{socket = Sock, rest = Rest, data = Data, conn_num = ConnNum} = State) ->
    ?INFO("received tcp ~p ", [Bytes]),
    {NewData, NewRest, NewConnNum} = case binary:last(Bytes) of
        $; ->
            NowBytes = binary:split(list_to_binary([Rest, Bytes]), <<">">>, [global]),
            {OtherBytes, [LastBytes]} = lists:split(length(NowBytes)-1, NowBytes),
            NowData = handle_recv_wait(OtherBytes),
            handle_recv_msg(LastBytes, State#state{data = Data ++ NowData}),
            {[], <<>>, check_tl1_table(ConnNum, State)};
        $> ->
            NowBytes = binary:split(list_to_binary([Rest, Bytes]), <<">">>, [global]),
            NowData = handle_recv_wait(NowBytes),
            {Data ++ NowData, <<>>, ConnNum};
        _ ->
            {Data, list_to_binary([Rest, Bytes]), ConnNum}
       end,
    {noreply, State#state{rest = NewRest, data = NewData, conn_num = NewConnNum}};

handle_info({tcp_closed, Socket}, State) ->
    ?ERROR("tcp close: ~p,~p,~p", [Socket]),
    {noreply, State#state{socket = null, conn_state = disconnect}};


handle_info(shakehand, #state{conn_num = ConnNum, socket = Socket, conn_state = connected} = State) ->
    tcp_send(Socket, "SHAKEHAND:::shakehand::;"),
    erlang:send_after(5 * 60 * 1000, self(), shakehand),
    {noreply, State#state{conn_num = ConnNum + 1}};

handle_info(shakehand, State) ->
    {noreply, State};

handle_info({timeout, retry_connect},  #state{host = Host, port = Port, username = Username, password = Password} = State) ->
    {ok, Socket, ConnState} = connect(Host, Port, Username, Password),
    {noreply, State#state{socket = Socket, conn_num = 0, conn_state = ConnState}};

handle_info(Info, State) ->
    ?WARNING("unexpected info: ~n~p", [Info]),
    {noreply, State}.

prioritise_info(get_status, _State) ->
    9;
prioritise_info(tcp_closed, _State) ->
    7;
prioritise_info({timeout, retry_connect}, _State) ->
    7;
prioritise_info(shakehand, _State) ->
    5;
prioritise_info(_, _State) ->
    0.

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

check_tl1_table(ConnNum, #state{tl1_table = Tl1Table} = State) ->
    check_tl1_table(ets:first(Tl1Table), ConnNum, State).

check_tl1_table('$end_of_table', ConnNum, _State) ->
    ConnNum - 1;
check_tl1_table(Reqid, ConnNum, #state{tl1_table = Tl1Table} = State) ->
    case ets:lookup(Tl1Table, Reqid) of
        [Pct] ->
            handle_send_tcp(Pct, Pct#pct.data, State),
            ets:delete(Tl1Table, Reqid);
        [] ->
            ok
     end,
     ConnNum.

%% send
handle_send_tcp(Pct, MsgData, #state{server = Server, conn_num = ConnNum, socket = Sock}) ->
   try etl1_mpd:generate_msg(Pct, MsgData) of
	{ok, Msg} ->
	    case tcp_send(Server, Pct, Sock, Msg) of
            succ -> ConnNum + 1;
            fail -> ConnNum
        end;
	{discarded, Reason} ->
        send_failed(Server, Pct, Reason),
        ConnNum
     catch
        Error:Exception ->
        ?ERROR("exception: ~p, ~n ~p", [{Error, Exception}, erlang:get_stacktrace()]),
        send_failed(Server, Pct, {'EXIT',Exception}),
        ConnNum
    end.

tcp_send(Sock, Msg) ->
    case (catch gen_tcp:send(Sock, Msg)) of
	ok ->
	    succ;
	Error ->
	    ?ERROR("failed sending message to ~n   ~p",[Error]), 
        fail
    end.

tcp_send(Server, Pct, Sock, Msg) ->
    case (catch gen_tcp:send(Sock, Msg)) of
	ok ->
	    ?INFO("send cmd  to :~p", [Msg]),
	    succ;
	{error, Reason} ->
	    ?ERROR("failed sending message to ~p",[Reason]),
        send_failed(Server, Pct, {tcp_send_error, Reason}),
        fail;
	Error ->
	    ?ERROR("failed sending message to ~n   ~p",[Error]),
        send_failed(Server, Pct, {tcp_send_exception, Error}),
        fail
    end.

send_failed(Server, Pct, ERROR) ->
    Server ! {tl1_error, Pct, ERROR}.

%% receive
handle_recv_wait(Bytes) ->
    handle_recv_wait(Bytes, []).

handle_recv_wait([], Data) ->
    Data;
handle_recv_wait([A|Bytes], Data) when is_list(Bytes)->
    handle_recv_wait(Bytes, Data ++ handle_recv_wait(A, []));

handle_recv_wait(<<>>, Data) ->
    Data;
handle_recv_wait(Bytes, Data) when is_binary(Bytes)->
    case (catch etl1_mpd:process_msg(Bytes)) of
	{ok, #pct{data = NewData} = _Pct}  ->
        case NewData of
            {ok, Data0} ->
                Data ++ Data0;
            {error, _Reason} ->
                Data
        end ;
	Error ->
	    ?ERROR("processing of received message failed: ~n ~p", [Error]),
	    Data
    end.

handle_recv_msg(<<>>, _State)  ->
    ok;
handle_recv_msg(Bytes, #state{server = Server, data = Data, socket = Socket, 
    username = Username, password = Password} = State) ->
    case (catch etl1_mpd:process_msg(Bytes)) of
        {ok, #pct{complete_code = "DENY", en = "AAFD"}} ->
            ?WARNING("begin to relogin...~p", [State]),
            login(Socket, Username, Password);
        {ok, #pct{request_id = "shakehand", complete_code = _CompletionCode}} ->
            ok;
        {ok, #pct{request_id = "login", complete_code = CompletionCode}} ->
            LoginState = case CompletionCode of
                "COMPLD" -> succ;
                "DENY" -> fail
            end,
            login_state(self(), LoginState),
            Server ! {tl1_login, self(), {login_state, LoginState}};
        {ok, #pct{type = 'alarm', data = {ok, Data2}} = _Pct}  ->
            Server ! {tl1_trap, self(), Data ++ Data2};
        {ok, #pct{type = 'output', data = NewData} = Pct}  ->
            AccData = case NewData of
                {ok, Data2} ->
                    {ok, Data ++ Data2};
                {error, _Reason} ->
                    {error, _Reason}
            end,
            Server ! {tl1_tcp, self(), Pct#pct{data = AccData}};

        Error ->
            ?ERROR("processing of received message failed: ~n ~p", [Error]),
            ok
    end.
