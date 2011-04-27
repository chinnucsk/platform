-module(telnet_conn).

-author("hejin-2011-4-27").

-behaviour(gen_server).

-export([send_data/1, send_data/2, get_data/1]).

%% Callback
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-include("elog.hrl").

-define(CONN_TIMEOUT, 10000).
-define(CALL_TIMEOUT, 12000).

-record(state, {
	  host,
	  port,
	  user,
	  password,
	  socket,
	  conn_state
	 }).
     

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

send_data(Cmd) ->
    send_data(Cmd, ?CALL_TIMEOUT).

send_data(Cmd, Timeout)  ->
    gen_server:call(?MODULE, {cmd, Cmd}, Timeout).


init([Opts]) ->
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 23),
    Username = proplists:get_value(username, Opts, "root"),
    Password = proplists:get_value(password, Opts, "public"),
    case (catch connect(Host, Port, ?CONN_TIMEOUT, true, Username, Password)) of
	{ok, Pid} ->
	    {ok, #state{host = Host, port = Port, socket = Pid, user = Username, password = Password, conn_state = connected}};
	{error, Reason} ->
	    {stop, Reason};
    {'EXIT', Reason} ->
        {stop, Reason}
	end.

connect(Ip,Port,Timeout,KeepAlive,Username,Password) ->
    ?INFO("telnet:connect",[]),
    Result =case ct_telnet_client:open(Ip,Port,Timeout,KeepAlive) of
                {ok,Pid} ->
                case ct_telnet:silent_teln_expect(Pid,[],[prompt],?prx,[]) of
                    {ok,{prompt,?username},_} ->
                    ok = ct_telnet_client:send_data(Pid,Username),
                    ?INFO("Username: ~s",[Username]),
                    case ct_telnet:silent_teln_expect(Pid,[],prompt,?prx,[]) of
                        {ok,{prompt,?password},_} ->
                        ok = ct_telnet_client:send_data(Pid,Password),
                        Stars = lists:duplicate(length(Password),$*),
                        ?INFO("Password: ~s",[Stars]),
                        ok = ct_telnet_client:send_data(Pid,""),
                        case ct_telnet:silent_teln_expect(Pid,[],prompt,?prx,[]) of
                            {ok,{prompt,Prompt},_}
                                    when Prompt=/=?username, Prompt=/=?password ->
                                {ok,Pid};
                            Error ->
                                ?INFO("Password failed\n~p\n",
                                     [Error]),
                                {error,Error}
                        end;
                        Error ->
                        ?WARNING("Login failed\n~p\n",[Error]),
                        {error,Error}
                    end;
                    {ok,[{prompt,_OtherPrompt1},{prompt,_OtherPrompt2}],_} ->
                    {ok,Pid};
                    Error ->
                    ?WARNING("Did not get expected prompt\n~p\n",[Error]),
                    {error,Error}
                end;
                Error ->
                ?WARNING("Could not open telnet connection\n~p\n",[Error]),
                Error
	end,
    Result.


handle_call(stop, _From, State) ->
    ?INFO("received stop request", []),
    {stop, normal, State};

handle_call({cmd, Cmd}, From, #state{conn_state = connected, socket = Socket} = State) ->
    ?INFO("handle_call,from:~p, Cmd,~p", [From, Cmd]),
    ct_telnet_client:send_data(Socket, Cmd),
    Resp = ct_telnet_client:get_data(Socket),
    ?INFO("respond :~p",[Resp]),
    {reply, Resp, State};

handle_call({cmd, _Cmd}, _From, #state{conn_state = ConnState} = State) ->
    {reply, {error, {conn_state, ConnState}}, State};

handle_call(Req, _From, State) ->
    ?WARNING("unexpect request: ~p", [Req]),
    {reply, {error, {invalid_request, Req}}, State}.


handle_cast(Msg, State) ->
    ?WARNING("unexpected message: ~n~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?WARNING("unexpected info: ~n~p", [Info]),
    {noreply, State}.

 terminate(_Reason, _State) ->
    ok.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.
