-module(telnet_conn).

-author("hejin-2011-4-27").

-behaviour(gen_server).

-export([start_link/1, start/1,
        get_data/2, get_data/3,
        send_data/2,
        stop/1]).

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

-define(username,"login: ").
-define(password,"Password: ").
-define(prx,"login: |Password: |\\\$ |> ").

%-define(username,"Username:").
%-define(password,"Password:").
%-define(prx,"Username:|Password:|\\\#|> ").

-define(keepalive, true).

-record(state, {
	  host,
	  port,
	  user,
	  password,
	  socket,
	  conn_state
	 }).
     

%%%%%%%%%%%%%%%%%%% extra interface %%%%%%%%%%%%%%%%%
start(Opts) ->
    case start_link(Opts) of
    {ok, Pid} ->
        {ok, Pid};
    {error, _Error} ->
        {ok, undefined}
    end.

get_data(Pid, Cmd) ->
    get_data(Pid, Cmd, ?CALL_TIMEOUT).

get_data(Pid, Cmd, Timeout)  ->
    gen_server:call(Pid, {cmd, Cmd}, Timeout).

send_data(Pid, Cmd) ->
    gen_server:cast(Pid, {cmd, Cmd}).

stop(Pid) ->
    gen_server:call(Pid, stop).



start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).


init([Opts]) ->
    io:format("starting telnet conn ...~p",[Opts]),
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 23),
    Username = proplists:get_value(username, Opts, "root"),
    Password = proplists:get_value(password, Opts, "public"),
    case (catch connect(Host, Port, ?CONN_TIMEOUT, ?keepalive, Username, Password)) of
	{ok, Pid} ->
	    {ok, #state{host = Host, port = Port, socket = Pid, user = Username, password = Password, conn_state = connected}};
	{error, Error} ->
	    {stop, Error};
    {'EXIT', Reason} ->
        {stop, Reason}
	end.

connect(Ip,Port,Timeout,KeepAlive,Username,Password) ->
    ?INFO("telnet:connect",[]),
    Result =case telnet_client:open(Ip,Port,Timeout,KeepAlive) of
                {ok,Pid} ->
                    ?INFO("open success...~p",[Pid]),
                    case telnet:silent_teln_expect(Pid,[],[prompt],?prx,[]) of
                        {ok,{prompt,?username},_} ->
                        ok = telnet_client:send_data(Pid,Username),
                        ?INFO("Username: ~s",[Username]),
                        case telnet:silent_teln_expect(Pid,[],prompt,?prx,[]) of
                            {ok,{prompt,?password},_} ->
                            ok = telnet_client:send_data(Pid,Password),
%                            Stars = lists:duplicate(length(Password),$*),
                            ?INFO("Password: ~s",[Password]),
                            ok = telnet_client:send_data(Pid,""),
                            case telnet:silent_teln_expect(Pid,[],prompt,
                                              ?prx,[]) of
                                {ok,{prompt,Prompt},_}
                                when Prompt=/=?username, Prompt=/=?password ->
                                {ok,Pid};
                                Error ->
                                ?WARNING("Password failed\n~p\n",
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
    telnet_client:send_data(Socket, Cmd),
    Resp = telnet_client:get_data(Socket),
    ?INFO("respond :~p",[Resp]),
    {reply, Resp, State};

handle_call({cmd, _Cmd}, _From, #state{conn_state = ConnState} = State) ->
    {reply, {error, {conn_state, ConnState}}, State};

handle_call(Req, _From, State) ->
    ?WARNING("unexpect request: ~p", [Req]),
    {reply, {error, {invalid_request, Req}}, State}.


handle_cast({cmd, Cmd}, #state{conn_state = connected, socket = Socket} = State) ->
    ?INFO("handle_cast, Cmd,~p", [Cmd]),
    telnet_client:send_data(Socket, Cmd),
    {noreply, State};

handle_cast(Req, State) ->
    ?WARNING("unexpected cast: ~n~p, state:~p", [Req, State]),
    {noreply, State}.

handle_info(Info, State) ->
    ?WARNING("unexpected info: ~n~p", [Info]),
    {noreply, State}.

 terminate(_Reason, _State) ->
    ok.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.
