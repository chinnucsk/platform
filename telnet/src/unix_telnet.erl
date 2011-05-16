-module(telnet_conn).

-author("hejin-2011-4-27").

-export([start/1,
        get_data/2, 
        send_data/2
        ]).

-include("elog.hrl").

-define(CONN_TIMEOUT, 10000).

-define(username,"login: ").
-define(password,"Password: ").
-define(prx,"login: |Password: |\\\$ |> ").

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
    case init(Opts) of
    {ok, Pid} ->
        {ok, Pid};
    {error, _Error} ->
        {ok, undefined}
    end.

get_data(Pid, Cmd) ->
    telnet_client:send_data(Pid, Cmd),
    Resp = telnet_client:get_data(Pid),
    {ok, Resp}.

send_data(Pid, Cmd) ->
     telnet_client:send_data(Pid, Cmd),
     ok.

 stop(Pid) ->
     telnet_client:close(Pid),
     ok.



init(Opts) ->
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
