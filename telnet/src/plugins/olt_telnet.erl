-module(olt_telnet).

-author("hejin-2011-5-16").

-export([start/1,
        get_data/2,
        close/1
        ]).

-include("elog.hrl").

-define(CONN_TIMEOUT, 10000).
-define(CMD_TIMEOUT, 3000).

-define(username,"Username:").
-define(password,"Password:").
-define(prx,"Username:|Password:|\\\#|> |--More--").

-define(keepalive, true).

start(Opts) ->
    init(Opts).

get_data(Pid, Cmd) ->
    get_data(Pid, Cmd, []).

get_data(Pid, Cmd, Acc) ->
    case telnet_gen_conn:teln_cmd(Pid, Cmd, ?prx, ?CMD_TIMEOUT) of
        {ok, Data, "--More--",Rest} ->
            ?INFO("more: ~p, ~n, ~p", [Data, Rest]),
            Data1 =  string:join(Data, "\r\n"),
             get_data(Pid, " ", [Data1|Acc]);
        {ok, Data, _PromptType,Rest} ->
            ?INFO("Return: ~p, ~p, ~n, ~p", [Data, Rest, Acc]),
            Data1 =  string:join(Data, "\r\n"),
            AllData = string:join(lists:reverse([Data1|Acc]), "\r\n"),
            {ok, AllData};
        Error ->
            ?WARNING("Return error: ~p", [Error]),
            Data1 = io_lib:format("telnet send cmd error, cmd: ~p, reason:~p", [Cmd, Error]),
            AllData = string:join(lists:reverse([Data1|Acc]), "\r\n"),
            {ok, AllData}
    end.

close(Pid) ->
    telnet_client:close(Pid).

init(Opts) ->
    io:format("starting telnet conn ...~p",[Opts]),
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 23),
    Username = proplists:get_value(username, Opts, "root"),
    Password = proplists:get_value(password, Opts, "public"),
    case (catch connect(Host, Port, ?CONN_TIMEOUT, ?keepalive, Username, Password)) of
	{ok, Pid} ->
	    {ok, Pid};
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
                                    ?INFO("get data over.............", []),
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
                    {error, Error}
	end,
    Result.
