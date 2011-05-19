-module(olt_telnet).

-author("hejin-2011-5-16").

-export([start/1,
        get_data/2
        ]).

-include("elog.hrl").

-define(CONN_TIMEOUT, 10000).
-define(CMD_TIMEOUT, 3000).

-define(username,"Username:").
-define(password,"Password:").
-define(prx,"Username:|Password:|\\\#|> |--More--").

-define(keepalive, true).

start(Opts) ->
    case init(Opts) of
    {ok, Pid} ->
        {ok, Pid};
    {error, _Error} ->
        {ok, undefined}
    end.

get_data(Pid, Cmd) ->
    get_data(Pid, Cmd, []).

get_data(Pid, Cmd, Acc) ->
    case telent_gen_conn:teln_cmd(Pid, Cmd, ?prx, ?CMD_TIMEOUT) of
        {ok, Data, "--More--",Rest} ->
            ?INFO("more: ~p, ~n, ~p", [Data, Rest]),
             get_data(Pid, " ", [Data|Acc]);
        {ok, Data, _PromptType,Rest} ->
            ?INFO("Return: ~p, ~n, ~p", [Data, Rest]),
            AllData = lists:flatten(lists:reverse([Data|Acc])),
            {ok, AllData};
        Error ->
            Retry = {retry, no, {cmd,Cmd}},
            ?INFO("Return: ~p", [Error]),
            Retry
    end.


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
