-module(olt_huawei_telnet).

-author("hejin 2011-8-17").

-export([start/1,
        get_data/2,
        close/1
        ]).

-include("elog.hrl").

-define(CONN_TIMEOUT, 10000).
-define(CMD_TIMEOUT, 3000).

-define(username,"User name:").
-define(password,"User password:").
-define(prx,"User name:|User password:|5680T#|> |-- More ").

-define(keepalive, true).

-define(splite, "\n").

start(Opts) ->
    init(Opts).

get_data(Pid, Cmd) ->
    get_data(Pid, Cmd, [], []).

get_data(Pid, Cmd, Acc, LastLine) ->
    case telnet_gen_conn:teln_cmd(Pid, Cmd, ?prx, ?CMD_TIMEOUT) of
        {ok, Data, "--More--",Rest} ->
            Lastline1 = string:strip(lists:last(Data)),
            ?INFO("more: ~p, lastline: ~p, ~n, Rest : ~p", [Data, Lastline1, Rest]),
            Data1 =  string:join(Data, ?splite),
            get_data(Pid, " ", [Data1|Acc], Lastline1);
        {ok, Data, PromptType, Rest} ->
            ?INFO("Return: ~p, PromptType : ~p, ~n, Rest :~p", [Data, PromptType, Rest]),
            Data1 =  string:join(Data, ?splite),
            Lastline1 = string:strip(lists:last(Data)),
            case  Lastline1 of
                LastLine ->
                    ?INFO("get end Lastline  ~p, ~n, acc :~p", [Lastline1, Acc]),
                    AllData = string:join(lists:reverse([Data1|Acc]), ?splite),
                    {ok, AllData};
                _ ->
                    Data2 = Data1 ++ PromptType ++ Rest,
                    get_data(Pid, " ", [Data2|Acc], Lastline1)
            end;
        Error ->
            ?WARNING("Return error: ~p", [Error]),
            Data1 = io_lib:format("telnet send cmd error, cmd: ~p, reason:~p", [Cmd, Error]),
            AllData = string:join(lists:reverse([Data1|Acc]), ?splite),
            {ok, AllData}
    end.

close(Pid) ->
    telnet_client:close(Pid).

init(Opts) ->
    io:format("starting telnet conn ...~p",[Opts]),
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 23),
    Username = proplists:get_value(username, Opts),
    Password = proplists:get_value(password, Opts),
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
                                    {ok,{prompt,Prompt},Rest}
                                    when Prompt=/=?username, Prompt=/=?password ->
                                        ?INFO("get data over.....propmpt:~p,~p", [Prompt, Rest]),
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

