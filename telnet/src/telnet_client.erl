-module(telnet_client).

-author("hejin-2011-4-27").

-export([send_cmd/1,
        start/4, stop/1]).

send_cmd(Cmd) ->
    telnet_conn:send_data(Cmd).


start(Opts) ->
    case telnet_conn:start_link(Opts) of
    {ok, Pid} ->
        {ok, Pid};
    {stop, _Error} ->
        {ok, undefined}
    end.

stop(undefined) ->
    ok;

stop(Pid) ->
    telnet_conn:stop(Pid).