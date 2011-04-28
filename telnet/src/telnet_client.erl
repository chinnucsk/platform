-module(telnet_client).

-author("hejin-2011-4-27").

-export([send_cmd/2, get_data/2,
        start/1, stop/1]).

send_cmd(Pid, Cmd) ->
    telnet_conn:send_data(Pid, Cmd).

get_data(Pid, Cmd) ->
    telnet_conn:get_data(Pid, Cmd).


start(Opts) ->
    case telnet_conn:start_link(Opts) of
    {ok, Pid} ->
        {ok, Pid};
    {error, _Error} ->
        {ok, undefined}
    end.

stop(undefined) ->
    ok;

stop(Pid) ->
    telnet_conn:stop(Pid).