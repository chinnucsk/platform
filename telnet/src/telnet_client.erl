-module(telnet_client).

-author("hejin-2011-4-27").

-export([send_cmd/1]).

send_cmd(Cmd) ->
    telnet_conn:send_data(Cmd).
