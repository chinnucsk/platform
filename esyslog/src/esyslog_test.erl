-module(esyslog_test).

-include("esyslog.hrl").

-compile(export_all).

run(Host) ->
    esyslog:start_link([{host, Host}]),
    ?SYSLOG_ERR("~s", ["fuck.....test"]),
    ?SYSLOG_ERR("~s", ["shitk.....test"]).

