-module(telnet_sup).

-author("hejin-2011--4-27").

-behavior(supervisor).

%% API
-export([start_link/1, init/1]).

start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Opts).

init(Opts) ->
    Client = {telnet_conn, {telnet_conn, start_link, [Opts]}, permanent, 10, worker, [telnet_conn]},
    {ok, {{one_for_one, 10, 1000}, Client}}.
