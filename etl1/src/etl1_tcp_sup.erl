-module(etl1_tcp_sup).

-created("hejin 2012-3-12").

-behaviour(supervisor).

-export([start_link/0, start_child/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, etl1_tcp_sup}, ?MODULE, []).

start_child(Params) ->
    supervisor:start_child(etl1_tcp_sup, Params).

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
        [{etl1_tcp, {etl1_tcp, start_link, []},
            transient , brutal_kill, worker, [etl1_tcp]}]}}.