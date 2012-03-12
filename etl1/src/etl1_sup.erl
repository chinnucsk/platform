-module(etl1_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Opts]).



init([Opts]) ->
    Broker = proplists:get_value(broker, Opts),
    Etl1Agent = {etl1_agent, {etl1_agent, start_link, [Broker]},
        permanent, 10, worker, [etl1_agent]},
    Tl1Options = proplists:get_value(ems, Opts),
    Etl1 = {etl1, {etl1, start_link, [Tl1Options]},
        permanent, 10, worker, [etl1]},
    Etl1TcpSub = {etl1_tcp_sup, {etl1_tcp_sup, start_link, [Tl1Options]},
        temporary, infinity , supervisor, [etl1_tcp_sup]},
	{ok, {{one_for_all, 10, 100}, [Etl1Agent, Etl1, Etl1TcpSub]}}.