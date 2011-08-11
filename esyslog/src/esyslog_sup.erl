%%%----------------------------------------------------------------------
%%% @author Ery Lee <ery.lee@gmail.com>
%%%  [ http://www.opengoss.com ]
%%% @copyright 2011 opengoss
%%% @doc esyslog supervisor
%%% @end
%%%----------------------------------------------------------------------
-module(esyslog_sup).

-author('ery.lee@gmail.com').

-behavior(supervisor).

%% API
-export([start_link/1, init/1]).

start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Opts).  

init(Opts) ->
	Esyslog = {esyslog, {esyslog, start_link, [Opts]}, 
		permanent, 10, worker, [esyslog]},
    {ok, {{one_for_one, 1, 10}, [Esyslog]}}.
