%%%----------------------------------------------------------------------
%%% @author Ery Lee <ery.lee@gmail.com>
%%%  [ http://www.opengoss.com ]
%%% @copyright 2011 opengoss
%%% @doc esyslog application
%%% @end
%%%----------------------------------------------------------------------
-module(esyslog_app).

-author('ery.lee@gmail.com').

-behavior(application).

-export([start/0, 
		stop/0,
		start/2,
		stop/1]).

start() -> 
	elog:init(5, "esyslog.log"),
    io:format("starting esyslog..."),
	application:start(esyslog).

stop() ->
	application:stop(esyslog).

start(normal, _Args) ->
	esyslog_sup:start_link(application:get_all_env()).

stop(_) ->
	ok.

