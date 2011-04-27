-module(telnet_app).

-author("hejin-2011-4-27").

-behavior(application).

-export([start/0, start/2, stop/1]).

start() ->
    application:start(crypto),
    io:format("starting telnet..."),
	application:start(telnet).

start(normal, _Args) ->
	telnet_sup:start_link(application:get_all_env()).

stop(_) ->
	ok.
