%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Errdb application.

-module(errdb_app).

-export([start/0, stop/0]).

-behavior(application).
%callback
-export([start/2, stop/1]).

%%@spec start() -> ok
%%@doc Start the errdb server
start() -> 
	errdb_deps:ensure(),
    ensure_started(crypto),
    init_elog(),
    application:start(core),
	application:start(errdb).

ensure_started(App) ->
    case application:start(App) of
    ok ->
        ok;
    {error, {already_started, App}} ->
        ok
    end.

init_elog() ->
    {ok, [[LogLevel]]} = init:get_argument(log_level),
    {ok, [[LogPath]]} = init:get_argument(log_path),
	elog:init(list_to_integer(LogLevel), LogPath).

%%@spec stop() -> ok
%%@doc Stop the errdb server
stop() -> 
    Res = application:stop(errdb),
    application:stop(core),
    application:stop(crypto),
    Res.

start(_Type, _Args) ->
    {ok, [[PoolSize]]} = init:get_argument(rrdtool_pool),
    {ok, [[CachedSock]]} = init:get_argument(rrdcached_sock), 
    Env = application:get_all_env(),
	errdb_sup:start_link([{rrdtool_pool, list_to_integer(PoolSize)}, {rrdcached_sock, CachedSock}|Env]).

stop(_State) ->
	ok.

