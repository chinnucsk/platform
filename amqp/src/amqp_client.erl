-module(amqp_client).

-include("elog.hrl").

-export([start/4, stop/1]).

start(Name, Opts, Succ, Fail) ->
    case amqp:start_link(Name, Opts) of
    {ok, Pid} ->
        Succ(Pid),
        {ok, Pid};
    {error, Error} ->
        Fail(Error),
        {ok, undefined}
    end.

stop(undefined) ->
    ok;

stop(Pid) ->
    amqp:stop(Pid).

