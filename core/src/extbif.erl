%%%----------------------------------------------------------------------
%%% File    : extbif.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Extended BIF
%%% Created : 08 Dec 2009
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(extbif).

-export([timestamp/0, to_list/1, to_binary/1]).

%%unit: second
timestamp() ->
	{MegaSecs, Secs, _MicroSecs} = erlang:now(),
	MegaSecs * 1000000 + Secs.

to_list(L) when is_list(L) ->
    L;

to_list(L) when is_integer(L) ->
    integer_to_list(L);

to_list(B) when is_binary(B) ->
    binary_to_list(B).

to_binary(B) when is_binary(B) ->
    B;

to_binary(L) when is_list(L) ->
    list_to_binary(L).


