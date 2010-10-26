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

-export([timestamp/0, to_list/1, to_binary/1, binary_to_atom/1, to_integer/1]).

%%unit: second
timestamp() ->
	{MegaSecs, Secs, _MicroSecs} = erlang:now(),
	MegaSecs * 1000000 + Secs.

to_list(L) when is_list(L) ->
    L;

to_list(L) when is_atom(L) ->
    atom_to_list(L);

to_list(L) when is_integer(L) ->
    integer_to_list(L);

to_list(L) when is_float(L) ->
    string:join(io_lib:format("~.2f", [L]),"");

to_list(B) when is_binary(B) ->
    binary_to_list(B).

to_binary(B) when is_binary(B) ->
    B;
to_binary(B) when is_integer(B) ->
    B;
to_binary(L) when is_list(L) ->
    list_to_binary(L).

to_integer(I) when is_integer(I) ->
    I;
to_integer(I) when is_list(I) ->
    case string:str(I, ".") of
        0 ->
           case string:to_integer(I) of
               {error, _} ->
                    0;
               {Value0 ,_}  ->
                    Value0
           end;
        _ ->
            {Value0 ,_} = string:to_float(I),
             Value0
    end;
 to_integer(I) when is_binary(I) ->
     list_to_integer(binary_to_list(I));
 to_integer(I) ->
     0.

binary_to_atom(B) ->
    list_to_atom(binary_to_list(B)).


