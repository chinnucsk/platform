%%%----------------------------------------------------------------------
%%% File    : core_app.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : core application
%%% Created : 24 Dec 2009
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(core_app).

-author('ery.lee@gmail.com').

-behavior(application).

-export([start/2, stop/1]).

start(normal, _Args) ->
	core_sup:start_link().

stop(_) ->
	ok. 

