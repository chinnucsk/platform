%%%----------------------------------------------------------------------
%%% File    : errdb_client.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : errdb client
%%% Created : 18 Feb 2008
%%% Updated : 22 DEC 2009
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(errdb_client).

-author('ery.lee@gmail.com').

-include("elog.hrl").

-compile(export_all).

-behavior(gen_server).

-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2, 
         terminate/2, 
         code_change/3]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

lookup(cache, RRDKey) ->
    ets:lookup(errdb_rrdfile, RRDKey).

insert(cache, RRDKey) ->
    gen_server:cast(?MODULE, {cache, RRDKey}).

exist(rrdb, DbName) ->
    call(get_pid(DbName), {exist, rrdb, DbName});

exist(rrdfile, {DbName, FileName}) ->
    call(get_pid(DbName ++ FileName), {exist, rrdfile, {DbName, FileName}}).

create(rrdb, Name) ->
	call(get_pid(Name), {create, rrdb, {Name, []}});

create(rrdfile, {DbName, FileName, Args}) ->
	call(get_pid(DbName ++ FileName), {create, rrdfile, {DbName, FileName, Args}});

create(datalog, {DbName, FileName, Datalog}) ->
	call(get_pid(DbName ++ FileName), {create, datalog, {DbName, FileName, Datalog}});

create(rrdfile_template, {TemplateName, Params}) ->
	call(get_pid(TemplateName), {create, rrdfile_template, {TemplateName, Params}});

create(rrdgraph_template, {TemplateName, Params}) ->
	call(get_pid(TemplateName), {create, rrdgraph_template, {TemplateName, Params}}).

async_create(datalog, {DbName, FileName, Datalog}) ->
	cast(get_pid(DbName ++ FileName), {async_create, datalog, {DbName, FileName, Datalog}}).

update(rrdfile_template, {TemplateName, Params}) ->
	call(get_pid(TemplateName), {update, rrdfile_template, {TemplateName, Params}});

update(rrdgraph_template, {TemplateName, Params}) ->
	call(get_pid(TemplateName), {update, rrdgraph_template, {TemplateName, Params}}).

move(rrdb, {SrcName, DstName}) ->
	call(get_pid(SrcName), {move, rrdb, {SrcName, DstName}});

move(rrdfile, {DbName, SrcName, DstName}) ->
	call(get_pid(DbName ++ SrcName), {move, rrdfile, {DbName, SrcName, DstName}}).

delete(rrdb, Name) ->
	call(get_pid(Name), {delete, rrdb, Name});	

delete(rrdfile, {DbName, FileName}) ->
	call(get_pid(DbName ++ FileName), {delete, rrdfile, {DbName, FileName}});

delete(rrdfile_template, TemplateName) ->
	call(get_pid(TemplateName), {delete, rrdfile_template, TemplateName});

delete(rrdgraph_template, TemplateName) ->
	call(get_pid(TemplateName), {delete, rrdgraph_template, TemplateName}).

fetch(datalogs, {DbName, FileName, Params})->
	call(get_pid(DbName ++ FileName), {fetch, datalogs, {DbName, FileName, Params}}).

list(rrdbs) ->
    RandId = integer_to_list(random:uniform(100)),
	call(get_pid(RandId), {list, rrdbs});

list(rrdfile_templates) ->
    RandId = integer_to_list(random:uniform(100)),
	call(get_pid(RandId), {list, rrdfile_templates});

list(rrdgraph_templates) ->
    RandId = integer_to_list(random:uniform(100)),
	call(get_pid(RandId), {list, rrdgraph_templates}).

list(rrdfiles, DbName) ->
	call(get_pid(DbName), {list, rrdfiles, DbName}).

info(rrdfile, {DbName, FileName}) ->
	call(get_pid(DbName ++ FileName), {info, rrdfile, {DbName, FileName}}). 

last(rrdfile, {DbName, FileName}) ->
	call(get_pid(DbName ++ FileName), {last, rrdfile, {DbName, FileName}});

last(datalog, {DbName, FileName}) ->
	call(get_pid(DbName ++ FileName), {last, datalog, {DbName, FileName}}).

tune(rrdfile, {DbName, FileName, Params}) ->
	call(get_pid(DbName ++ FileName), {tune, rrdfile, {DbName, FileName, Params}}).

graph({DbName, FileName, Args}) ->
    call(get_pid(DbName ++ FileName), {graph, {DbName, FileName, Args}}).

graph(ImgName, {DbName, FileName, Args}) ->
	call(get_pid(DbName ++ FileName), {graph, {ImgName, DbName, FileName, Args}}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    chash_pg:create(errdb),
	ets:new(errdb_rrdfile, [set, named_table]),
	?INFO("Errdb client is started...[ok]", []),
    {ok, state}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
    ?ERROR("Unexpected request: ~p", [Request]),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({cache, RRDKey}, State) ->
    ets:insert(errdb_rrdfile, {RRDKey, true}),
    {noreply, State};

handle_cast(Msg, State) ->
    ?ERROR("Unexpected Msg: ~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    ?ERROR("Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
call(Pid, Req) ->
    gen_server:call(Pid, Req).

cast(Pid, Msg) ->
    gen_server:cast(Pid, Msg).

get_pid(Key) ->
    chash_pg:get_pid(errdb, Key).
