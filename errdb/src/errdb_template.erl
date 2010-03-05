%%%----------------------------------------------------------------------
%%% File    : errdb_template.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Errdb template server
%%% Created : 08 Sep. 2009
%%% License : http://www.opengoss.com/license
%%%
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(errdb_template).

-author('ery.lee@gmail.com').

-behavior(gen_server).

-include("elog.hrl").

-export([start_link/1, list/1, lookup/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(TemplateDir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [TemplateDir], []).

list(file_templates) ->
	ets:foldl(fun({TemplateId, Templates}, Acc) -> 
		case lists:keysearch(create, 1, Templates) of
        {value, {create, Template}} ->
            [{TemplateId, Template} | Acc];
        false ->
            [{TemplateId, "no create template"} | Acc]
        end
	end, [], template);

list(graph_templates) ->
	ets:foldl(fun({TemplateId, Templates}, Acc) -> 
		case lists:keysearch(graph, 1, Templates) of
        {value, {graph, Template}} ->
            [{TemplateId, Template} | Acc];
        false ->
            [{TemplateId, "no graph template"} | Acc]
        end
	end, [], template).

lookup(TemplateId) ->
    ets:lookup(template, TemplateId).

%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([TemplateDir]) ->
	{ok, TemplateFiles} = file:list_dir(TemplateDir),
	ets:new(template, [set, named_table]),
	lists:foreach(fun(TemplateFile) -> 
        case lists:suffix(".templates", TemplateFile) of
        true -> 
		  case file:consult(filename:join(TemplateDir, TemplateFile)) of
			{ok, Terms} ->
				lists:foreach(fun(Term) -> ets:insert(template, Term) end, Terms);
			{error, Reason} ->
				?ERROR("Can't load template file ~p: ~p", [TemplateFile, Reason]),
				{stop, "Cannot load errdb template file: " ++ TemplateFile ++ ", reason: " ++ file:format_error(Reason)}
		  end;
        false ->
            ignore
        end
	end, TemplateFiles),
    ?INFO("Errdb template server is started...[ok]", []),
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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

