%% @author Ery Lee<ery.lee@opengoss.com>

%% @copyright www.opengoss.com

%% @doc Errdb server
-module(errdb).

-author('ery.lee@gmail.com').

-include("elog.hrl").

-include("errdb.hrl").

-export([start_link/2]).

-export([exist/3, 
         list/2, 
         list/3, 
         last/3, 
         info/3, 
         create/3, 
         async_create/3, 
         update/3, 
         fetch/3, 
         tune/3, 
         move/3, 
         delete/3, 
         graph/2, 
         graph/3]).

-behavior(gen_server).

-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2, 
         terminate/2, 
         code_change/3]).

-record(state, {id, 
        rrdcmd, 
        rrdpid, 
        rrdcached, 
        data_folder, 
        graphs_folder}).
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Id, Opts) ->
    gen_server:start_link({local, Id}, ?MODULE, Opts, []).

%%--------------------------------------------------------------------
%% Function: create(rrdb, {Name, Args}) -> ok | {error, {Status, Reason}}
%% Description: create rrdb
%%--------------------------------------------------------------------
exist(Pid, rrdb, DbName) ->
    call(Pid, {exist, rrdb, DbName});

exist(Pid, rrdfile, {DbName, FileName}) ->
    call(Pid, {exist, rrdfile, {DbName, FileName}}).

create(Pid, rrdb, Name) ->
	call(Pid, {create, rrdb, {Name, []}});

create(Pid, rrdfile, {DbName, FileName, Args}) ->
	call(Pid, {create, rrdfile, {DbName, FileName, Args}});

create(Pid, datalog, {DnName, FileName, Datalog}) ->
	call(Pid, {create, datalog, {DnName, FileName, Datalog}});

create(Pid, rrdfile_template, {TemplateName, Params}) ->
	call(Pid, {create, rrdfile_template, {TemplateName, Params}});

create(Pid, rrdgraph_template, {TemplateName, Params}) ->
	call(Pid, {create, rrdgraph_template, {TemplateName, Params}}).

async_create(Pid, datalog, {DnName, FileName, Datalog}) ->
	cast(Pid, {async_create, datalog, {DnName, FileName, Datalog}}).

update(Pid, rrdfile_template, {TemplateName, Params}) ->
	call(Pid, {update, rrdfile_template, {TemplateName, Params}});

update(Pid, rrdgraph_template, {TemplateName, Params}) ->
	call(Pid, {update, rrdgraph_template, {TemplateName, Params}}).

move(Pid, rrdb, {SrcName, DstName}) ->
	call(Pid, {move, rrdb, {SrcName, DstName}});

move(Pid, rrdfile, {DbName, SrcName, DstName}) ->
	call(Pid, {move, rrdfile, {DbName, SrcName, DstName}}).

delete(Pid, rrdb, Name) ->
	call(Pid, {delete, rrdb, Name});	

delete(Pid, rrdfile, {DbName, FileName}) ->
	call(Pid, {delete, rrdfile, {DbName, FileName}});

delete(Pid, rrdfile_template, TemplateName) ->
	call(Pid, {delete, rrdfile_template, TemplateName});

delete(Pid, rrdgraph_template, TemplateName) ->
	call(Pid, {delete, rrdgraph_template, TemplateName}).

fetch(Pid, datalogs, {DbName, FieName, Params})->
	call(Pid, {fetch, datalogs, {DbName, FieName, Params}}).

list(Pid, rrdbs) ->
	call(Pid, {list, rrdbs});

list(Pid, rrdfile_templates) ->
	call(Pid, {list, rrdfile_templates});

list(Pid, rrdgraph_templates) ->
	call(Pid, {list, rrdgraph_templates}).

list(Pid, rrdfiles, DbName) ->
	call(Pid, {list, rrdfiles, DbName}).

info(Pid, rrdfile, {DbName, FileName}) ->
	call(Pid, {info, rrdfile, {DbName, FileName}}). 

last(Pid, rrdfile, {DbName, FileName}) ->
	call(Pid, {last, rrdfile, {DbName, FileName}});

last(Pid, datalog, {DbName, FileName}) ->
	call(Pid, {last, datalog, {DbName, FileName}}).

tune(Pid, rrdfile, {DbName, FileName, Params}) ->
	call(Pid, {tune, rrdfile, {DbName, FileName, Params}}).

graph(Pid, {DbName, FileName, Args}) ->
	call(Pid, {graph, {DbName, FileName, Args}}).

graph(Pid, ImgName, {DbName, FileName, Args}) ->
	call(Pid, {graph, {ImgName, DbName, FileName, Args}}).

call(Pid, Req) ->
	gen_server:call(Pid, Req).

cast(Pid, Msg) ->
	gen_server:cast(Pid, Msg).

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
init(Opts) ->
    process_flag(trap_exit, true),
    {value, Id} = dataset:get_value(id, Opts),
    {value, DataDir} = dataset:get_value(data_dir, Opts),
	DataFolder = filename:join(DataDir, "data"),
	GraphsFolder = filename:join(DataDir, "graphs"),
    {value, RrdCmd} = dataset:get_value(rrdtool_cmd, Opts),
    {ok, Pid} = erlrrd:start_link(RrdCmd),
    {value, CachedSock} = dataset:get_value(rrdcached_sock, Opts),
    CachedSock1 = CachedSock ++ "/rrdcached" ++ integer_to_list(Id) ++ ".sock",
    ok = chash_pg:create(errdb),
    ok = chash_pg:join(errdb, self()),
    ?INFO("errdb~p is started...[ok]", [Id]),
    {ok, #state{id = Id, rrdcmd = RrdCmd, rrdpid = Pid, 
                rrdcached = CachedSock1, 
                data_folder = DataFolder, 
                graphs_folder = GraphsFolder}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({exist, rrdb, DbName}, _From, State) ->
	Dir = filename:join([State#state.data_folder, DbName]),
    Reply = filelib:is_dir(Dir),
    {reply, Reply, State};

handle_call({exist, rrdfile, {DbName, FileName}}, _From, State) ->
    RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
    Reply = filelib:is_file(RRDFile),
    {reply, Reply, State};

handle_call({create, rrdb, {Name, _Args}}, _From, State) ->
	Dir = filename:join([State#state.data_folder, Name]),
	?DEBUG("create rrdb: ~p", [Dir]),
	Reply = case filelib:is_dir(Dir) of
		false ->
            filelib:ensure_dir(Dir),
            case file:make_dir(Dir) of
				ok -> 
                    GraphDir = filename:join(State#state.graphs_folder, Name),
					filelib:ensure_dir(GraphDir),
                    file:make_dir(GraphDir),
					{ok, Name};
				{error, Reason} -> 
                    {error, {500, Reason}}
			end;
		true ->
			{error, {409, "Conflict: the rrdb is existed."}}
	end,
    {reply, Reply, State};

handle_call({create, rrdfile, {DbName, FileName, Args}}, _From, State) ->
    Dir = filename:join([State#state.data_folder, DbName]),
    case filelib:is_dir(Dir) of
    false -> 
        filelib:ensure_dir(Dir),
        file:make_dir(Dir),
        GraphDir = filename:join(State#state.graphs_folder, DbName),
        filelib:ensure_dir(GraphDir),
        file:make_dir(GraphDir);
    true -> 
        ok 
	end,
	RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
	case filelib:is_file(RRDFile) of
	false ->
		Reply = handle_create_rrdfile(RRDFile, Args, State),
		{reply, Reply, State};
	true ->
		{reply, {error, {409, <<"Conflict: rrdfile is existed.">>}}, State}
	end;

handle_call({create, datalog, {DbName, FileName, Datalog}}, _From, State) ->
	RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
	case filelib:is_file(RRDFile) of
	true ->
		Reply = handle_update_rrdfile(RRDFile, Datalog, State),
		{reply, Reply, State};
	false ->
		{reply, {error, {404, <<"rrdfile does not exist.">>}}, State}
	end;

handle_call({create, rrdfile_template, {TemplateName, Params}}, _From, State) ->
	?DEBUG("create rrdfile_template: ~p ~p", [TemplateName, Params]),
	%TODO: unsupported now
	{reply, {error, {500, <<"still not support to create rrdfile template.">>}}, State};

handle_call({create, rrdgraph_template, {TemplateName, Params}}, _From, State) ->
	?DEBUG("create rrdgraph_template: ~p ~p", [TemplateName, Params]),
	%TODO: unsupported now
	{reply, {error, {500, <<"still not support to create graph template.">>}}, State};

handle_call({update, rrdfile_template, {TemplateName, Params}}, _From, State) ->
	?DEBUG("update rrdfile_template: ~p ~p", [TemplateName, Params]),
	%TODO: unsupported now
	{reply, {error, {500, <<"still not support to update rrdfile template.">>}}, State};
	
handle_call({update, rrdgraph_template, {TemplateName, Params}}, _From, State) ->
	?DEBUG("update rrdgraph_template: ~p ~p", [TemplateName, Params]),
	%TODO: unsupported now
	{reply, {error, {500, <<"still not support to update rrdgraph template.">>}}, State};

handle_call({move, rrdb, {SrcName, DstName}}, _From, State) ->
	SrcDir = filename:join(State#state.data_folder, SrcName),
	DstDir = filename:join(State#state.data_folder, DstName),
	Reply = case filelib:is_dir(SrcDir) of
		true ->
			case filelib:is_dir(DstDir) of
				false ->
					case os:cmd("mv "++ SrcDir ++ " " ++ DstDir) of
						[] -> ok;
						Error -> {error, {500, Error}}
					end;
				true ->
					{error, {409, "The destination rrdb is existed."}}
			end;
		false ->
			{error, {404, "The source rrdb is not existed."}}
	end,
	{reply, Reply, State};

handle_call({move, rrdfile, {DbName, SrcName, DstName}}, _From, State) ->
	SrcFile = filename:join(State#state.data_folder, DbName, SrcName),
	DstFile = filename:join(State#state.data_folder, DbName, DstName),
	Reply = case filelib:is_file(SrcFile) of
		true ->
			case filelib:is_dir(DstFile) of
				false ->
					case file:rename(SrcFile, DstFile) of
						ok -> ok;
						{error, Reason} -> {error, {500, atom_to_list(Reason)}}
					end;
				true ->
					{error, {409, "The destination rrdfile is existed."}}
			end;
		false ->
			{error, {404, "The source rrdfile is not existed."}}
	end,
	{reply, Reply, State};

handle_call({delete, rrdb, DbName}, _From, State) ->
	DbDir = filename:join(State#state.data_folder, DbName),
	Reply = case file:del_dir(DbDir) of
		ok -> ok;
		{error, Reason} -> {error, {500, atom_to_list(Reason)}}
	end,
	{reply, Reply, State};

handle_call({delete, rrdfile, {DbName, FileName}}, _From, State) ->
	RRDFile = filename:join(State#state.data_folder, DbName, FileName),
	Reply = case filelib:is_file(RRDFile) of
		true -> 
			case file:delete(RRDFile) of
				ok -> ok;
				{error, Reason} -> {error, {500, atom_to_list(Reason)}} 
			end;
		false ->
			{error, {404, "The rrdfile is not existed"}}
	end,
	{reply, Reply, State};

handle_call({delete, rrdfile_template, TemplateName}, _From, State) ->
	?DEBUG("delete rrdfile template: ~p", [TemplateName]),
	{reply, "still not support to delete rrdfile template", State};

handle_call({delete, rrdgraph_template, TemplateName}, _From, State) ->
	?DEBUG("delete rrdgraph template: ~p", [TemplateName]),
	{reply, "still not support to delete rrdgraph template", State};

handle_call({info, rrdfile, {DbName, FileName}}, _From, #state{rrdpid = Pid} = State) ->
	RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
	Reply = case erlrrd:info(Pid, RRDFile) of
		{ok, Resp} -> {ok, Resp};
		{error, Reason} -> {error, {500, Reason}}
	end,
	{reply, Reply, State};

handle_call({last, rrdfile, {DbName, FileName}}, _From, #state{rrdpid = Pid} = State) ->
	RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
	Reply = case erlrrd:last(Pid, RRDFile) of
		{ok, Resp} -> {ok, Resp};
		{error, Reason} -> {error, {500, Reason}}
	end,
	{reply, Reply, State};

handle_call({last, datalog, {DbName, FileName}}, _From, #state{rrdpid = Pid} = State) ->
	RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
	Reply = case erlrrd:lastupdate(Pid, RRDFile) of
		{ok, Resp} -> {ok, Resp};
		{error, Reason} -> {error, {500, Reason}}
	end,
	{reply, Reply, State};

handle_call({tune, rrdfile, {DbName, FileName, Params}}, _From, State) ->
	?DEBUG("tune ~p ~p ~p", [DbName, FileName, Params]),
	{reply, "still not support to tune rrdfile", State};

handle_call({list, rrdbs}, _From, State) ->
	Reply = case file:list_dir(State#state.data_folder) of
        {error, Reason} -> {error, {500, Reason}};
        {ok, RRDbs} -> {ok, RRDbs}
    end,
	{reply, Reply, State};

handle_call({list, rrdfiles, DbName}, _From, State) ->
	Reply = case file:list_dir(filename:join(State#state.data_folder, DbName)) of
        {error, Reason} -> {error, {500, Reason}};
        {ok, RRDbs} -> {ok, RRDbs}
    end,
	{reply, Reply, State};

handle_call({list, rrdfile_templates}, _From, State) ->
    Result = errdb_template:list(file_templates),
	{reply, {ok, Result}, State};

handle_call({list, rrdgraph_templates}, _From, State) ->
    Result = errdb_template:list(graph_templates),
	{reply, {ok, Result}, State};

handle_call({graph, {DbName, FileName, Args}}, _From, State) ->
    {value, ImgFormat} = dataset:get_value(imgformat, Args, "png"),
	RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
	ImgFile = filename:join([State#state.graphs_folder, DbName, uuid:gen() ++ "." ++ string:to_lower(ImgFormat)]),
	Reply = case filelib:is_file(RRDFile) of
	true ->
		case handle_graph_rrdfile(ImgFile, RRDFile, Args, State) of
			{ok, _Resp} -> 
				case file:read_file(ImgFile) of
					{ok, Binary} -> file:delete(ImgFile), {ok, Binary};
					{error, Reason} -> {error, {500, atom_to_list(Reason)}}
				end;
			{error, Reason} -> 
				{error, Reason}
		end;
	false ->
		{error, {404, "rrdfile does not exist."}}
	end,
	{reply, Reply, State};

handle_call({graph, {ImgName, DbName, FileName, Args}}, _From, State) ->
	RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
	ImgFile = filename:join([State#state.graphs_folder, DbName, ImgName]),
	case filelib:is_file(RRDFile) of
	true ->
		Reply = handle_graph_rrdfile(ImgFile, RRDFile, Args, State),
		{reply, Reply, State};
	false ->
		{reply, {error, {404, "rrdfile does not exist."}}, State}
	end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({async_create, datalog, {DbName, FileName, Datalog}}, State) ->
	RRDFile = filename:join([State#state.data_folder, DbName, FileName]),
	case filelib:is_file(RRDFile) of
	true ->
		handle_update_rrdfile(RRDFile, Datalog, State),
		{noreply, State};
	false ->
        ?ERROR("rrdfile does not exist for db: ~p", [RRDFile]),
		{noreply, State}
	end;

handle_cast(_Msg, State) ->
    {noreply, State}.
%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({'EXIT', Pid, Reason}, #state{rrdcmd = RrdCmd, rrdpid = Pid} = State) ->
    ?WARNING("erlrrd is crashed: ~p", [Reason]),
    {ok, Pid} = erlrrd:start_link(RrdCmd),
    {noreply, State#state{rrdpid = Pid}};
    
handle_info(Info, State) ->
    ?ERROR("Unexpeted info: ~p", [Info]),
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
handle_create_rrdfile(RRDFile, Args, State) ->
	handle_template_action(create, RRDFile, Args, State).

handle_update_rrdfile(RRDFile, Datalog, State) ->
	handle_template_action(update, RRDFile, Datalog, State).

handle_graph_rrdfile(ImgFile, RRDFile, Args, State) ->
	handle_template_action(graph, RRDFile, [{imagefile, ImgFile} | Args], State).

handle_template_action(Action, RRDFile, Args, #state{id = Id, rrdpid = Pid, rrdcached = CAddr} = _State) ->
	case lists:keysearch(template, 1, Args) of
		{value, {template, TemplateId}} ->
			case errdb_template:lookup(TemplateId) of
				[{_, Templates}] ->
					case lists:keysearch(Action, 1, Templates) of
						{value, {_, Template}} ->
							Cmd = parse_cmd(Template, [{rrdfile, RRDFile}, {rrdcached, " --daemon " ++ CAddr} | Args], []),
							?INFO("Action: ~p,~p, ~p", [Action, Id, Cmd]),
							case apply(erlrrd, Action, [Pid, Cmd]) of
								{ok, Resp} -> {ok, Resp};
								{error, Error} -> 
                                    ?ERROR("error in action: ~p, ~p, ~p, ~p", [Action, Id, Cmd, Error]),
                                    {error, {500, Error}}
							end;
						false ->
							{error, {500, "Cannot find create template: " ++ TemplateId}}
					end;
				[] ->
					{error, {500, "Cannot find template: " ++ TemplateId}}
			end;	
		false ->
			{error, {400, "No 'template' in params"}}
	end.

parse_cmd([H|T],Args,Acc) ->
	if 
		is_atom(H) -> 
			case lists:keysearch(H,1,Args) of
				{value,Value} ->
					parse_cmd(T,Args,[to_string(erlang:element(2,Value))|Acc]);
				false ->
					parse_cmd(T,Args,[atom_to_list(H)|Acc])
			end;
		true -> 
			parse_cmd(T, Args, [H|Acc])
	end;

parse_cmd([],_Args,Acc) -> 
    lists:reverse(Acc).

to_string(Value) ->
    if
    	is_integer(Value) -> integer_to_list(Value);
		is_float(Value) -> float_to_list(Value);
        true -> Value
    end.         
