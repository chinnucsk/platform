-module(erlrrd).

-export([create/2, update/2, updatev/2, dump/2, restore/2, last/2, first/2, info/2, fetch/2, tune/2, resize/2, xport/2, graph/2, lastupdate/2, ls/1, cd/2, mkdir/2, pwd/1]).

-export([start_link/1]).

-export([combine/1, c/1]).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {port}).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @spec start_link(RRDToolCmd) -> Result
%%   RRDToolCmd = [ term() ]
%%   Result = {ok,Pid} | ignore | {error,Error}
%%     Pid = pid()
%%     Error = {already_started,Pid} | shutdown | term()
%% @doc calls gen_server:start_link
%%   RRDToolCmd is the command passed to open_port()
%%   usually "rrdtool -"
start_link(RRDToolCmd) ->
  gen_server:start_link(?MODULE, RRDToolCmd, []).

%% @spec combine(List) -> List
%%   List = [ term() ]
%% @doc "joins" and quotes the given arg list. 
%%   takes a list of arguments, and returns a deeplist with 
%%   each argument surrounded by double quotes
%%   then separated by spaces
%%
%%   combine(["these", "are", "my args"]). ->
%%
%%   [["\"","these","\""]," ",["\"","are","\""]," ",["\"","my args","\""]]
%%
%%   it is intended as a convinence function to the 
%%   rrdtool commands which all take a single iodata() argument
%%   which represents the string to be passed as the arguments 
%%   to the corresponding rrdtool command.  
%%
%%   erlrrd:xport(erlrrd:c(["DEF:foo=/path with/space/foo.rrd:foo:AVERAGE", "XPORT:foo"])).
combine(Args) -> 
    join([ [ "\"", X, "\"" ] || X <- Args ], " ").
%% @spec c(List) -> List
%%   List = [ term() ]
% @equiv combine(Args)
c(Args) -> combine(Args).


% rrdtool commands

%% @spec create(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Set up a new Round Robin Database (RRD). Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdcreate.en.html rrdcreate].
create     (Pid, Args) when is_list(Args) -> do(Pid, create, Args).

%% @spec update(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Store new data values into an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdupdate.en.html rrdupdate].
update	   (Pid, Args) when is_list(Args) -> do(Pid, update, Args).

%% @spec updatev(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Operationally equivalent to update except for output. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdupdate.en.html rrdupdate].
updatev   (Pid, Args) when is_list(Args) -> do(Pid, updatev, Args).

%% @spec dump(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc    Dump the contents of an RRD in plain ASCII. In connection with
%%         restore you can use this to move an RRD from one computer
%%         architecture to another.  Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrddump.en.html rrddump].
dump (Pid, Args) when is_list(Args) -> do(Pid, dump, Args).

%% @spec restore(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Restore an RRD in XML format to a binary RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdrestore.en.html rrdrestore]
restore (Pid, Args) when is_list(Args) -> do(Pid, restore, Args).

%% @spec last(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = integer()
%% @doc    Return the date of the last data sample in an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdlast.en.html rrdlast]
last       (Pid, Args) when is_list(Args) -> 
  case do(Pid, last, Args) of
    { error, Reason } -> { error, Reason };
    { ok, [[Response]] } -> { ok, erlang:list_to_integer(Response) }
  end.

%% @spec lastupdate(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc    Return the most recent update to an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdlastupdate.en.html rrdlastupdate]
lastupdate (Pid, Args) when is_list(Args) -> do(Pid, lastupdate, Args).

%% @spec first(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = integer()
%% @doc Return the date of the first data sample in an RRA within an
%%       RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdfirst.en.html rrdfirst]
first       (Pid, Args) when is_list(Args) -> 
  case do(Pid, first, Args) of
    { error, Reason } -> { error, Reason };
    { ok, [[Response]] } -> { ok, erlang:list_to_integer(Response) }
  end.

%% @spec info(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Get information about an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdinfo.en.html rrdinfo].
info       (Pid, Args) when is_list(Args) -> do(Pid, info, Args).

%% @spec fetch(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc   Get data for a certain time period from a RRD. The graph func-
%%         tion uses fetch to retrieve its data from an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdfetch.en.html rrdfetch].
fetch      (Pid, Args) when is_list(Args) -> do(Pid, fetch, Args).

%% @spec tune(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Alter setup of an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdtune.en.html rrdtune].
tune       (Pid, Args) when is_list(Args) -> do(Pid, tune, Args).

%% @spec resize(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc    Change the size of individual RRAs. This is dangerous! Check
%%         rrdresize.
resize     (Pid, Args) when is_list(Args) -> do(Pid, resize, Args).

%% @spec xport(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc   Export data retrieved from one or several RRDs. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdxport.en.html rrdxport]
%%
%%  erlrrd:xport("'DEF:foo=/path with/space/foo.rrd:foo:AVERAGE' XPORT:foo").
%%
%%  erlrrd:xport(erlrrd:c(["DEF:foo=/path with/space/foo.rrd:foo:AVERAGE", "XPORT:foo"])).
xport      (Pid, Args) when is_list(Args) -> do(Pid, xport, Args).

%% @spec graph(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc   Create a graph from data stored in one or several RRDs. Apart
%%         from generating graphs, data can also be extracted to stdout.
%%         Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdgraph.en.html rrdgraph].
graph      (Pid, Args) when is_list(Args) -> 
  % TODO: scan for this pattern w/out flattening the io_list? 
  % TODO: support  graphing to stdout!!! :) 
  Flat = erlang:binary_to_list(erlang:iolist_to_binary(Args)),
  case regexp:match(Flat, "(^| )-( |$)") of
    { match, _, _ } -> 
      % graph to stdout will break this Ports parsing of reponses..
      { error, "Graphing to stdout not supported." };
    nomatch -> 
      do(Pid, graph, Args)
  end.

% rrd "remote" commands
%% @spec cd(pid(), erlang:iodata()) -> ok |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc   ask the rrdtool unix process to change directories
%% 
%%    erlrrd:cd("/usr/share/rrd/data").
%%
%%    erlrrd:cd(erlrrd:combine(["/Users/foo/Library/Application Support/myapp/rrd"]).
cd         (Pid, Arg)  when is_list(Arg) -> 
  case do(Pid, cd, Arg) of
    { error, Reason } -> { error, Reason };
    { ok, _ } -> ok
  end.

%% @spec mkdir(pid(), erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc   ask the rrdtool unix process to create a directory
mkdir      (Pid, Arg)  when is_list(Arg) -> do(Pid, mkdir, Arg).

%% @spec ls(pid()) -> { ok, Response }  | { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc  lists all *.rrd files in rrdtool unix process'
%%       current working directory
ls         (Pid)     -> do(Pid, ls, []).

%% @spec pwd(pid()) -> { ok, Response }  |  { error, Reason } 
%%  Reason = iolist()
%%  Response = string()
%% @doc  return the rrdtool unix process'
%%       current working directory.
pwd        (Pid)     -> 
  case do(Pid, pwd, []) of
    {error, Reason} -> {error, Reason };
    {ok, [[Response]]} -> { ok, Response }
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen server interface poo
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @hidden
init(RRDToolCmd) -> 
  process_flag(trap_exit, true),
  Port = erlang:open_port(
    {spawn, RRDToolCmd}, 
    [{line, 50000}, eof, exit_status, stream]),
  {ok, #state{port = Port}}.

%%
%% handle_call
%% @hidden
handle_call({do, Action, Args, Timeout}, _From, #state{port = Port} = State) ->
  Line = [erlang:atom_to_list(Action), " ", Args , "\n"],
  case port_command(Port, Line) of %, [nosuspend]
  true ->
      case collect_response(Port, Timeout) of
            {response, Response} -> 
                {reply, {ok, Response}, State};
            {error, timeout} ->
                {error, "port timeout", State};
            {error, Error} -> 
                {reply, {error, Error}, State}
      end;
  false ->
    {reply, {error, "port busy"}, State}
  end.

%% handle_cast
%% @hidden
handle_cast(Msg, State) -> 
  io:format(user, "Got unexpected cast msg: ~p~n", [Msg]),
  {noreply, State}.

%% handle_info
%% @hidden
handle_info(Msg, State) -> 
  io:format(user, "Got unexpected info msg: ~p~n", [Msg]),
  {noreply, State}.

%% terminate
%% @hidden
terminate(_Reason, _State) -> ok.

%% code_change
%% @hidden
code_change(_OldVsn, State, _Extra) -> 
  {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Private poo
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
do(Pid, Command, Args) -> 
  do(Pid, Command, Args, 4000).
do(Pid, Command, Args, Timeout) -> 
  case has_newline(Args) of
    true -> {error, "No newlines."};
    false -> gen_server:call(Pid, {do, Command, Args , Timeout}) 
  end.

join([Head | [] ], _Sep) ->
  [Head];
join([Head | Tail], Sep) ->
  [ Head, Sep | join(Tail, Sep) ].

has_newline([]) -> false;
has_newline(<<>>) -> false;
has_newline([ H |  T]) 
  when is_list(H); is_binary(H) ->
    case has_newline(H) of
      true -> true;
      false -> has_newline(T)
    end;
has_newline([ H | T]) when is_integer(H) ->
  if 
    H =:= $\n -> true;
    true -> has_newline(T)
  end;
has_newline(<<H:8,T/binary>>) ->
  if 
    H =:= $\n -> true;
    true -> has_newline(T)
  end.

collect_response(Port, Timeout ) ->
    collect_response(Port, [], [], Timeout ).

collect_response(Port, RespAcc, LineAcc, Timeout) ->
    receive
        {Port, {data, {eol, "OK u:" ++ _T }}} ->
            {response, lists:reverse(RespAcc)};
        {Port, {data, {eol, "ERROR: " ++ Error }}} ->
            {error, [ Error, lists:reverse(RespAcc)]};
        {Port, {data, {eol, Result}}} ->
			Line = lists:reverse([Result | LineAcc]),
			collect_response(Port, [Line | RespAcc], [], Timeout);
        {Port, {data, {noeol, Result}}} ->
            collect_response(Port, RespAcc, [Result | LineAcc], Timeout)
    %% Prevent the gen_server from hanging indefinitely in case the
    %% spawned process is taking too long processing the request.
    after Timeout ->  % TODO user configurable timeout.
            { error, timeout }
    end.

