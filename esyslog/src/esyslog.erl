%%%----------------------------------------------------------------------
%%% @author Ery Lee <ery.lee@gmail.com>
%%%  [ http://www.opengoss.com ]
%%% @copyright 2011 opengoss
%%% @doc erlang syslog client, send syslog by udp 
%%% @end
%%%----------------------------------------------------------------------
-module(esyslog).

-author('ery.lee@gmail.com').

-include("elog.hrl").

-import(proplists, [get_value/2, get_value/3]).

-behavior(gen_server).

-export([start_link/1,
        set_severity/1,
        get_severity/0,
		crit/4,
        error/4,
        warn/4,
        info/4,
		debug/4,
		syslog/5,
        stop/0]).

-export([init/1,
        handle_call/3,
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-define(LOG_EMERGENCY, 0). % system is unusable
-define(LOG_ALERT,     1). % action must be taken immediately
-define(LOG_CRITICAL,  2). % critical conditions
-define(LOG_ERROR,     3). % error conditions
-define(LOG_WARNING,   4). % warning conditions
-define(LOG_NOTICE,    5). % normal but significant condition
-define(LOG_INFO,      6). % informational
-define(LOG_DEBUG,     7). % debug-level messages

% facility codes
-define(FAC_KERN,     (0 bsl 3)). % kernel messages
-define(FAC_USER,     (1 bsl 3)). % random user-level messages
-define(FAC_MAIL,     (2 bsl 3)). % mail system
-define(FAC_DAEMON,   (3 bsl 3)). % system daemons
-define(FAC_AUTH,     (4 bsl 3)). % security/authorization messages
-define(FAC_SYSLOG,   (5 bsl 3)). % messages generated internally by syslogd
-define(FAC_LPR,      (6 bsl 3)). % line printer subsystem
-define(FAC_NEWS,     (7 bsl 3)). % network news subsystem
-define(FAC_UUCP,     (8 bsl 3)). % UUCP subsystem
-define(FAC_CRON,     (9 bsl 3)). % clock daemon
-define(FAC_AUTHPRIV, (10 bsl 3)). % security/authorization messages (private)
-define(FAC_FTP,      (11 bsl 3)). % ftp daemon

-define(FAC_LOCAL0,   (16 bsl 3)). % reserved for local use
-define(FAC_LOCAL1,   (17 bsl 3)). % reserved for local use
-define(FAC_LOCAL2,   (18 bsl 3)). % reserved for local use
-define(FAC_LOCAL3,   (19 bsl 3)). % reserved for local use
-define(FAC_LOCAL4,   (20 bsl 3)). % reserved for local use
-define(FAC_LOCAL5,   (21 bsl 3)). % reserved for local use
-define(FAC_LOCAL6,   (22 bsl 3)). % reserved for local use
-define(FAC_LOCAL7,   (23 bsl 3)). % reserved for local use

-record(syslog, {facility = ?FAC_USER, severity, module, pid, line, format, args}).

-record(state, {fd, host, port}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Opts) ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

get_severity() ->
    [{_, Severity}] = ets:lookup(esyslog, severity),
    severity(Severity).

set_severity(Severity) ->
    gen_server2:call(?MODULE, {set_severity, severity(Severity)}).

crit(Mod, Line, Format, Args) ->
    syslog(?LOG_CRITICAL, Mod, Line, Format, Args).

error(Mod, Line, Format, Args) ->
    syslog(?LOG_ERROR, Mod, Line, Format, Args).

warn(Mod, Line, Format, Args) ->
    syslog(?LOG_WARNING, Mod, Line, Format, Args).
    
info(Mod, Line, Format, Args) ->
    syslog(?LOG_INFO, Mod, Line, Format, Args).

debug(Mod, Line, Format, Args) ->
    syslog(?LOG_DEBUG, Mod, Line, Format, Args).

syslog(Severity, Mod, Line, Format, Args) ->
    [{_, Threshold}] = ets:lookup(esyslog, severity),
    if 
    Threshold >= Severity ->
        Syslog = #syslog{severity = Severity, module = Mod, 
            line = Line, pid = self(), format = Format, args = Args},
        gen_server2:cast(?MODULE, {syslog, Syslog});
    true ->
        ignore
    end.

stop() ->
    gen_server2:call(?MODULE, stop).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Opts]) ->
    Host = get_value(host, Opts, "localhost"),
    Port = get_value(port, Opts, 514),
    Severity = get_value(severity, Opts, error),
    ets:new(esyslog, [set, protected, named_table]),
    ets:insert(esyslog, {severity, severity(Severity)}),
	case gen_udp:open(0) of
    {ok, Fd} ->
        {ok, #state{fd=Fd, host=Host, port=Port}};
    {error, Reason} ->
        {stop, Reason}
	end.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({set_severity, Severity}, _From, State) ->
    ets:insert(esyslog, {severity, Severity}),
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {reply, {error, badreq}, State}.

priorities_call(stop, _From, _State) ->
    10;
priorities_call({set_severity, _}, _From, _State) ->
    9;
priorities_call(_, _From, _State) ->
    0.
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({syslog, #syslog{facility = Facility, severity = Severity, 
    module = Mod, pid = Pid, line = Line, format = Format, args = Args}},
    #state{fd = Fd, host = Host, port = Port} = State) ->
	Pri = integer_to_list(Facility bor Severity),
    {ok, HostName} = inet:gethostname(),
    Head = list_to_binary([systime(), " ", HostName]), 
    Tag = io_lib:format("[~p/~p/~p/~p].~p", [node(),Mod,Pid,Line,severity(Severity)]),
    Msg = io_lib:format(Format, Args),
    Pkt = list_to_binary(["<", Pri, ">", Head, " ", Tag, ": ", Msg, "\n"]),
	gen_udp:send(Fd, Host, Port, Pkt),
    {noreply, State};

handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
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

systime() ->
    {_, M, D} = date(),
    {H, MM, S} = time(),
    Date = [month(M), " ",  extbif:zeropad(D)],
    Time = string:join([extbif:zeropad(I) || I <- [H, MM, S]], ":"),
    Date ++ " " ++ Time.

severity(emerg) -> ?LOG_EMERGENCY;
severity(alert) -> ?LOG_ALERT;
severity(crit) -> ?LOG_CRITICAL;
severity(error) -> ?LOG_ERROR;
severity(warn) -> ?LOG_WARNING;
severity(notice) -> ?LOG_NOTICE;
severity(info) -> ?LOG_INFO;
severity(debug) -> ?LOG_DEBUG;

severity(0) -> emerg;
severity(1) -> alert;
severity(2) -> crit;
severity(3) -> error;
severity(4) -> warn;
severity(5) -> notice;
severity(6) -> info;
severity(7) -> debug.

month(1) -> "Jan";
month(2) -> "Feb";
month(3) -> "Mar";
month(4) -> "Apr";
month(5) -> "May";
month(6) -> "Jun";
month(7) -> "Jul";
month(8) -> "Aug";
month(9) -> "Sep";
month(10) -> "Oct";
month(11) -> "Nov";
month(12) -> "Dec".

