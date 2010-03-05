%% @author Ery Lee<ery.lee@opengoss.com>
%% @copyright www.opengoss.com

%% @doc Supervisor for the errdb application.

-module(errdb_sup).

-author('Ery Lee<ery.lee@opengoss.com>').

-behaviour(supervisor).

%% External exports
-export([start_link/1, upgrade/0]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link(Opts) -> ServerRet
%%  Opts = list()
%% @doc API for starting the supervisor.
start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Opts]).

%% @spec upgrade() -> ok
%% @doc Add processes if necessary.
upgrade() ->
    {ok, {_, Specs}} = init([]),

    Old = sets:from_list(
            [Name || {Name, _, _, _} <- supervisor:which_children(?MODULE)]),
    New = sets:from_list([Name || {Name, _, _, _, _, _} <- Specs]),
    Kill = sets:subtract(Old, New),

    sets:fold(fun (Id, ok) ->
                      supervisor:terminate_child(?MODULE, Id),
                      supervisor:delete_child(?MODULE, Id),
                      ok
              end, ok, Kill),

    [supervisor:start_child(?MODULE, Spec) || Spec <- Specs],
    ok.

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([Opts]) ->
    Monitor = {errdb_monitor, {errdb_monitor, start_link, []},
           permanent, 10, worker, [errdb_monitor]},

	{value, TemplateDir} = dataset:get_value(template_dir, Opts),
    Template = {errdb_template, {errdb_template, start_link, [TemplateDir]},
           permanent, 10, worker, [errdb_template]},

    {value, DataDir} = dataset:get_value(data_dir, Opts),
    {value, PoolSize} = dataset:get_value(rrdtool_pool, Opts),
    {value, CachedSock} = dataset:get_value(rrdcached_sock, Opts),
    {value, Cmd} = dataset:get_value(rrdtool_cmd, Opts),
    Errdbs = [ begin
        Id = list_to_atom("errdb_" ++ integer_to_list(I)), 
        ErrdbOpts = [{id, I}, {data_dir, DataDir}, 
            {rrdtool_cmd, Cmd}, {rrdcached_sock, CachedSock}],
        {Id, {errdb, start_link, [Id, ErrdbOpts]}, permanent, 10, worker, [errdb]}
      end || I <- lists:seq(0, PoolSize)],

	%% Httpd config
	{ok, HttpdConf} = case application:get_env(httpd) of
		{ok, Conf} ->
			NewConf = lists:keymerge(1, Conf, [{ip, mochiweb(ip)}, {port, mochiweb(port)}, {docroot, errdb_deps:local_path(["priv", "www"])}]),
			{ok, NewConf};
		false ->
			{ok, [{ip, mochiweb(ip)}, {port, mochiweb(port)}, {docroot, errdb_deps:local_path(["priv", "www"])}]}
	end,

	%% Httpd 
    Httpd = {errdb_httpd, {errdb_httpd, start, [HttpdConf]},
           permanent, 10, worker, [errdb_httpd]},

	%% Errdb
    {ok, {{one_for_one, 10, 100}, lists:append([[Monitor, Template], Errdbs, [Httpd]])}}.

mochiweb(ip) ->
	case os:getenv("MOCHIWEB_IP") of false -> "0.0.0.0"; Any -> Any end;   

mochiweb(port) ->
	case os:getenv("MOCHIWEB_PORT") of false -> 8000; Any -> Any end;

mochiweb(docroot) ->
	["priv", "www"].

