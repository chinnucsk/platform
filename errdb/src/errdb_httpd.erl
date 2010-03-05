%% @author Ery Lee<ery.lee@opengoss.com>
%% @copyright www.opengoss.com

%% @doc Http server for errdb.
-module(errdb_httpd).

-export([start/1, stop/0, loop/2]).

-include("elog.hrl").

%% External API
start(Options) ->
    {DocRoot, Options1} = get_option(docroot, Options),
    Loop = fun (Req) -> ?MODULE:loop(Req, DocRoot) end,
    ?INFO("Errdb httpd is started...[ok]", []),
    mochiweb_http:start([{name, ?MODULE}, {loop, Loop} | Options1]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req, _DocRoot) ->
    Method = Req:get(method),
	Path = list_to_tuple(string:tokens(Req:get(path), "/")),
    ?INFO("HTTP Req: ~p", [Path]),
	handle(Method, Path, Req).

handle('GET', {"rrdbs"}, Req) ->
	case errdb_client:list(rrdbs) of
		{ok, Resp} -> 
			io:format("~p", [Resp]),
			Req:ok({"text/plain", string:join(Resp, ",")});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% Create RRDB
%% PUT /rrdb/${dbname}
%% PUT /rrdb/${newdb}
%% x-errdb-copy-source: ${srcdb} 
handle('PUT', {"rrdbs", Name}, Req) ->
	%TODO: valid ?
	case Req:get_header_value("x-errdb-copy-source") of
		{value, SrcName} ->
			case errdb_client:move(rrdb, {SrcName, Name}) of
				ok -> 
					Req:respond({201, [{"Location", Name}], <<"ok">>});
				{error, {Status, Reason}} ->
					Req:respond({Status, [], Reason})
			end;
		false ->
			case errdb_client:create(rrdb, Name) of
				{ok, Location} -> 
					Req:respond({201, [{"Location", Location}], <<"ok">>});
				{error, {Status, Reason}} ->
					Req:respond({Status, [], Reason})
			end
	end;

%% Delete RRDB
%% DELETE /rrdb/${dbname}
handle('DELETE', {"rrdbs", Name}, Req) ->
	case errdb_client:delete(rrdb, Name) of
		ok -> 
			Req:ok({"text/plain", <<"ok">>});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% LIST RRDFiles
%% GET /rrdb/${dbname}/rrdfiles
handle('GET', {"rrdbs", Dn, "rrdfiles"}, Req) ->
    DbName = to_rrdb(Dn),
	case errdb_client:list(rrdfiles, DbName) of
		{ok, Files} ->
			%ignore format
			Req:ok({"text/plain", list_to_binary(string:join(Files, ","))});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% Info RRDFile
%% HEAD /rrdbs/${dbname}/rrdfiles/${filename}
handle('GET', {"rrdbs", Dn, "rrdfiles", FileName}, Req) ->
    DbName = to_rrdb(Dn),
	case errdb_client:info(rrdfile, {DbName, FileName}) of
		{ok, Info} ->
			%ignore format
			Req:ok({"text/plain", list_to_binary(Info)});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

handle('GET', {"rrdbs", Dn, "rrdfiles", FileName, "first"}, Req) ->
    DbName = to_rrdb(Dn),
	case errdb_client:last(rrdfile, {DbName, FileName}) of
		{ok, First} ->
			Req:ok({"text/plain", list_to_binary(integer_to_list(First))});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

handle('GET', {"rrdbs", Dn, "rrdfiles", FileName, "last"}, Req) ->
    DbName = to_rrdb(Dn),
	case errdb_client:last(rrdfile, {DbName, FileName}) of
		{ok, Last} ->
			%ignore format
			Req:ok({"text/plain", list_to_binary(integer_to_list(Last))});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% Create RRDFile
%% POST /rrdbs/${dbname}/rrdfiles
handle('POST', {"rrdbs", Dn, "rrdfiles"}, Req) ->
	Params = Req:parse_post(),
    DbName = to_rrdb(Dn),
	case lists:keysearch("filename", 1, Params) of
		{value, {"filename", FileName}} ->
			case errdb_client:create(rrdfile, {DbName, FileName, Params}) of
				ok ->
					Req:respond({201, [{"Location", "rrdb/" ++ DbName ++ "/" ++ FileName}], []});
				{error, {Status, Reason}} ->
					Req:respond({Status, [], Reason})
			end;
		false ->
			Req:respond({400, [], []})
	end;

%% Tune RRDFile
%% PUT /rrdb/${dbname}/rrdfiles/${filename}
%% Move RRDFile
%% PUT /rrdb/${dbname}/rrdfiles/${filename}
%% x-errdb-copy-source: ${src_rrdfile} 
handle('PUT', {"rrdbs", Dn, "rrdfiles", FileName}, Req) ->
	Params = Req:parse_post(),
    DbName = to_rrdb(Dn),
	case Req:get_header_value("x-errdb-copy-source") of
		{value, SrcName} ->
			case errdb_client:move(rrdfile, {DbName, SrcName, FileName}) of
				ok ->
					Req:respond({201, [{"Location", "rrdb/" ++ DbName ++ "/rrdfiles/" ++ FileName}], <<"ok">>});
				{error, {Status, Reason}} ->
					Req:respond({Status, [], Reason})
			end;
		false ->
			case errdb_client:tune(rrdfile, {DbName, FileName, Params}) of
				ok -> 
					Req:ok("text/plain", []);
				{error, {Status, Reason}} ->
					Req:respond({Status, [], Reason})
			end
	end;

%% DELETE rrdfile
%% DELETE /rrdb/${dbname}/rrdfiles/${filename}
handle('DELETE', {"rrdbs", Dn, "rrdfiles", FileName}, Req) ->
    DbName = to_rrdb(Dn),
	case errdb_client:delete(rrdfile, {DbName, FileName}) of
		ok ->
			Req:ok({"text/plain", []});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% List Templates
%% GET /rrdfile_templates
handle('GET', {"rrdfile_templates"}, Req) ->
	case errdb_client:list(rrdfile_templates) of
		{ok, Templates} -> 
			io:format("~p", [Templates]),
			Req:ok({"text/plain", io_lib:write(Templates)});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% Create Template
%% POST /rrdfile_templates
handle('POST', {"rrdfile_templates"}, Req) ->
	Params = Req:parse_post(),
	case lists:keysearch("template_name", 1, Req) of
		{value, TemplateName} ->
			case errdb_client:create(rrdfile_template, {TemplateName, Params}) of
				ok ->
					Req:respond({201, [{"Location", "rrdfile_templates/" ++ TemplateName}], <<"ok">>});
				{error, {Status, Reason}} ->
					Req:respond({Status, [], Reason})
			end;
		false ->
			Req:respons({400, [], []})
	end;

%% Update Template
%% PUT /rrdfile_templates/${template_name}
handle('PUT', {"rrdfile_templates", TemplateName}, Req) ->
	Params = Req:parse_post(),
	case errdb_client:update(rrdfile_template, {TemplateName, Params}) of
		ok ->
			Req:ok({"text/plain", <<"ok">>});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

handle('DELETE', {"rrdfile_templates", TemplateName}, Req) ->
	case errdb_client:delete(rrdfile_template, TemplateName) of
		ok ->
			Req:ok({"text/plain", <<"ok">>});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% Fetch Datalogs
%% GET /rrdb/${dbname}/rrdfiles/${filename}/datalogs?${query}
handle('GET', {"rrdbs", DbName, "rrdfiles", FileName, "datalogs"}, Req) ->
	Params = Req:parse_qs(),
	case errdb_client:fetch(datalogs, {DbName, FileName, Params}) of
		{ok, Datalogs} ->
			Req:ok({"text/plain", string:join(Datalogs, "\r\n")});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% Create Datalog
%% POST /rrdb/${dbname}/rrdfiles/${filename}/datalogs
handle('POST', {"rrdbs", Dn, "rrdfiles", FileName, "datalogs"}, Req) ->
	Datalog = Req:parse_post(),
    DbName = to_rrdb(Dn),
	case errdb_client:update(datalogs, {DbName, FileName, Datalog}) of
		ok ->
			Req:ok({"text/plain", []});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

handle('GET', {"rrdbs", Dn, "rrdfiles", FileName, "datalogs", "last"}, Req) ->
    DbName = to_rrdb(Dn),
	case errdb_client:last(datalogs, {DbName, FileName}) of
		{ok, Datalog} ->
			Req:ok({"text/plain", list_to_binary(Datalog)});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;
  
%% Get Graph
%% GET /rrdgraphs/${name}.${format}?${query}
handle('GET', {"rrdbs", Dn, "rrdfiles", FileName, "rrdgraphs", ImgName0}, Req) ->
	Params = Req:parse_qs(),
	Params1 = lists:map(fun({Key, Val}) -> {list_to_atom(Key), Val} end, Params),
    DbName = to_rrdb(Dn),
    {ImgName, ImgFormat} = 
    case string:tokens(ImgName0, ".") of
    [Name] -> {Name, "PNG"};
    [Name, Format] -> {Name, string:to_upper(Format)}
    end,
    Params2 = [{imgformat, ImgFormat} | Params1],
	case ImgName of
    "-" -> 
        case errdb_client:graph({DbName, FileName, Params2}) of
        {ok, Binary} -> 
            Req:ok({img_mime(ImgFormat), Binary});
        {error, {Status, Reason}} ->
            Req:respond({Status, [], Reason})
        end;
    ImageName ->
        case errdb_client:graph(ImageName, {DbName, FileName, Params2}) of
        {ok, Resp} -> 
            Req:ok({"text/plain", Resp});
        {error, {Status, Reason}} ->
            Req:respond({Status, [], Reason})
        end
	end;

%% List Graph Templates
%% GET /rrdgraph_templates
handle('GET', {"rrdgraph_templates"}, Req) ->
	case errdb_client:list(rrdgraph_templates) of
		{ok, Templates} ->
			Req:ok({"text/plain", string:join(Templates, "\r\n")});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% Create Graph Template
%% POST /rrdgraph_templates/
handle('POST', {"rrdgraph_templates"}, Req) ->
	Params = Req:parse_post(),
	case lists:keysearch("template_name", 1, Params) of
		{value, TemplateName} ->
			case errdb_client:create(rrdgraph_template, {TemplateName, Params}) of
				ok -> 
					Req:respond({201, [{"Location", "rrdgraph_templates/" ++ TemplateName}], <<"ok">>});
				{error, {Status, Reason}} ->
					Req:respond({Status, [], Reason})
				
			end;
		false ->
			Req:respond({400, [], []})
	end;

%% Update Graph Template
%% PUT /rrdgraph_templates/${template_name}
handle('PUT', {"rrdgraph_templates", TemplateName}, Req) ->
	Params = Req:parse_post(),
	case errdb_client:update(rrdgraph_template, {TemplateName, Params}) of
		ok -> 
			Req:ok({"text/plain", []});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

%% Delete Graph Template
%% DELETE /rrdgraph_templates/${template_name}
handle('DELETE', {"rrdgraph_templates", TemplateName}, Req) ->
	case errdb_client:delete(rrdgraph_template, TemplateName) of
		ok -> 
			Req:ok({"text/plain", <<"ok">>});
		{error, {Status, Reason}} ->
			Req:respond({Status, [], Reason})
	end;

handle(_Other, _Path, Req) ->
	Req:respond({500, [], <<"unsupported request">>}). 
	
%% Internal API
get_option(Option, Options) ->
    {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.

to_rrdb(Dn) ->
  Idx = string:chr(Dn, $=),
  if
  Idx == 0 -> %%uuid of monit
    string:join(string:tokens(Dn, ","), "/");
  true -> %%dn of wifioss or eponoss
    string:join(lists:reverse(string:tokens(Dn, ",")), "/")
  end.

img_mime("SVG") ->
    "image/svg+xml";
img_mime("PNG") ->
    "image/png";
img_mime(_) ->
    "image/png".
