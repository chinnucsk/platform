-module(sesnmp).

-export([get_group/2, get_group/3, get_group/4, 
         get_table/2, get_table/3, get_table/4, 
         get_entry/3, get_entry/4, get_entry/5]).

-include_lib("snmp/include/snmp_types.hrl").

-define(PORT, 161).

-define(RETRIES, 2).

-define(TIMEOUT, 3000).

%%Scalars = [{Name, Oid}]
get_group(Addr, Scalars) ->
    get_group(Addr, Scalars, []).

get_group(Addr, Scalars, AgentData) ->
    get_group(Addr, ?PORT, Scalars, AgentData).

get_group(Addr, Port, Scalars, AgentData) ->
	{Names, Oids} = split_vars(Scalars),
	case retry(fun() -> sesnmp_client:get(Addr, Port, Oids, AgentData, ?TIMEOUT) end, ?RETRIES) of
	{ok, {noError, 0, Varbinds}, _} -> 
		{ok, merge_vars(Names, Varbinds)};
	Error -> 
		{error, Error}
	end.

get_table(Addr, Columns) ->
    get_table(Addr, Columns, []).

get_table(Addr, Columns, AgentData) ->
    get_table(Addr, ?PORT, Columns, AgentData).
    
get_table(Addr, Port, Columns, AgentData) ->
	[{_, Col1Oid} | _] = Columns,
	case get_table(Addr, Port, Col1Oid, Columns, AgentData, []) of
	{ok, Rows} ->
		{ok, lists:reverse(Rows)};
	{error, Error} ->
		{error, Error}
	end.

get_table(Addr, Port, Col1Oid, Columns, AgentData, Acc) ->
	{Names, Oids} = split_vars(Columns),
	case retry(fun() -> sesnmp_client:get_next(Addr, Port, Oids, AgentData, ?TIMEOUT) end, ?RETRIES) of
	{ok, {noError, 0, Varbinds}, _} -> 
		#varbind{oid=Oid} = lists:nth(1, Varbinds),
		case start_with_oid(Col1Oid, Oid) of
		true ->
			NewOids = lists:map(fun(Varbind) -> Varbind#varbind.oid end, Varbinds),
			NewColumns = to_name_oid_map(Names, NewOids),
			get_table(Addr, Port, Col1Oid, NewColumns, AgentData, [ [{tableIndex, Oid -- Col1Oid} | merge_vars(Names, Varbinds)] | Acc]);
		false ->
			{ok, Acc}
		end;
	Other -> 
		{error, Other}
	end.

get_entry(Addr, Columns, Indices) ->
    get_entry(Addr, Columns, Indices, []). 

get_entry(Addr, Columns, Indices, AgentData) ->
    get_entry(Addr, ?PORT, Columns, Indices, AgentData).

get_entry(Addr, Port, Columns, Indices, AgentData) ->
	{Names, Oids} = split_vars(Columns),
	Oids1 = [lists:append(Oid, Indices) || Oid <- Oids],
    case retry(fun() -> sesnmp_client:get(Addr, Port, Oids1, AgentData, ?TIMEOUT) end, ?RETRIES) of
	{ok, {noError, 0, Varbinds}, _} -> 
		{ok, merge_vars(Names, Varbinds)};
	Error -> 
		{error, Error}
	end.

%%-------------------------------------------------------
%% Retry
%%-------------------------------------------------------
retry(Fun, 0) ->
    Fun();

retry(Fun, R) when R < 0 ->
    Fun();

retry(Fun, R) ->
    case Fun() of
    {error, {timeout, _}} -> 
        retry(Fun, R - 1);
    Result -> 
        Result
    end.

%%Internale functions.
split_vars(Vars) ->
	split_vars(Vars, [], []).

split_vars([{Name, Oid}|T], Names, Oids) ->
	split_vars(T, [Name|Names], [Oid|Oids]);

split_vars([], Names, Oids) ->
	{lists:reverse(Names), lists:reverse(Oids)}.
	
merge_vars(Names, Varbinds) ->
	merge_vars(Names, Varbinds, []).

merge_vars([Name|T1], [Varbind|T2], Acc) ->
	merge_vars(T1, T2, [{Name, Varbind#varbind.value}|Acc]);

merge_vars([], [], Acc) ->
	lists:reverse(Acc).

to_name_oid_map(Names, Oids) ->
	to_name_oid_map(Names, Oids, []).

to_name_oid_map([Name|T1], [Oid|T2], Acc) ->
	to_name_oid_map(T1, T2, [{Name, Oid} | Acc]);

to_name_oid_map([], [], Acc) ->
	lists:reverse(Acc).

%%Oid2 is the oid with index.
start_with_oid(Oid1, Oid2) ->
	if 
	length(Oid2) > length(Oid1) -> 
		{Oid3, _} = lists:split(length(Oid1), Oid2),
		Oid1 == Oid3;
	true ->
		false
	end.
