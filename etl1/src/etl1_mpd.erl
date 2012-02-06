-module(etl1_mpd).

-author("hejin-03-24").

-export([process_msg/1, generate_msg/2]).

-export([get_status_value/2]).

-include("elog.hrl").

-define(empty_msg_size, 24).

-define(max_message_size, 20480).

-include("tl1.hrl").

-import(extbif, [to_list/1, to_integer/1]).

-record(tl1_response, {en, endesc, fields, records}).

%%-----------------------------------------------------------------
%% Func: process_msg(Packet, TDomain, TAddress, State) ->
%%       {ok, SnmpVsn, Pdu, PduMS, ACMData} | {discarded, Reason}
%% Types: Packet = binary()
%%        TDomain = snmpUDPDomain | atom()
%%        TAddress = {Ip, Udp}
%%        State = #state
%% Purpose: This is the main Message Dispatching function. (see
%%          section 4.2.1 in rfc2272)
%%-----------------------------------------------------------------
%   ZTE_192.168.41.10 2011-07-19 16:45:35
%   M  CTAG DENY
process_msg(MsgData) ->
    ?INFO("get respond :~p", [MsgData]),
    Lines = string:tokens(to_list(MsgData), "\r\n"),
    ?INFO("get respond splite :~p", [Lines]),
    _Header = lists:nth(1, Lines),
    RespondId = lists:nth(2, Lines),
    {ReqId, CompletionCode} = get_response_id(RespondId),
    RespondBlock = lists:nthtail(2, Lines),
    {{En, Endesc}, Rest} = get_response_status(RespondBlock),
    ?INFO("get en endesc :~p data:~p",[{En,Endesc}, Rest]),
    RespondData = case get_response(CompletionCode, Rest) of
        no_data ->
            {ok, [{en, En}, {endesc, Endesc}]};
        {data, #tl1_response{fields = Fileds, records = Records}} ->
            {ok, to_tuple_records(Fileds, Records)};
        {error, Reason} ->
            {error, Reason}
     end,
    Terminator = lists:last(Lines),
    ?INFO("req_id: ~p,comp_code: ~p, terminator: ~p, data:~p",[ReqId, CompletionCode, Terminator, RespondData]),
    Pct = #pct{type = 'output',
               request_id = ReqId,
               complete_code = CompletionCode,
               data =  RespondData
               },
    {ok, Pct}.

get_response_id(RespondId) ->
    Data = string:tokens(RespondId, " "),
    ReqId = lists:nth(2, Data),
    CompletionCode = lists:last(Data),
    {to_integer(ReqId), CompletionCode}.

get_response_status([Status|Data]) ->
    ?INFO("get status :~p", [Status]),
    {En, Rest} = get_status("EN=", Status),
    ?INFO("get en :~p, ~p", [En, Rest]),
    case En of
        false ->
            {{false, false}, [Status|Data]};
        _ ->
            {Endesc, Rest1} = get_status("ENDESC=", Rest),
            ?INFO("get endesc :~p, ~p", [En, Rest1]),
            {{En, Endesc}, Data}
     end.       

%DELAY, DENY, PRTL, RTRV
get_response("COMPLD", Data)->
    %\r\n\r\n -> error =[]
     get_response_data(Data);
get_response("PRTL", Data)->
     get_response_data(Data);
get_response(_CompCode, Data)->
     {error, string:join(Data, ",")}.


get_response_data(Block) ->
    ?INFO("get Block:~p", [Block]),
    {TotalPackageNo, Block0} = get_response_data("total_blocks=", Block),
    {CurrPackageNo, Block1} = get_response_data("block_number=", Block0),
    {PackageRecordsNo, Block2} = get_response_data("block_records=", Block1),
    {Title, Block3} = get_response_data("list |List ", Block2),
    {_SpliteLine, Block4} = get_response_data("---", Block3),
    ?INFO("get response:~p", [{TotalPackageNo, CurrPackageNo, PackageRecordsNo, Title}]),
    case get_response_data(fields, Block4) of
			{fields, []} ->
                no_data;
			{Fields, Block5} ->
			    {ok, Rows} =  get_rows(Block5),
				{data, #tl1_response{endesc= "COMPLD", fields = Fields, records = Rows}}
		    end.


get_status(Name, String) ->
    case string:str(String, Name) of
        0 ->
            {false, ""};
        N ->
            Rest = string:substr(String, N + length(Name)),
            ?INFO("rest :~p", [Rest]),
            get_status_value(Rest, [])
        end.

get_status_value([], Acc) ->
    {lists:reverse(Acc), ""};
get_status_value([$\s|String], Acc) ->
    {lists:reverse(Acc), String};
get_status_value([A|String], Acc) ->
    get_status_value(String, [A|Acc]).


get_rows(Datas) ->
    get_rows(Datas, []).

get_rows([], Values) ->
    {ok, lists:reverse(Values)};
get_rows([";"], Values) ->
    {ok, lists:reverse(Values)};
get_rows([">"], Values) ->
    {ok, lists:reverse(Values)};
get_rows(["---" ++ _|_], Values) ->
    {ok, lists:reverse(Values)};
get_rows([Line|Response], Values) ->
    Row = string:tokens(Line, "\t"),
    get_rows(Response, [Row | Values]).


get_response_data(Name, []) ->
    {Name, []};
get_response_data(fields, [Line|Response]) ->
     Fields = string:tokens(Line, "\t"),
    {Fields, Response};
get_response_data(Name, [Line|Response]) ->
    case re:run(Line, Name) of
        nomatch ->
            get_response_data(Name, Response);
        {match, _N} ->
            Value = string:strip(Line) -- Name,
            {Value, Response}
        end.


to_tuple_records(_Fields, []) ->
	[];

to_tuple_records(Fields, Records) ->
	[to_tuple_record(Fields, Record) || Record <- Records].
	
to_tuple_record(Fields, Record) when length(Fields) =< length(Record) ->
	to_tuple_record(Fields, Record, []).

to_tuple_record([], [], Acc) ->
	Acc;

to_tuple_record([_F|FT], [undefined|VT], Acc) ->
	to_tuple_record(FT, VT, Acc);

to_tuple_record([F|FT], [V|VT], Acc) ->
	to_tuple_record(FT, VT, [{list_to_atom(F), V} | Acc]).



%%-----------------------------------------------------------------
%% Generate a message
%%-----------------------------------------------------------------
generate_msg(Pct, MsgData) ->
    if length(MsgData) =< ?max_message_size ->
            Cmd = to_list(MsgData),
            case re:replace(Cmd, "CTAG", to_list(Pct#pct.request_id), [global,{return, list}]) of
                Cmd -> {discarded, no_ctag};
                NewString -> {ok, NewString}
            end;
       true ->
            ?INFO("msg size :~p", [size(MsgData)]),
           {discarded, too_big}
    end.
