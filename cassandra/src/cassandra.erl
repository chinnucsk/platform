%%%----------------------------------------------------------------------
%%% File    : cassandra.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Cassandra client api.
%%% Created : 01 Mar 2010
%%% License : http://www.monit.cn
%%%
%%% Copyright (C) 2007-2010, www.monit.cn
%%%----------------------------------------------------------------------
-module(cassandra).

-author('ery.lee@gmail.com').

-include("elog.hrl").

-include("cassandra_types.hrl").

-behavior(gen_server).

-export([start_link/1, start_link/2]).

-export([describe/1,
        get/4, get/5,
        get_all/3, get_first/3, get_last/3,
        get_slice/4, get_slice/5,
        multiget/4, multiget/5, 
        multiget_slice/4, multiget_slice/5,
        get_count/3, get_count/4,
        get_range_slice/4, get_range_slice/5,
        insert/4, insert/5, insert/6,
        remove/3, remove/4, remove/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {host, port, keyspace, client, trans_opts}).

%%--------------------------------------------------------------------
%% Function: start_link(KeySpace, Opts) -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(KeySpace) ->
    start_link(KeySpace, []).

start_link(KeySpace, Opts) ->
    gen_server:start_link(?MODULE, [KeySpace, Opts], []).

describe(Client) ->
    call(Client, describe).

%% cassandra api: ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level) 
%%  Get the Column or SuperColumn at the given column_path. If no value is present, NotFoundException is thrown. (This is the only method that can throw an exception under non-failure conditions.) 
get(Client, {super, ColumnFamily}, Key, Column) ->
    call(Client, {get, {super, ColumnFamily}, Key, Column});

get(Client, ColumnFamily, Key, Column) when is_list(Column) ->
    call(Client, {get, ColumnFamily, Key, Column}).

get(Client, {super, ColumnFamily}, Key, SuperColumn, Column) ->
    call(Client, {get, ColumnFamily, Key, SuperColumn, Column}).

get_all(Client, {super, ColumnFamily}, Key) ->
    get_all(Client, ColumnFamily, Key); 
get_all(Client, ColumnFamily, Key) ->
    get_slice(Client, ColumnFamily, Key, []).

get_first(Client, {super, ColumnFamily}, Key) ->
    get_first(Client, ColumnFamily, Key); 
get_first(Client, ColumnFamily, Key) ->
    get_slice(Client, ColumnFamily, Key, [{count, 1}]).

get_last(Client, {super, ColumnFamily}, Key) ->
    get_last(Client, ColumnFamily, Key);
get_last(Client, ColumnFamily, Key) ->
    get_slice(Client, ColumnFamily, Key, [{reversed, true}, {count, 1}]).

%% doc list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
%%  Get the group of columns contained by column_parent  (either a ColumnFamily name or a ColumnFamily/SuperColumn name pair) specified by the given SlicePredicate struct.  
get_slice(Client, {super, ColumnFamily}, Key, Predicate) ->
    get_slice(Client, ColumnFamily, Key, Predicate); 
get_slice(Client, ColumnFamily, Key, Predicate) ->
    call(Client, {get_slice, ColumnFamily, Key, Predicate}).

get_slice(Client, {super, ColumnFamily}, Key, SuperColumn, Predicate) ->
    call(Client, {get_slice, ColumnFamily, Key, SuperColumn, Predicate}).

%% doc map<string,ColumnOrSuperColumn> multiget(keyspace, keys, column_path, consistency_level)
%%  Perform a get for column_path  in parallel on the given list<string> keys. The return value maps keys to the ColumnOrSuperColumn  found. If no value corresponding to a key is present, the key will still be in the map, but both the column and super_column references of the ColumnOrSuperColumn object it maps to will be null. 
multiget(Client, {super, ColumnFamily}, Keys, Column) ->
    call(Client, {multiget, {super, ColumnFamily}, Keys, Column});
multiget(Client, ColumnFamily, Keys, Column) ->
    call(Client, {multiget, ColumnFamily, Keys, Column}).

multiget(Client, {super, ColumnFamily}, Keys, SuperColumn, Column) ->
    call(Client, {multiget, ColumnFamily, Keys, SuperColumn, Column}).

%% doc map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
%%  Performs a get_slice for column_parent  and predicate for the given keys in parallel. 
multiget_slice(Client, {super, ColumnFamily}, Keys, Predicate) ->
    multiget_slice(Client, ColumnFamily, Keys, Predicate); 
multiget_slice(Client, ColumnFamily, Keys, Predicate) ->
    call(Client, {multiget_slice, ColumnFamily, Keys, Predicate}).

multiget_slice(Client, {super, ColumnFamily}, Keys, SuperColumn, Predicate) ->
    call(Client, {multiget_slice, ColumnFamily, Keys, SuperColumn, Predicate}).

%% doc i32 get_count(keyspace, key, column_parent, consistency_level)
%% Counts the columns present in column_parent.  
%% The method is not O(1). It takes all the columns from disk to calculate the answer. The only benefit of the method is that you do not need to pull all the columns over Thrift interface to count them. 
get_count(Client, {super, ColumnFamily}, Key) ->
    get_count(Client, ColumnFamily, Key); 
get_count(Client, ColumnFamily, Key) ->
    call(Client, {get_count, ColumnFamily, Key}).

get_count(Client, {super, ColumnFamily}, Key, SuperColumn) ->
    call(Client, {get_count, ColumnFamily, Key, SuperColumn}).

%% doc list<KeySlice> get_range_slice(keyspace, column_parent, predicate, start_key, finish_key, row_count=100, consistency_level)
%%  Replaces get_key_range. Returns a list of slices, sorted by row key, starting with start, ending with finish (both inclusive) and at most count long. The empty string ("") can be used as a sentinel value to get the first/last existing key (or first/last column in the column predicate parameter). Unlike get_key_range, this applies the given predicate to all keys in the range, not just those with undeleted matching data. This method is only allowed when using an order-preserving partitioner. 
get_range_slice(Client, {super, ColumnFamily}, Predicate, RangePredicate) ->
    get_range_slice(Client, ColumnFamily, Predicate, RangePredicate);
get_range_slice(Client, ColumnFamily, Predicate, RangePredicate) ->
    call(Client, {get_range_slice, ColumnFamily, Predicate, RangePredicate}).
    
get_range_slice(Client, {super, ColumnFamily}, SuperColumn, Predicate, RangePredicate) ->
    call(Client, {get_range_slice, ColumnFamily, SuperColumn, Predicate, RangePredicate}).

%% doc batch_insert(keyspace, key, batch_mutation, consistency_level)
%%  Insert Columns or SuperColumns across different Column Families for the same row key. batch_mutation is a map<string, list<ColumnOrSuperColumn>>  -- a map which pairs column family names with the relevant ColumnOrSuperColumn objects to insert. 
insert(Client, ColumnFamily, Key, [H|_] = Columns) when is_list(ColumnFamily) and is_tuple(H) ->
    call(Client, {batch_insert, ColumnFamily, Key, Columns}).

%% doc batch_insert(keyspace, key, batch_mutation, consistency_level)
%%  Insert Columns or SuperColumns across different Column Families for the same row key. batch_mutation is a map<string, list<ColumnOrSuperColumn>>  -- a map which pairs column family names with the relevant ColumnOrSuperColumn objects to insert. 
insert(Client, {super, ColumnFamily}, Key, SuperColumn, [H|_] = Columns) when is_tuple(H) ->
    call(Client, {batch_insert, ColumnFamily, Key, SuperColumn, Columns});
    
%% doc insert(keyspace, key, column_path, value, timestamp, consistency_level)
%% Insert a Column consisting of (column_path.column, value, timestamp) at the given column_path.column_family  and optional column_path.super_column. Note that column_path.column is here required, since a SuperColumn cannot directly contain binary values -- it can only contain sub-Columns. 
insert(_Client, {super, _ColumnFamily}, _Key, _Column, Value) when is_list(Value) ->
    {error, "cannot insert into super column family"};

insert(Client, ColumnFamily, Key, Column, Value) when is_list(ColumnFamily) ->
    call(Client, {insert, ColumnFamily, Key, Column, Value}).

insert(Client, {super, ColumnFamily}, Key, SuperColumn, Column, Value) ->
    call(Client, {insert, ColumnFamily, Key, SuperColumn, Column, Value}).

%% doc remove(keyspace, key, column_path, timestamp, consistency_level)
%% Remove data from the row specified by key at the granularity specified by column_path, and the given timestamp. Note that all the values in column_path besides column_path.column_family  are truly optional: you can remove the entire row by just specifying the ColumnFamily, or you can remove a SuperColumn or a single Column by specifying those levels too. Note that the timestamp  is needed, so that if the commands are replayed in a different order on different nodes, the same result is produced. 
remove(Client, {super, ColumnFamily}, Key) ->  
    remove(Client, ColumnFamily, Key);

remove(Client, ColumnFamily, Key) ->  
    call(Client, {remove, ColumnFamily, Key}).

remove(Client, {super, ColumnFamily}, Key, SuperColumn) ->  
    call(Client, {remove, {super, ColumnFamily}, Key, SuperColumn});

remove(Client, ColumnFamily, Key, Column) when is_list(ColumnFamily) ->  
    call(Client, {remove, ColumnFamily, Key, Column}).

remove(Client, {super, ColumnFamily}, Key, SuperColumn, Column) ->
    call(Client, {remove, ColumnFamily, Key, SuperColumn, Column}).

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
init([KeySpace, Opts]) ->
    {value, Host} = dataset:get_value(host, Opts, "localhost"),
    {value, Port} = dataset:get_value(port, Opts, 9160),
    {value, TransOpts} = dataset:get_value(transport, Opts, [{connect_timeout, 5000}]),
    {ok, Client} = thrift_client:start_link(Host, Port, cassandra_thrift, TransOpts),
    {ok, #state{host = Host, port = Port, keyspace = KeySpace, client = Client, trans_opts = TransOpts}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(describe, _From, #state{keyspace = KeySpace, client = Client} = State) ->
    Reply = thrift_call(Client, describe_keyspace, [KeySpace]), 
    {reply, Reply, State};

handle_call({get, {super, ColumnFamily}, Key, Column}, _From, #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, super_column = Column},
    Reply = 
    case thrift_call(Client, get, [KeySpace, Key, ColumnPath, ?cassandra_ONE]) of
    {ok, #columnOrSuperColumn{super_column = #superColumn{name = Col, columns = Columns}}} ->
        Result = {Col, [{SubCol, Val, Timestamp} || #column{name = SubCol, value = Val, timestamp = Timestamp} <- Columns]},
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({get, ColumnFamily, Key, Column}, _From, #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, column = Column},
    Reply = 
    case thrift_call(Client, get, [KeySpace, Key, ColumnPath, ?cassandra_ONE]) of
    {ok, #columnOrSuperColumn{column = #column{name = Name, value = Value, timestamp = Timestamp}}} ->
        {ok, {Name, Value, Timestamp}};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({get, ColumnFamily, Key, SuperColumn, Column}, _From, #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, super_column = SuperColumn, column = Column},
    Reply = 
    case thrift_call(Client, get, [KeySpace, Key, ColumnPath, ?cassandra_ONE]) of
    {ok, #columnOrSuperColumn{column = #column{name = Name, value = Value, timestamp = Timestamp}}} ->
        {ok, {Name, Value, Timestamp}};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({get_slice, ColumnFamily, Key, Predicate}, _From, #state{keyspace = KeySpace, client = Client} = State) ->
    SlicePredicate = slice_predicate(Predicate),
    ColumnParent = #columnParent{column_family = ColumnFamily},
    Reply = 
    case thrift_call(Client, get_slice, [KeySpace, Key, ColumnParent, SlicePredicate, ?cassandra_ONE]) of
    {ok, DataList} ->
        Result = 
        lists:map(
            fun(#columnOrSuperColumn{column = Column, super_column = undefined}) ->
                #column{name = Name, value = Val, timestamp = Timestamp} = Column,
                {Name, Val, Timestamp};
               (#columnOrSuperColumn{column = undefined, super_column = SuperColumn}) ->
                #superColumn{name = Name, columns = SubColumns} = SuperColumn,
                {Name, [{N, V, T} || #column{name = N, value = V, timestamp = T} <- SubColumns]}
            end, DataList),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({get_slice, ColumnFamily, Key, SuperColumn, Predicate}, _From, #state{keyspace = KeySpace, client = Client} = State) ->
    SlicePredicate = slice_predicate(Predicate),
    ColumnParent = #columnParent{column_family = ColumnFamily, super_column = SuperColumn},
    Reply = 
    case thrift_call(Client, get_slice, [KeySpace, Key, ColumnParent, SlicePredicate, ?cassandra_ONE]) of
    {ok, DataList} ->
        Result = 
        lists:map(fun(#columnOrSuperColumn{column = Column}) ->
                #column{name = Name, value = Val, timestamp = Timestamp} = Column,
                {Name, Val, Timestamp}
        end, DataList),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({multiget, {super, ColumnFamily}, Keys, Column}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, super_column = Column},
    Reply = 
    case thrift_call(Client, multiget, [KeySpace, Keys, ColumnPath, ?cassandra_ONE]) of
    {ok, Dict} -> %map<string,ColumnOrSuperColumn>
        Result = 
        dict:map(fun(_Key, #columnOrSuperColumn{super_column = #superColumn{name = Name, columns = SubColumns}}) -> 
            {Name, [{N, V, T} || #column{name = N, value = V, timestamp = T} <- SubColumns]}
        end, Dict),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({multiget, ColumnFamily, Keys, Column}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, column = Column},
    Reply = 
    case thrift_call(Client, multiget, [KeySpace, Keys, ColumnPath, ?cassandra_ONE]) of
    {ok, Dict} ->
        Result = 
        dict:map(fun(_Key, #columnOrSuperColumn{column = #column{name = N, value = V, timestamp = T}}) -> 
            {N, V, T} 
        end, Dict),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({multiget, ColumnFamily, Keys, SuperColumn, Column}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, super_column = SuperColumn, column = Column},
    Reply = 
    case thrift_call(Client, multiget, [KeySpace, Keys, ColumnPath, ?cassandra_ONE]) of
    {ok, Dict} ->
        Result = 
        dict:map(fun(_Key, #columnOrSuperColumn{column = #column{name = N, value = V, timestamp = T}}) -> 
            {N, V, T} 
        end, Dict),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({multiget_slice, ColumnFamily, Keys, Predicate}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnParent = #columnParent{column_family = ColumnFamily},
    SlicePredicate = slice_predicate(Predicate),
    Reply = 
    case thrift_call(Client, multiget_slice, [KeySpace, Keys, ColumnParent, SlicePredicate, ?cassandra_ONE]) of
    {ok, Dict} ->
        Result = 
        dict:map(fun(_Key, ColumnList) ->  
            lists:map(
                fun(#columnOrSuperColumn{column = Column, super_column = undefined}) ->
                        #column{name = Name, value = Val, timestamp = Timestamp} = Column,
                        {Name, Val, Timestamp};
                   (#columnOrSuperColumn{column = undefined, super_column = SuperColumn}) ->
                        #superColumn{name = Name, columns = SubColumns} = SuperColumn,
                        {Name, [{N, V, T} || #column{name = N, value = V, timestamp = T} <- SubColumns]}
            end, ColumnList)
        end, Dict),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({multiget_slice, ColumnFamily, Keys, SuperColumn, Predicate}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnParent = #columnParent{column_family = ColumnFamily, super_column = SuperColumn},
    SlicePredicate = slice_predicate(Predicate),
    Reply = 
    case thrift_call(Client, multiget_slice, [KeySpace, Keys, ColumnParent, SlicePredicate, ?cassandra_ONE]) of
    {ok, Dict} ->
        Result = 
        dict:map(fun(_Key, ColumnList) ->  
            lists:map(
                fun(#columnOrSuperColumn{column = Column}) ->
                    #column{name = Name, value = Val, timestamp = Timestamp} = Column,
                    {Name, Val, Timestamp}
            end, ColumnList)
        end, Dict),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({get_count, ColumnFamily, Key}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnParent = #columnParent{column_family = ColumnFamily},
    Reply = 
    case thrift_call(Client, get_count, [KeySpace, Key, ColumnParent, ?cassandra_ONE]) of
    {ok, Count} ->
        {ok, Count};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({get_count, ColumnFamily, Key, SuperColumn}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnParent = #columnParent{column_family = ColumnFamily, super_column = SuperColumn},
    Reply = 
    case thrift_call(Client, get_count, [KeySpace, Key, ColumnParent, ?cassandra_ONE]) of
    {ok, Count} ->
        {ok, Count};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({get_range_slice, ColumnFamily, Predicate, RangePredicate}, _From,
    #state{keyspace = KeySpace, client = Client} = State) ->
    SlicePredicate = slice_predicate(Predicate),
    ColumnParent = #columnParent{column_family = ColumnFamily},
    StartKey = proplists:get_value(start_key, RangePredicate, ""),
    FinishKey = proplists:get_value(finish_key, RangePredicate, ""),
    RowCount = proplists:get_value(row_count, RangePredicate, 100),
    Reply = 
    case thrift_call(Client, get_range_slice, [KeySpace, ColumnParent, SlicePredicate, StartKey, FinishKey, RowCount, ?cassandra_ONE]) of
    {ok, KeySliceList} ->
        Dict = dict:new(),
        Result = 
        lists:foldl(fun(#keySlice{key = Key, columns = ColumnList}, D) ->
            DataList =
            lists:map(
                fun(#columnOrSuperColumn{column = Column, super_column = undefined}) ->
                        #column{name = Name, value = Val, timestamp = Timestamp} = Column,
                        {Name, Val, Timestamp};
                   (#columnOrSuperColumn{column = undefined, super_column = SuperColumn}) ->
                        #superColumn{name = Name, columns = SubColumns} = SuperColumn,
                        {Name, [{N, V, T} || #column{name = N, value = V, timestamp = T} <- SubColumns]}
                end, ColumnList),
            dict:store(Key, DataList, D)
        end, Dict, KeySliceList),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};
    
handle_call({get_range_slice, ColumnFamily, SuperColumn, Predicate, RangePredicate}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    SlicePredicate = slice_predicate(Predicate),
    ColumnParent = #columnParent{column_family = ColumnFamily, super_column = SuperColumn},
    StartKey = proplists:get_value(start_key, RangePredicate, ""),
    FinishKey = proplists:get_value(finish_key, RangePredicate, ""),
    RowCount = proplists:get_value(row_count, RangePredicate, 100),
    Reply = 
    case thrift_call(Client, get_range_slice, [KeySpace, ColumnParent, SlicePredicate, StartKey, FinishKey, RowCount, ?cassandra_ONE]) of
    {ok, KeySliceList} ->
        Dict = dict:new(),
        Result = 
        lists:foldl(fun(#keySlice{key = Key, columns = ColumnList}, D) ->
            DataList = [{N, V, T} || #column{name = N, value = V, timestamp = T} <- ColumnList],
            dict:store(Key, DataList, D)
        end, Dict, KeySliceList),
        {ok, Result};
    {error, Error} ->
        {error, Error}
    end,
    {reply, Reply, State};

handle_call({batch_insert, ColumnFamily, Key, Columns}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    Timestamp = timestamp(),
    ColumnList = [#columnOrSuperColumn{column = #column{name = Name, value = Value, timestamp = Timestamp}} || {Name, Value} <- Columns],
    Dict = dict:new(),
    BatchMutation = dict:store(ColumnFamily, ColumnList, Dict), 
    Reply = thrift_call(Client, batch_insert, [KeySpace, Key, BatchMutation, ?cassandra_ONE]),
    {reply, Reply, State};

handle_call({batch_insert, ColumnFamily, Key, SuperColumn, Columns}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    Timestamp = timestamp(),
    ColumnList = [#column{name = Name, value = Value, timestamp = Timestamp} || {Name, Value} <- Columns],
    SuperColumnRecord = #superColumn{name = SuperColumn, columns = ColumnList},
    SuperColumnList = [#columnOrSuperColumn{super_column = SuperColumnRecord}],
    Dict = dict:new(),
    BatchMutation = dict:store(ColumnFamily, SuperColumnList, Dict), 
    Reply = thrift_call(Client, batch_insert, [KeySpace, Key, BatchMutation, ?cassandra_ONE]),
    {reply, Reply, State};

handle_call({insert, ColumnFamily, Key, Column, Value}, _From, #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, column = Column},
    Reply = thrift_call(Client, insert, [KeySpace, Key, ColumnPath, Value, timestamp(), ?cassandra_ONE]),
    {reply, Reply, State};
    
handle_call({insert, ColumnFamily, Key, SuperColumn, Column, Value}, _From, #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, super_column = SuperColumn, column = Column},
    Reply = thrift_call(Client, insert, [KeySpace, Key, ColumnPath, Value, timestamp(), ?cassandra_ONE]),
    {reply, Reply, State};

handle_call({remove, ColumnFamily, Key}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily},
    Reply = thrift_call(Client, remove, [KeySpace, Key, ColumnPath, timestamp(), ?cassandra_ONE]),
    {reply, Reply, State};

handle_call({remove, {super, ColumnFamily}, Key, SuperColumn}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, super_column = SuperColumn},
    Reply = thrift_call(Client, remove, [KeySpace, Key, ColumnPath, timestamp(), ?cassandra_ONE]),
    {reply, Reply, State};
  
handle_call({remove, ColumnFamily, Key, Column}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, column = Column},
    Reply = thrift_call(Client, remove, [KeySpace, Key, ColumnPath, timestamp(), ?cassandra_ONE]),
    {reply, Reply, State};

handle_call({remove, ColumnFamily, Key, SuperColumn, Column}, _From, 
    #state{keyspace = KeySpace, client = Client} = State) ->
    ColumnPath = #columnPath{column_family = ColumnFamily, super_column = SuperColumn, column = Column},
    Reply = thrift_call(Client, remove, [KeySpace, Key, ColumnPath, timestamp(), ?cassandra_ONE]),
    {reply, Reply, State};

handle_call(Request, _From, State) ->
    ?ERROR("unexpected request: ~p", [Request]),
    {reply, {error, unexpected_request}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    ?ERROR("unexpected msg: ~p", [Msg]),
    {noreply, State}.
%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    ?ERROR("unexpected info: ~p", [Info]),
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
call(Client, Req) ->
    gen_server:call(Client, Req).

slice_predicate(Predicate) ->
    ColumnNames = proplists:get_value(columns, Predicate),
    if
    ColumnNames == undefined ->
        Start = proplists:get_value(start, Predicate, ""),
        Finish = proplists:get_value(finish, Predicate, ""),
        Reversed = proplists:get_value(reversed, Predicate, false),
        Count = proplists:get_value(count, Predicate, 1000),
        SliceRange = #sliceRange{start = Start, finish = Finish, reversed = Reversed, count = Count},
        #slicePredicate{slice_range = SliceRange};
    true ->
        #slicePredicate{column_names = ColumnNames}
    end.

thrift_call(Client, Fun, Args) ->
    try thrift_client:call(Client, Fun, Args) 
    catch 
    _:Exception -> 
        {error, Exception}
    end.

timestamp() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    (MegaSecs * 1000000 + Secs) * 1000 + (MicroSecs div 1000).

