%%%----------------------------------------------------------------------
%%% File    : cassandra_cf.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Cassandra column family.
%%% Created : 02 Mar 2010
%%% License : http://www.monit.cn
%%%
%%% Copyright (C) 2007-2010, www.monit.cn
%%%----------------------------------------------------------------------
-module(cassandra_cf, [Client, ColumnFamily]).

-export([get/2, get/3, 
        get_all/1, get_first/1, get_last/1,
        get_slice/2, get_slice/3, 
        multiget/2, multiget/3,
        multiget_slice/2, multiget_slice/3,
        get_count/1, get_count/2,
        get_range_slice/2, get_range_slice/3,
        insert/2, insert/3, insert/4,
        remove/1, remove/2, remove/3]).

get(Key, Column) ->
    cassandra:get(Client, ColumnFamily, Key, Column).

get(Key, SuperColumn, Column) ->
    cassandra:get(Client, ColumnFamily, Key, SuperColumn, Column).

get_all(Key) ->
    cassandra:get_all(Client, ColumnFamily, Key).

get_first(Key) ->
    cassandra:get_first(Client, ColumnFamily, Key).

get_last(Key) ->
    cassandra:get_last(Client, ColumnFamily, Key).

get_slice(Key, Predicate) ->
    cassandra:get_slice(Client, ColumnFamily, Key, Predicate).

get_slice(Key, SuperColumn, Predicate) ->
    cassandra:get_slice(Client, ColumnFamily, Key, SuperColumn, Predicate).

multiget(Keys, Column) ->
    cassandra:multiget(Client, ColumnFamily, Keys, Column).

multiget(Keys, SuperColumn, Column) ->
    cassandra:multiget(Client, ColumnFamily, Keys, SuperColumn, Column).

multiget_slice(Keys, Predicate) ->
    cassandra:multiget_slice(Client, ColumnFamily, Keys, Predicate).

multiget_slice(Keys, SuperColumn, Predicate) ->
    cassandra:multiget_slice(Client, ColumnFamily, Keys, SuperColumn, Predicate).

get_count(Key) ->
    cassandra:get_count(Client, ColumnFamily, Key).

get_count(Key, SuperColumn) ->
    cassandra:get_count(Client, ColumnFamily, Key, SuperColumn).

get_range_slice(Predicate, RangePredicate) ->
    cassandra:get_range_slice(Client, ColumnFamily, Predicate, RangePredicate).

get_range_slice(SuperColumn, Predicate, RangePredicate) ->
    cassandra:get_range_slice(Client, ColumnFamily, SuperColumn, Predicate, RangePredicate).

insert(Key, [H|_] = Columns) when is_tuple(H) ->
    cassandra:insert(Client, ColumnFamily, Key, Columns).

insert(Key, SuperColumn, [H|_] = Columns) when is_tuple(H) ->
    cassandra:insert(Client, ColumnFamily, Key, SuperColumn, Columns);

insert(Key, Column, Value) ->
    cassandra:insert(Client, ColumnFamily, Key, Column, Value).

insert(Key, SuperColumn, Column, Value) ->
    cassandra:insert(Client, ColumnFamily, Key, SuperColumn, Column, Value).

remove(Key) ->
    cassandra:remove(Client, ColumnFamily, Key).

remove(Key, Column) ->
    cassandra:remove(Client, ColumnFamily, Key, Column).
    
remove(Key, SuperColumn, Column) ->
    cassandra:remove(Client, ColumnFamily, Key, SuperColumn, Column).

