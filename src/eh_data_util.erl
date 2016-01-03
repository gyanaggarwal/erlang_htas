%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Copyright (c) 2015 Gyanendra Aggarwal.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(eh_data_util).

-export([query_data/3,
         snapshot_data/3,
         make_data/5,
         make_transient_data/5,
         merge_data/3,
         add_key_value/2]).

-include("erlang_htas.hrl").

process_data(ProcessFun, ProcessCriteria, Q0, Acc0) ->
  case queue:out(Q0) of
    {empty, _}                 ->
      Acc0;
    {{value, StorageData}, Q1} ->
      process_data(ProcessFun, ProcessCriteria, Q1, ProcessFun(ProcessCriteria, StorageData, Acc0))
  end.

sort_fun(#eh_storage_data{timestamp=Timestamp1, data_index=DataIndex1},
         #eh_storage_data{timestamp=Timestamp2, data_index=DataIndex2}) ->
  (Timestamp1 < Timestamp2) orelse (Timestamp1 =:= Timestamp2 andalso DataIndex1 =< DataIndex2).

snapshot_fun({CTimestamp, CDataIndex, StorageKey}, 
             #eh_storage_value{timestamp=Timestamp, data_index=DataIndex}=StorageValue, Qo0) 
  when Timestamp > CTimestamp orelse (Timestamp =:= CTimestamp andalso DataIndex > CDataIndex) ->
  [storage_data(StorageKey, StorageValue) |  Qo0];
snapshot_fun(_, _, Qo0) ->
  Qo0.

snapshot_data(Timestamp, DataIndex, Mi0) ->
 Acc0 = maps:fold(fun(K, Qi0, Acc) -> process_data(fun snapshot_fun/3, {Timestamp, DataIndex, K}, Qi0, Acc) end, [], Mi0),
 queue:from_list(lists:sort(fun sort_fun/2, Acc0)).

query_fun(_, #eh_storage_value{status=?STATUS_INACTIVE, column=undefined}, _Lo0) ->
  [];
query_fun(_, #eh_storage_value{status=?STATUS_INACTIVE, column=Column}, Lo0) ->
  lists:keydelete(Column, 1, Lo0);
query_fun(_, #eh_storage_value{status=?STATUS_ACTIVE, column=Column, value=Value}, Lo0) ->
  [{Column, Value} | lists:keydelete(Column, 1, Lo0)].

query_data(ObjectType, ObjectId, Mi0) ->
  case maps:find(make_key(ObjectType, ObjectId), Mi0) of
    error     ->
      [];
    {ok, Qi0} ->
      process_data(fun query_fun/3, {ObjectType, ObjectId}, Qi0, [])
  end.

make_data(ObjectType, ObjectId, Timestamp, ?STATUS_INACTIVE, Di0) ->
  Qi0 = queue:new(),
  StorageData = storage_data(ObjectType, ObjectId, Timestamp, 1, {undefined, ?STATUS_INACTIVE, undefined}),
  add_data(StorageData, Qi0, Di0);
make_data(ObjectType, ObjectId, Timestamp, Columns, Di0) ->
  make_data_acc(ObjectType, ObjectId, Timestamp, Columns, {0, queue:new(), Di0}).

make_data_acc(ObjectType, ObjectId, Timestamp, [H | T], {DataIndex0, Qi0, Di0}) ->
  DataIndex1 = DataIndex0+1,
  StorageData = storage_data(ObjectType, ObjectId, Timestamp, DataIndex1, H),
  make_data_acc(ObjectType, ObjectId, Timestamp, T, add_data(StorageData, Qi0, Di0)); 
make_data_acc(_, _, _, [], Acc0) ->
  Acc0.

add_data(StorageData, Q0, D0) ->
  {StorageData#eh_storage_data.data_index, queue:in(StorageData, Q0), add_key_value(StorageData, D0)}.

make_transient_data(ObjectType, ObjectId, Timestamp, ?STATUS_INACTIVE, Qi0) ->
  StorageData = storage_data(ObjectType, ObjectId, Timestamp, 1, {undefined, ?STATUS_INACTIVE, undefined}),
  queue:in(StorageData, Qi0);
make_transient_data(ObjectType, ObjectId, Timestamp, Columns, Qi0) ->
  make_transient_data_acc(ObjectType, ObjectId, Timestamp, Columns, 0, Qi0).

make_transient_data_acc(ObjectType, ObjectId, Timestamp, [H | T], DataIndex0, Qi0) ->
  DataIndex1 = DataIndex0+1,
  StorageData = storage_data(ObjectType, ObjectId, Timestamp, DataIndex1, H),
  make_transient_data_acc(ObjectType, ObjectId, Timestamp, T, DataIndex1, queue:in(StorageData, Qi0));
make_transient_data_acc(_, _, _, [], _, Acc0) ->
  Acc0.

merge_data(Q0, TQ0, M0) ->
  {TS1, DI1, Q1, M1} = merge_data_acc(Q0, queue:new(), M0, 0, 0, true),
  merge_data_acc(TQ0, Q1, M1, TS1, DI1, true).

merge_data_acc(Qi0, Qo0, Mi0, TS0, DI0, CheckFlag) ->
  case queue:out(Qi0) of
    {empty, _}                  ->
      {TS0, DI0, Qo0, Mi0};
    {{value, StorageData}, Qi1} ->
      case (not CheckFlag) orelse (StorageData#eh_storage_data.timestamp > TS0) of
        true  ->
          {DI1, Qo1, Mi1} = add_data(StorageData, Qo0, Mi0),
          merge_data_acc(Qi1, Qo1, Mi1, StorageData#eh_storage_data.timestamp, DI1, false);
        false ->
          merge_data_acc(Qi1, Qo0, Mi0, TS0, DI0, CheckFlag)
      end
  end.
   
storage_data(#eh_storage_key{object_type=ObjectType, object_id=ObjectId},
             #eh_storage_value{timestamp=Timestamp, data_index=DataIndex, status=Status, column=Column, value=Value}) ->
  storage_data(ObjectType, ObjectId, Timestamp, DataIndex, {Column, Status, Value}).

storage_data(ObjectType, ObjectId, Timestamp, DataIndex, {Column, Status}) ->
  storage_data(ObjectType, ObjectId, Timestamp, DataIndex, {Column, Status, undefined});
storage_data(ObjectType, ObjectId, Timestamp, DataIndex, {Column, Status, Value}) ->
  #eh_storage_data{object_type=ObjectType,
	           object_id=ObjectId,
		   timestamp=Timestamp,
		   data_index=DataIndex,
		   status=Status,
                   column=Column,
                   value=Value}.

make_key(ObjectType, ObjectId) ->
  #eh_storage_key{object_type=ObjectType, object_id=ObjectId}.

make_value(Timestamp, DataIndex, Status, Column, Value) ->
  #eh_storage_value{timestamp=Timestamp, data_index=DataIndex, status=Status, column=Column, value=Value}.

make_key_value(#eh_storage_data{object_type=ObjectType, object_id=ObjectId, 
                                timestamp=Timestamp, data_index=DataIndex,
                                status=Status, column=Column, value=Value}) ->
  {make_key(ObjectType, ObjectId), make_value(Timestamp, DataIndex, Status, Column, Value)}.

add_key_value(StorageData, M0) ->
  {Key, Value} = make_key_value(StorageData),
  Q1 = case maps:find(Key, M0) of
        error		     ->
      	   queue:new();
      	    {ok, Q0} ->
      	       Q0
       end,
  maps:put(Key,	queue:in(Value,	Q1), M0).

