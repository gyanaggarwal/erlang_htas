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
         make_data/5]).

-include("erlang_htas.hrl").

process_data(ProcessFun, ProcessCriteria, Q0, Acc0) ->
  case queue:out(Q0) of
    {empty, _}                 ->
      Acc0;
    {{value, StorageData}, Q1} ->
      process_data(ProcessFun, ProcessCriteria, Q1, ProcessFun(ProcessCriteria, StorageData, Acc0))
  end.

snapshot_fun({CTimestamp, CDataIndex}, #eh_storage_data{timestamp=Timestamp, data_index=DataIndex}=StorageData, Qo0) 
  when Timestamp > CTimestamp orelse (Timestamp =:= CTimestamp andalso DataIndex > CDataIndex) ->
  queue:in(StorageData, Qo0);
snapshot_fun(_, _, Qo0) ->
  Qo0.

snapshot_data(Timestamp, DataIndex, Qi0) ->
  process_data(fun snapshot_fun/3, {Timestamp, DataIndex}, Qi0, queue:new()).

query_fun({ObjectType, ObjectId}, #eh_storage_data{object_type=ObjectType, object_id=ObjectId, status=?STATUS_INACTIVE, column=undefined}, _Lo0) ->
  [];
query_fun({ObjectType,	ObjectId}, #eh_storage_data{object_type=ObjectType, object_id=ObjectId, status=?STATUS_INACTIVE, column=Column}, Lo0) ->
  lists:keydelete(Column, 1, Lo0);
query_fun({ObjectType,	ObjectId}, #eh_storage_data{object_type=ObjectType, object_id=ObjectId, status=?STATUS_ACTIVE, column=Column, value=Value}, Lo0) ->
  [{Column, Value} | lists:keydelete(Column, 1, Lo0)];
query_fun(_, _, Lo0) ->
  Lo0.

query_data(ObjectType, ObjectId, Qi0) ->
  process_data(fun query_fun/3, {ObjectType, ObjectId}, Qi0, []).

make_data(ObjectType, ObjectId, Timestamp, ?STATUS_INACTIVE, Di0) ->
  Qi0 = queue:new(),
  StorageData = storage_data(ObjectType, ObjectId, Timestamp, 1, {undefined, ?STATUS_INACTIVE, undefined}),
  add_data(StorageData, 1, Qi0, Di0);
make_data(ObjectType, ObjectId, Timestamp, Columns, Di0) ->
  make_data_acc(ObjectType, ObjectId, Timestamp, Columns, {0, queue:new(), Di0}).

make_data_acc(ObjectType, ObjectId, Timestamp, [H | T], {DataIndex0, Qi0, Di0}) ->
  DataIndex1 = DataIndex0+1,
  StorageData = storage_data(ObjectType, ObjectId, Timestamp, DataIndex1, H),
  make_data_acc(ObjectType, ObjectId, Timestamp, T, add_data(StorageData, DataIndex1, Qi0, Di0)); 
make_data_acc(_, _, _, [], Acc0) ->
  Acc0.

add_data(StorageData, DataIndex, Q0, D0) ->
  {DataIndex, queue:in(StorageData, Q0), queue:in(StorageData, D0)}.
  
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

   