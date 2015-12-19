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

-module(erlang_htas).

-export([start/0, 
         stop/0,
         setup_ring/1,
         query/3,
         delete/3,
         update/5]).

-include("erlang_htas.hrl").

start() ->
  application:start(erlang_htas).

stop() ->
  application:stop(erlang_htas).

setup_ring(NodeList) ->
  gen_server:abcast(NodeList, ?EH_SYSTEM_SERVER, ?EH_SETUP_RING).

query(Node, ObjectType, ObjectId) ->
  send(Node, ?EH_QUERY, {ObjectType, ObjectId}, ?READ_TIMEOUT).

delete(Node, ObjectType, ObjectId) ->
  send(Node, ?EH_UPDATE, {ObjectType, ObjectId, ?STATUS_INACTIVE}, ?UPDATE_TIMEOUT).

update(Node, ObjectType, ObjectId, UpdateColumns, DeleteColumns) ->
  Columns = combine_columns(UpdateColumns, DeleteColumns),
  send(Node, ?EH_UPDATE, {ObjectType, ObjectId, Columns}, ?UPDATE_TIMEOUT).

send(Node, MsgTag, Msg, Timeout) ->
  Ref = make_ref(),
  gen_server:cast({?EH_SYSTEM_SERVER, Node}, {MsgTag, {self(), Ref, Msg}}),
  receive
    {reply, Ref, Reply} ->
      Reply
  after Timeout ->
      {error, {?EH_NODEDOWN, Node}}
  end.

combine_columns(UpdateColumns, DeleteColumns) ->
  get_columns(lists:reverse(UpdateColumns), get_columns(lists:reverse(DeleteColumns), [])).

get_columns([{Column, Value} | T], Acc) ->
  get_columns(T, [{Column, ?STATUS_ACTIVE, Value} | Acc]);
get_columns([Column | T], Acc) ->
  get_columns(T, [{Column, ?STATUS_INACTIVE} | Acc]);
get_columns([], Acc) ->
  Acc.