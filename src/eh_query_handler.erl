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

-module(eh_query_handler).

-export([reply/3,
         error_node_unavailable/1,
         error_node_down/1,
         error_being_updated/2,
         updated/2,
         query/5,
         process_pending/3]).

-include("erlang_htas.hrl").

-callback process(ObjectType :: atom(), ObjectId :: term(), From :: pid(), Ref :: term(), State :: #eh_system_state{}) -> #eh_system_state{}.

reply(From, Ref, Reply) ->
  From ! {reply, Ref, Reply}.

error_node_down(NodeId) ->
  error_node(NodeId, ?EH_NODEDOWN).

error_node_unavailable(NodeId) ->
  error_node(NodeId, ?EH_NODE_UNAVAILABLE).

error_being_updated(ObjectType, ObjectId) -> 
  object(error, ObjectType, ObjectId, ?EH_BEING_UPDATED).

updated(ObjectType, ObjectId) ->
  object(ok, ObjectType, ObjectId, ?EH_UPDATED).
 
object_tuple(ObjectType, ObjectId, Msg) ->
  {ObjectType, ObjectId, Msg}.

node_tuple(NodeId, Msg) ->
  {NodeId, Msg}.

error_node(NodeId, Msg) ->
  {error, node_tuple(NodeId, Msg)}.

object(Tag, ObjectType, ObjectId, Msg) ->
  {Tag, object_tuple(ObjectType, ObjectId, Msg)}.

query(ObjectType, ObjectId, From, Ref, #eh_system_state{app_config=AppConfig}=State) ->
  reply(From, Ref, {ok, query_reply(ObjectType, ObjectId, AppConfig)}),
  State.

process_pending(ObjectType, ObjectId, #eh_system_state{query_data=QueryData, app_config=AppConfig}=State) ->
  case maps:find({ObjectType, ObjectId}, QueryData) of
    error      ->
      ok;
    {ok, []}   ->
      ok;
    {ok, List} ->
      QueryReply = query_reply(ObjectType, ObjectId, AppConfig),
      lists:foreach(fun({From, Ref}) -> reply(From, Ref, {ok, QueryReply}) end, List)
  end,
  eh_node_timestamp:update_state_remove_query_data(ObjectType, ObjectId, State).

query_reply(ObjectType, ObjectId, AppConfig) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:query({ObjectType, ObjectId}).


