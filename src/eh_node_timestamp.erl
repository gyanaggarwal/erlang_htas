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

-module(eh_node_timestamp).

-export([update_state_client_reply/2,
         update_state_completed_set/2,
         update_state_new_msg/4,
         update_state_timestamp/2,
         update_state_msg_data/3,
         update_state_merge_completed_set/2,
         update_state_add_query_data/5,
         update_state_remove_query_data/3,
         valid_pre_update_msg/3,
         valid_update_msg/3,
         valid_add_node_msg/2]).

-include("erlang_htas.hrl").

update_state_add_query_data(ObjectType, ObjectId, From, Ref, #eh_system_state{query_data=QueryData}=State) ->
  Key = {ObjectType, ObjectId},
  Value = {From, Ref},
  List1 = case maps:find(Key, QueryData) of
            error      ->
              [Value];
            {ok, List} ->
              [Value | List]
          end,
  State#eh_system_state{query_data=maps:put(Key, List1, QueryData)}.

update_state_remove_query_data(ObjectType, ObjectId, #eh_system_state{query_data=QueryData}=State) ->
  State#eh_system_state{query_data=maps:remove({ObjectType, ObjectId}, QueryData)}.

update_state_client_reply(UpdateMsgKey, 
                          #eh_system_state{successor=Succ, ring_completed_map=RingCompletedMap, msg_data=MsgData, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  MsgData1 = eh_system_util:remove_map(UpdateMsgKey, MsgData),
  RingCompletedMap1 = case Succ of
                        undefined ->
                          RingCompletedMap;
                        _         ->
                          eh_ring_completed_map:add_msg_key(NodeId, UpdateMsgKey, RingCompletedMap)
                      end,
  State#eh_system_state{ring_completed_map=RingCompletedMap1, msg_data=MsgData1}.

update_state_completed_set(CompletedSet, 
                           #eh_system_state{ring_completed_map=RingCompletedMap, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  State#eh_system_state{ring_completed_map=eh_ring_completed_map:subtract_replace_completed_set(NodeId, CompletedSet, RingCompletedMap)}.

update_state_merge_completed_set(OtherNodeId,
                                 #eh_system_state{ring_completed_map=RingCompletedMap, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  RingCompletedMap1 = eh_ring_completed_map:merge_completed_set(NodeId, OtherNodeId, RingCompletedMap),
  State#eh_system_state{ring_completed_map=RingCompletedMap1}.

update_state_msg_data(NodeId,
                      CompletedSet,
                      #eh_system_state{msg_data=MsgData, ring_completed_map=RingCompletedMap}=State) ->
  RingCompletedMap1 = eh_ring_completed_map:add_completed_set(NodeId, CompletedSet, RingCompletedMap),
  MsgData1 = sets:fold(fun(X, Acc) -> eh_system_util:remove_map(X, Acc) end, MsgData, eh_ring_completed_map:get_ring_completed_set(RingCompletedMap1)),
  State#eh_system_state{msg_data=MsgData1, ring_completed_map=RingCompletedMap1}.

update_state_new_msg(?EH_PRED_PRE_UPDATE, 
                     UpdateMsgKey, 
                     UpdateMsgData, 
                     #eh_system_state{pre_msg_data=PreMsgData}=State) ->
  PreMsgData1 = eh_system_util:add_map(UpdateMsgKey, UpdateMsgData, PreMsgData),
  State#eh_system_state{pre_msg_data=PreMsgData1};
update_state_new_msg(?EH_PRED_UPDATE, 
                     UpdateMsgKey, 
                     UpdateMsgData, 
                     #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  PreMsgData1 = eh_system_util:remove_map(UpdateMsgKey, PreMsgData),
  MsgData1 = eh_system_util:add_map(UpdateMsgKey, UpdateMsgData, MsgData),
  State#eh_system_state{pre_msg_data=PreMsgData1, msg_data=MsgData1}.

update_state_timestamp(MsgTimestamp, 
                       #eh_system_state{timestamp=Timestamp}=State) ->
  State#eh_system_state{timestamp=max(MsgTimestamp, Timestamp)}.

valid_add_node_msg(Node, 
                   #eh_system_state{repl_ring=ReplRing, node_state=NodeState, app_config=AppConfig}) ->
  case {eh_node_state:client_state(NodeState), Node =:= eh_system_config:get_node_id(AppConfig), lists:member(Node, ReplRing)} of
    {?EH_STATE_TRANSIENT, true, _}   ->
      ?EH_VALID_FOR_NEW;
    {?EH_STATE_NORMAL, false, false} ->
      ?EH_VALID_FOR_EXISTING;
    {_, _, _}                        ->
      ?EH_INVALID_MSG
  end.

returned_msg(#eh_update_msg_data{node_id=MsgNodeId}, 
             #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  OriginNodeId = eh_repl_ring:originating_node_id(MsgNodeId, ReplRing),
  {NodeId =:= OriginNodeId, ?EH_RETURNED_MSG, State}.

valid_msg(Fun, 
          UpdateMsgKey,
          #eh_update_msg_data{node_id=MsgNodeId}=UpdateMsgData,
          MsgData,
          MsgData1,
          IsKeyFun,
          State) ->
  case maps:find(UpdateMsgKey, MsgData) of
    error                                        ->
       {not IsKeyFun(UpdateMsgKey, MsgData1), ?EH_RING_MSG, State};
    {ok, #eh_update_msg_data{node_id=MsgNodeId}} ->
      returned_msg(UpdateMsgData, State);
    {ok, EUpdateMsgData}                         ->
      Fun(UpdateMsgKey, UpdateMsgData, EUpdateMsgData, State)   
  end.

update_conflict_resolver(_, _, _, State) ->
  {false, ?EH_RING_MSG, State}.

valid_update_msg(UpdateMsgKey,
                 UpdateMsgData,
                 #eh_system_state{msg_data=MsgData, ring_completed_map=RingCompletedMap}=State) ->
  valid_msg(fun update_conflict_resolver/4, UpdateMsgKey, UpdateMsgData, MsgData, eh_ring_completed_map:get_ring_completed_set(RingCompletedMap), fun eh_system_util:is_key_set/2, State).

pre_update_conflict_resolver(#eh_update_msg_key{object_type=ObjectType, object_id=ObjectId}=UpdateMsgKey,
                             #eh_update_msg_data{node_id=MsgNodeId},
                             #eh_update_msg_data{node_id=EMsgNodeId, client_id=ClientId, reference=Ref},
                             #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  ConflictResolver = eh_system_config:get_write_conflict_resolver(AppConfig),
  ResolvedNodeId = ConflictResolver:resolve(MsgNodeId, EMsgNodeId),
  case ResolvedNodeId =:= MsgNodeId of
    true  ->
      PreMsgData1 = eh_system_util:remove_map(UpdateMsgKey, PreMsgData),
      State1 = State#eh_system_state{pre_msg_data=PreMsgData1},
      case EMsgNodeId =:= NodeId of
        true  ->
          eh_query_handler:reply(ClientId, Ref, eh_query_handler:error_being_updated(ObjectType, ObjectId));
        false ->
          ok
      end,
      {true, ?EH_RING_MSG, State1};
    false ->
      {false, ?EH_RING_MSG, State}
  end.

valid_pre_update_msg(UpdateMsgKey,
                     UpdateMsgData,
                     #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  valid_msg(fun pre_update_conflict_resolver/4, UpdateMsgKey, UpdateMsgData, PreMsgData, MsgData, fun eh_system_util:is_key_map/2, State).









