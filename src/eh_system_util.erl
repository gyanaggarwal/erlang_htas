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

-module(eh_system_util).

-export([get_node_name/1,
         get_node_atom/1,
         get_file_name/3,
         get_node_timestamp/2,
         get_object_map/3,
         add_pre_update_msg/2,
         remove_pre_update_msg/2,
         add_update_msg/2,
         remove_update_msg/2,
         cleanup_msg/1,
         get_successor_message/2,
         get_update_timestamp/1,
         get_pre_update_timestamp/1,
         make_list_to_string/2,
         exist_pre_update_msg/3,
         valid_pre_update_message/2,
         valid_update_message/2,
         returned_message/2,
         valid_add_node_message/2,
         reply/3]).

-include("erlang_htas.hrl").

-spec get_node_name(Node :: atom()) -> string().
get_node_name(Node) ->
  lists:takewhile(fun(X) -> X =/= $@ end, atom_to_list(Node)).

-spec get_node_atom(Node :: atom()) -> atom().
get_node_atom(Node) ->
  list_to_atom(get_node_name(Node)).

-spec get_file_name(NodeName :: string(), DataDir :: string(), FileName :: string()) -> string().
get_file_name(NodeName, DataDir, FileName) ->
  DataDir ++ NodeName ++ FileName.

reply(From, Ref, Reply) ->
  From ! {reply, Ref, Reply}.

get_node_timestamp(NodeId, RingTimestamp) ->
  case maps:find(NodeId, RingTimestamp) of
    error               ->
      #eh_node_timestamp{};
    {ok, NodeTimestamp} ->
      NodeTimestamp
  end.

get_object_map(ObjectType, ObjectId, Map) ->
  case maps:find({ObjectType, ObjectId}, Map) of
    error           ->
      maps:new();
    {ok, ObjectMap} ->
      ObjectMap
  end.

add_pre_update_msg(#eh_update_msg{object_type=ObjectType, object_id=ObjectId, timestamp=Timestamp}=UpdateMsg, Map) ->
  ObjectMap = get_object_map(ObjectType, ObjectId, Map),
  NewObjectMap = maps:put(Timestamp, UpdateMsg, ObjectMap),
  maps:put({ObjectType, ObjectId}, NewObjectMap, Map).

remove_pre_update_msg(#eh_update_msg{object_type=ObjectType, object_id=ObjectId, timestamp=Timestamp}, Map) ->
  ObjectMap = get_object_map(ObjectType, ObjectId, Map),
  NewObjectMap = maps:remove(Timestamp, ObjectMap),
  maps:put({ObjectType, ObjectId}, NewObjectMap, Map).

add_update_msg(#eh_update_msg{timestamp=Timestamp}=UpdateMsg, Map) ->
  maps:put(Timestamp, UpdateMsg, Map).

remove_update_msg(#eh_update_msg{timestamp=Timestamp}, Map) ->
  maps:remove(Timestamp, Map).

cleanup_msg(#eh_system_state{successor=Succ, ring_timestamp=RingTimestamp, msg_data=MsgData, pre_msg_data=PreMsgData}=State) ->
  NodeTimestamp = get_node_timestamp(Succ, RingTimestamp),
  NewMsgData = cleanup_update_msg(NodeTimestamp#eh_node_timestamp.pred_update, MsgData),
  NewPreMsgData = cleanup_pre_update_msg(NodeTimestamp#eh_node_timestamp.pred_pre_update, PreMsgData),
  State#eh_system_state{msg_data=NewMsgData, pre_msg_data=NewPreMsgData}.

cleanup_update_msg(Timestamp, MsgMap) ->
  maps:fold(fun(K, _, Acc) -> case K =< Timestamp of
                                true  -> maps:remove(K, Acc);
                                false -> Acc
                              end end, MsgMap, MsgMap).

cleanup_pre_update_msg(Timestamp, PreMsgMap) ->
  maps:fold(fun(K, V, Acc) -> maps:put(K, cleanup_update_msg(Timestamp, V), Acc) end, PreMsgMap, PreMsgMap).

get_successor_message(SuccId, #eh_system_state{msg_data=MsgData, pre_msg_data=PreMsgData, ring_timestamp=RingTimestamp}) ->
  #eh_node_timestamp{pred_pre_update=PredPreUpdate, pred_update=PredUpdate} = get_node_timestamp(SuccId, RingTimestamp),
  {get_successor_pre_update_message(PredPreUpdate, PreMsgData), get_successor_update_message(PredUpdate, MsgData)}. 

get_successor_pre_update_message(Timestamp, Map) ->
  Values = maps:values(Map),
  lists:foldl(fun(M, Acc) -> get_successor_update_message(Timestamp, M) ++ Acc end, [], Values).

get_successor_update_message(Timestamp, Map) ->
  maps:fold(fun(K, V, Acc) -> case K > Timestamp of
                                true  -> [V | Acc];
                                false -> Acc
                              end end, [], Map).  

get_update_timestamp(Map) ->
  maps:keys(Map).

get_pre_update_timestamp(Map) ->
  Values = maps:values(Map),
  lists:foldl(fun(M, Acc) -> get_update_timestamp(M) ++ Acc end, [], Values).

make_list_to_string(Fun, List) ->
  lists:foldl(fun(N, Acc) -> case length(Acc) of
                               0 -> Acc ++ Fun(N);
                               _ -> Acc ++ "," ++ Fun(N)
                             end end, [], List).

exist_pre_update_msg(ObjectType, ObjectId, Map) ->
  case maps:find({ObjectType, ObjectId}, Map) of
    error           ->
      false;
    {ok, ObjectMap} ->
      maps:size(ObjectMap) > 0
  end.

valid_pre_update_message(#eh_update_msg{object_type=ObjectType, 
                                        object_id=ObjectId, 
                                        timestamp=Timestamp, 
                                        node_id=MsgNodeId}=UpdateMsg,
                         #eh_system_state{pre_msg_data=PreMsgData,
                                          msg_data=MsgData, 
                                          app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  case returned_message(UpdateMsg, State) of
    true  ->
      {true, State};
    false ->
      case maps:find({ObjectType, ObjectId}, PreMsgData) of
        error           ->
          {true, State};
        {ok, ObjectMap} ->
          case  maps:find(Timestamp, ObjectMap) of
            error           ->
              {true, State};
            {ok, #eh_update_msg{node_id=ENodeId, client_id=EClientId, reference=ERef}=EUpdateMsg} ->
              case MsgNodeId =:= ENodeId of
                true  ->
                  {false, State};
                false ->
                  ConflictResolver = eh_system_config:get_write_conflict_resolver(AppConfig),
                  ResolvedNodeId = ConflictResolver:resolve(MsgNodeId, ENodeId),
                  case ResolvedNodeId =:= MsgNodeId of
                    true  ->
                      NewPreMsgData = remove_pre_update_msg(EUpdateMsg, PreMsgData),
                      NewMsgData = remove_update_msg(EUpdateMsg, MsgData),
                      NewState = State#eh_system_state{pre_msg_data=NewPreMsgData, msg_data=NewMsgData},
                      case ENodeId =:= NodeId of
                        true  ->
                          reply(EClientId, ERef, {ENodeId, ObjectType, ObjectId, ?EH_BEING_UPDATED});
                        false ->
                          ok
                      end,
                      {true, NewState};
                    false ->
                      {false, State}
                  end
              end
          end
      end
  end.

valid_update_message(#eh_update_msg{timestamp=Timestamp}=UpdateMsg,
                     #eh_system_state{msg_data=MsgData}=State) ->
  case returned_message(UpdateMsg, State) of
    true  ->
      true;
    false ->
      case maps:find(Timestamp, MsgData) of
        error   ->
          true;
        {ok, _} ->
          false
      end
  end.

returned_message(#eh_update_msg{node_id=MsgNodeId}, #eh_system_state{app_config=AppConfig}) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  NodeId =:= MsgNodeId.

valid_add_node_message(Node, #eh_system_state{repl_ring=ReplRing, node_state=NodeState, app_config=AppConfig}) ->
  case {eh_node_state:client_state(NodeState), Node =:= eh_system_config:get_node_id(AppConfig), lists:member(Node, ReplRing)} of
    {?EH_STATE_TRANSIENT, true, _}   ->
      ?EH_VALID_FOR_NEW;
    {?EH_STATE_NORMAL, false, false} ->
      ?EH_VALID_FOR_EXISTING;
    {_, _, _}                        ->
      ?EH_INVALID_MSG
  end.



