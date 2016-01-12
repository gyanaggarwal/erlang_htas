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

-module(eh_system_server).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erlang_htas.hrl").

-define(SERVER, ?EH_SYSTEM_SERVER).

start_link(AppConfig) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [AppConfig], []).


init([AppConfig]) ->
  ReplRing = eh_system_config:get_repl_ring(AppConfig),
  NodeId = eh_system_config:get_node_id(AppConfig),
  Succ = eh_repl_ring:successor(NodeId, ReplRing),
  State = #eh_system_state{repl_ring=ReplRing, successor=Succ, app_config=AppConfig},
  {ok, State}.

handle_call(?EH_DATA_VIEW, 
            _From, 
            #eh_system_state{app_config=AppConfig}=State) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  Reply = ReplDataManager:data_view(),
  {reply, Reply, State};
  
handle_call(_Msg, _From, State) ->
  {reply, ok, State}.


handle_cast(?EH_SETUP_RING, 
            #eh_system_state{repl_ring=ReplRing, node_state=NodeState, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  FailureDetector:set(NodeId, ReplRing),
  {Timestamp, _} = ReplDataManager:timestamp(),
  NewState2 = State#eh_system_state{timestamp=Timestamp, node_state=eh_node_state:state(NodeState)},
  event_state("handle_cast.eh_setup_ring.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_ADD_NODE, {Node, NodeList}}, 
            #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
  event_state("handle_cast.eh_add_node.00", State),
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  NewState2 = case eh_node_timestamp:valid_add_node_msg(Node, State) of
                ?EH_VALID_FOR_NEW      ->
                  NewNodeList = eh_repl_ring:add(Node, NodeList), 
                  NewSucc = eh_repl_ring:successor(Node, NewNodeList),
                  NewPred = eh_repl_ring:predecessor(Node, NewNodeList),
                  UniqueIdGenerator = eh_system_config:get_unique_id_generator(AppConfig),
                  Ref = UniqueIdGenerator:unique_id(),
                  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
                  FailureDetector:set(Node, NewNodeList),
                  {Timestamp, DataIndex} = ReplDataManager:timestamp(),
                  gen_server:cast({?EH_SYSTEM_SERVER, NewPred}, {?EH_SNAPSHOT, {self(), Ref, {Timestamp, DataIndex}}}),
                  State#eh_system_state{successor=NewSucc, repl_ring=NewNodeList, reference=Ref};
                ?EH_VALID_FOR_EXISTING -> 
                  NodeId = eh_system_config:get_node_id(AppConfig),
                  NewReplRing = eh_repl_ring:add(Node, ReplRing),
                  NewSucc = eh_repl_ring:successor(NodeId, NewReplRing),
                  FailureDetector:set(Node),
                  State#eh_system_state{repl_ring=NewReplRing, successor=NewSucc}; 
                _                      ->
                  State
              end,
  event_state("handle_cast.eh_add_node.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_SNAPSHOT, {From, Ref, {Timestamp, DataIndex}}}, 
            #eh_system_state{app_config=AppConfig}=State) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  Q0 = ReplDataManager:snapshot(Timestamp, DataIndex),
  gen_server:cast(From, {?EH_UPDATE_SNAPSHOT, {Ref, Q0}}),  
  event_state("handle_cast.eh_snapshot.99", State),
  {noreply, State};

handle_cast({?EH_UPDATE_SNAPSHOT, {Ref, Q0}}, 
            #eh_system_state{node_state=NodeState, reference=Ref, app_config=AppConfig}=State) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:update_snapshot(Q0),
  NewState2 = State#eh_system_state{node_state=eh_node_state:update_snapshot(NodeState)},
  event_state("handle_cast.eh_snapshot.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_UPDATE_SNAPSHOT, _}, State) ->
  {noreply, State};

handle_cast({?EH_QUERY, {From, Ref, {ObjectType, ObjectId}}}, 
            #eh_system_state{pre_msg_data=PreMsgData, node_state=NodeState, app_config=AppConfig}=State) ->
  Reply = case eh_node_state:data_state(NodeState) of
            ?EH_STATE_TRANSIENT ->
              {error, ?EH_NODE_UNAVAILABLE};
            _                   ->  
              case eh_system_util:exist_map_msg(ObjectType, ObjectId, PreMsgData) of
                false ->
                  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
                  {ok, ReplDataManager:query({ObjectType, ObjectId})};
                true  ->
                  {error, {ObjectType, ObjectId, ?EH_BEING_UPDATED}}
              end
          end,
  eh_system_util:reply(From, Ref, Reply),
  {noreply, State};

handle_cast({?EH_UPDATE, {From, Ref, {ObjectType, ObjectId, UpdateData}}},
            #eh_system_state{timestamp=Timestamp, node_state=NodeState, successor=Succ, app_config=AppConfig}=State) ->
  NewState9 = case eh_node_state:client_state(NodeState) of
                ?EH_STATE_TRANSIENT ->
                  State;
                _                   ->
                  Timestamp1 = Timestamp+1,
                  NodeId = eh_system_config:get_node_id(AppConfig),
                  {UMsgKey, UMsgData} = eh_system_util:get_update_msg(ObjectType, 
                                                                      ObjectId,
                                                                      UpdateData,
                                                                      Timestamp1,
                                                                      From,
                                                                      NodeId,
                                                                      Ref),
                  case Succ of 
                    undefined ->
                      NewState1 = eh_node_timestamp:update_state_timestamp(Timestamp1, State),
                      reply_to_client(fun persist_data/3, UMsgKey, UMsgData, NewState1);
                    _         -> 
                      send_pre_update_msg(fun no_persist_data/3, UMsgKey, UMsgData, State)
                  end                            
              end,
  event_state("handle_cast.eh_update.99", NewState9),
  {noreply, NewState9};

handle_cast({?EH_PRED_PRE_UPDATE, {UMsgKey, #eh_update_msg_data{node_id=MsgNodeId}=UMsgData, CompletedSet}}, 
            #eh_system_state{node_state=NodeState}=State) ->
  event_message("handle_cast.eh_pred_pre_update.00", UMsgKey, UMsgData, CompletedSet),
  NewState7 = case eh_node_timestamp:valid_pre_update_msg(UMsgKey, UMsgData, State) of
                {false, _, NewState1}               ->
                  NewState1;
                {true, ?EH_RETURNED_MSG, NewState1} ->
                   NewState2 = eh_node_timestamp:update_state_completed_set(CompletedSet, NewState1),
                   send_update_msg(fun persist_data/3, UMsgKey, UMsgData, NewState2);
                {true, _, NewState1}                ->
                   NewState2 = eh_node_timestamp:update_state_msg_data(MsgNodeId, CompletedSet, NewState1),
                   send_pre_update_msg(fun no_persist_data/3, UMsgKey, UMsgData, NewState2)
              end,
  NewState8 = NewState7#eh_system_state{node_state=eh_node_state:pre_update_msg(NodeState)},
  event_state("handle_cast.eh_pred_pre_update.99", NewState8),
  {noreply, NewState8};

handle_cast({?EH_PRED_UPDATE, {#eh_update_msg_key{timestamp=MsgTimestamp}=UMsgKey, #eh_update_msg_data{node_id=MsgNodeId}=UMsgData, CompletedSet}}, 
            State) ->
  event_message("handle_cast.eh_pred_update.00", UMsgKey, UMsgData, CompletedSet),
  NewState8 = case eh_node_timestamp:valid_update_msg(UMsgKey, UMsgData, State) of
                {false, _, NewState1}               ->
                  NewState1;
                {true, ?EH_RETURNED_MSG, NewState1} ->
                  NewState2 = eh_node_timestamp:update_state_completed_set(CompletedSet, NewState1),
                  NewState3 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState2),
                  reply_to_client(fun no_persist_data/3, UMsgKey, UMsgData, NewState3);
                {true, _, NewState1}                ->
	          NewState2 = eh_node_timestamp:update_state_msg_data(MsgNodeId, CompletedSet,	NewState1),
                  send_update_msg(fun persist_data/3, UMsgKey, UMsgData, NewState2)
              end,
  event_state("handle_cast.eh_pred_update.99", NewState8),
  {noreply, NewState8};

handle_cast({stop, Reason}, State) ->
  {stop, Reason, State};

handle_cast(_Msg, State) ->
  {noreply, State}.


handle_info(Msg, #eh_system_state{repl_ring=ReplRing, successor=Succ, app_config=AppConfig}=State) ->
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  NewState9 = case FailureDetector:detect(Msg) of
                {?EH_NODEDOWN, DownNode} ->
                  event_data("handle_info.01", "failure", {?EH_NODEDOWN, eh_system_util:get_node_name(DownNode)}),
                  NodeId = eh_system_config:get_node_id(AppConfig),
                  NewReplRing = eh_repl_ring:drop(DownNode, ReplRing),
                  NewSucc = eh_repl_ring:successor(NodeId, NewReplRing),
                  NewState1 = State#eh_system_state{repl_ring=NewReplRing, successor=NewSucc},
                  case Succ =:= DownNode of
                    true  ->
                      NewState2 = eh_node_timestamp:update_state_merge_completed_set(DownNode, NewState1),
                      send_down_msg(NewState2);
                    false ->
                      NewState1
                  end;
                _                        ->
                  State 
              end,
  event_state("handle_info.99", NewState9),
  {noreply, NewState9}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

event_state(Msg, State) ->
  eh_event:state(?MODULE, Msg, State).

event_message(Msg, MsgKey, MsgData, CompletedSet) ->
  eh_event:message(?MODULE, Msg, {MsgKey, MsgData, CompletedSet}).

event_data(Msg, DataMsg, Data) ->
  eh_event:data(?MODULE, Msg, DataMsg, Data).

persist_data(#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId}, 
             #eh_update_msg_data{update_data=UpdateData}, 
             #eh_system_state{node_state=NodeState, app_config=AppConfig}) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:update(eh_node_state:data_state(NodeState), Timestamp, {ObjectType, ObjectId, UpdateData}).

no_persist_data(_, _, _) ->
  ok.

reply_to_client(PersistFun,
                #eh_update_msg_key{object_type=ObjectType, object_id=ObjectId}=UMsgKey, 
                #eh_update_msg_data{client_id=ClientId, reference=Ref}=UMsgData,
                State) -> 
  PersistFun(UMsgKey, UMsgData, State),
  eh_system_util:reply(ClientId, Ref, {ObjectType, ObjectId, ?EH_UPDATED}),
  eh_node_timestamp:update_state_client_reply(UMsgKey, State).

send_msg(Tag, 
         PersistFun,
         #eh_update_msg_key{timestamp=MsgTimestamp}=UMsgKey, 
         #eh_update_msg_data{node_id=MsgNodeId}=UMsgData,
         #eh_system_state{successor=Succ, repl_ring=ReplRing}=State) ->
  PersistFun(UMsgKey, UMsgData, State),
  State1 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, State),
  State2 = eh_node_timestamp:update_state_new_msg(Tag, UMsgKey, UMsgData, State1),
  OriginNodeId = eh_repl_ring:originating_node_id(MsgNodeId, ReplRing),
  gen_server:cast({?EH_SYSTEM_SERVER, Succ}, {Tag, {UMsgKey, UMsgData, eh_ring_completed_map:get_completed_set(OriginNodeId, State2#eh_system_state.ring_completed_map)}}),
  State2.  

send_pre_update_msg(PersistFun,
                    UMsgKey,
                    UMsgData,
                    State) ->
  send_msg(?EH_PRED_PRE_UPDATE, PersistFun, UMsgKey, UMsgData, State).
 
send_update_msg(PersistFun,
                UMsgKey,
	        UMsgData,
		State) ->
  send_msg(?EH_PRED_UPDATE, PersistFun, UMsgKey, UMsgData, State).

send_down_msg(#eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData, successor=undefined}=State) ->
  maps:fold(fun(K, V, S) -> process_down_msg(fun persist_data/3, K, V, S) end, State, PreMsgData),
  maps:fold(fun(K, V, S) -> process_down_msg(fun no_persist_data/3, K, V, S) end, State, MsgData),  
  State#eh_system_state{pre_msg_data=maps:new(), msg_data=maps:new(), ring_completed_map=maps:new()};
send_down_msg(#eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  State1 = send_down_msg(fun no_persist_data/3, fun send_pre_update_msg/4, fun persist_data/3, fun send_update_msg/4, PreMsgData, State),
  send_down_msg(fun no_persist_data/3, fun send_update_msg/4, fun no_persist_data/3, fun reply_to_client/4, MsgData, State1).

down_fold_fun(PersistRingFun,
              RingFun,
              PersistReturnedFun,
              ReturnedFun,
              NodeId,
              ReplRing,
              UMsgKey, 
              #eh_update_msg_data{node_id=MsgNodeId}=UMsgData,
              State) ->
  OriginNodeId = eh_repl_ring:originating_node_id(MsgNodeId, ReplRing),
  case NodeId =:= OriginNodeId of
    true  ->
      ReturnedFun(PersistReturnedFun, UMsgKey, UMsgData, State);
    false ->
      RingFun(PersistRingFun, UMsgKey, UMsgData, State)
  end.

send_down_msg(PersistRingFun,
              RingFun,
              PersistReturnedFun,
              ReturnedFun,
              MsgData,
              #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  maps:fold(fun(K, V, S) -> down_fold_fun(PersistRingFun, RingFun, PersistReturnedFun, ReturnedFun, NodeId, ReplRing, K, V, S) end, State, MsgData).

process_down_msg(PersistFun,
                 UMsgKey,
                 UMsgData,
                 State) ->
  reply_to_client(PersistFun, UMsgKey, UMsgData, State).

                 




