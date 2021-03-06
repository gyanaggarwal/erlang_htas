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
  State = #eh_system_state{app_config=AppConfig},
  {ok, State}.

handle_call(?EH_DATA_VIEW, 
            _From, 
            #eh_system_state{app_config=AppConfig}=State) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  Reply = ReplDataManager:data_view(),
  {reply, Reply, State};
  
handle_call(_Msg, _From, State) ->
  {reply, ok, State}.


handle_cast({?EH_SETUP_RING, ReplRing}, 
            #eh_system_state{app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  Succ = eh_repl_ring:successor(NodeId, ReplRing),
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  FailureDetector:set(NodeId, ReplRing),
  {Timestamp, _} = ReplDataManager:timestamp(),
  NewState1 = eh_node_state:update_state_ready(State),
  NewState2 = NewState1#eh_system_state{repl_ring=ReplRing, successor=Succ, timestamp=Timestamp},
  event_state("setup_ring.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_ADD_NODE, {Node, NodeList}}, 
            #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
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
                  {Timestamp, Snapshot} = ReplDataManager:timestamp(),
                  gen_server:cast({?EH_SYSTEM_SERVER, NewPred}, {?EH_SNAPSHOT, {Node, self(), Ref, {Timestamp, Snapshot}}}),
                  NewState1 = eh_node_state:update_state_transient(State),
                  NewState1#eh_system_state{successor=NewSucc, repl_ring=NewNodeList, reference=Ref};
                ?EH_VALID_FOR_EXISTING -> 
                  NodeId = eh_system_config:get_node_id(AppConfig),
                  NewReplRing = eh_repl_ring:add(Node, ReplRing),
                  NewSucc = eh_repl_ring:successor(NodeId, NewReplRing),
                  FailureDetector:set(Node),
                  State#eh_system_state{repl_ring=NewReplRing, successor=NewSucc}; 
                _                      ->
                  State
              end,
  event_state("add_node.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_SNAPSHOT, {Node, From, Ref, {Timestamp, Snapshot}}}, 
            #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  NewReplRing = eh_repl_ring:add(Node, ReplRing),
  NewSucc = eh_repl_ring:successor(NodeId, NewReplRing),
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  Q0 = ReplDataManager:snapshot(Timestamp, Snapshot),
  gen_server:cast(From, {?EH_UPDATE_SNAPSHOT, {Ref, Q0}}),
  NewState2 = State#eh_system_state{repl_ring=NewReplRing, successor=NewSucc},
  event_state("snapshot.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_UPDATE_SNAPSHOT, {Ref, Q0}}, 
            #eh_system_state{reference=Ref, app_config=AppConfig}=State) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:update_snapshot(Q0),
  NewState2 = eh_node_state:update_state_snapshot(State),
  event_state("update_snapshot.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_UPDATE_SNAPSHOT, _}, State) ->
  {noreply, State};

handle_cast({?EH_QUERY, {From, Ref, {ObjectType, ObjectId}}}, 
            #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  State1 = case eh_node_state:data_state(State) of
             ?EH_NOT_READY ->
               eh_query_handler:reply(From, Ref, eh_query_handler:error_node_unavailable(NodeId)),
               State;
             _             ->  
               case eh_system_util:exist_map_msg(ObjectType, ObjectId, PreMsgData) of
                 false ->
                    eh_query_handler:query(ok, ObjectType, ObjectId, From, Ref, State);
                 true  -> 
                    QueryHandler = eh_system_config:get_query_handler(AppConfig),
                    QueryHandler:process(ObjectType, ObjectId, From, Ref, State)
               end
           end,
  {noreply, State1};

handle_cast({?EH_UPDATE, {From, Ref, ObjectList}},
            #eh_system_state{timestamp=Timestamp, successor=Succ, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  NewState9 = case eh_node_state:client_state(State) of
                ?EH_NOT_READY ->
                  eh_query_handler:reply(From, Ref, eh_query_handler:error_node_unavailable(NodeId)),
                  State;
                _             ->
                  Timestamp1 = Timestamp+1,
                  {NodeId, ObjectType, ObjectId, UpdateData} = lists:keyfind(NodeId, 1, ObjectList),
                  {UMsgKey, UMsgData} = eh_system_util:get_update_msg(ObjectType, 
                                                                      ObjectId,
                                                                      UpdateData,
                                                                      Timestamp1,
                                                                      From,
                                                                      NodeId,
                                                                      Ref),
                  NewState1 = eh_node_timestamp:update_state_timestamp(Timestamp1, State),
                  case Succ of 
                    undefined ->
                      reply_to_client(fun persist_data/3, UMsgKey, UMsgData, NewState1);
                    _         -> 
                      send_pre_update_msg(fun no_persist_data/3, UMsgKey, UMsgData, NewState1)
                  end                            
              end,
  event_state("update.99", NewState9),
  {noreply, NewState9};

handle_cast({?EH_PRED_PRE_UPDATE, {UMsgKey, UMsgData, CompletedSet}}, State) ->
  NewState1 = process_msg(?EH_PRED_PRE_UPDATE,
                          fun eh_node_timestamp:valid_pre_update_msg/3,
                          fun send_update_msg/4,
                          fun persist_data/3,
                          fun send_pre_update_msg/4,
                          fun no_persist_data/3,
                          UMsgKey,
                          UMsgData,
                          CompletedSet,
                          State),
  {noreply, NewState1};

handle_cast({?EH_PRED_UPDATE, {UMsgKey, UMsgData, CompletedSet}}, State) ->
  NewState1 = process_msg(?EH_PRED_UPDATE,
                          fun eh_node_timestamp:valid_update_msg/3,
                          fun reply_to_client/4,
                          fun no_persist_data/3,
                          fun send_update_msg/4,
                          fun persist_data/3,
                          UMsgKey,
                          UMsgData,
                          CompletedSet,
                          State),
  {noreply, NewState1};

handle_cast({stop, Reason}, State) ->
  event_data("stop", status, stopped),
  {stop, Reason, State};

handle_cast(_Msg, State) ->
  {noreply, State}.


handle_info(Msg, #eh_system_state{repl_ring=ReplRing, successor=Succ, app_config=AppConfig}=State) ->
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  NewState9 = case FailureDetector:detect(Msg) of
                {?EH_NODEDOWN, DownNode} ->
                  event_data("failure", node_down, eh_system_util:get_node_name(DownNode)),
                  NodeId = eh_system_config:get_node_id(AppConfig),
                  NewReplRing = eh_repl_ring:drop(DownNode, ReplRing),
                  NewSucc = eh_repl_ring:successor(NodeId, NewReplRing),
                  NewState1 = State#eh_system_state{repl_ring=NewReplRing, successor=NewSucc},
                  NewState3 = case Succ =:= DownNode of
                                true  ->
                                  NewState2 = eh_node_timestamp:update_state_merge_completed_set(DownNode, NewState1),
                                  send_down_msg(NewState2);
                                false ->
                                   NewState1
                              end,
                  event_state("failure.99", NewState3),
                  NewState3;
                _                        ->
                  State 
              end,
   {noreply, NewState9}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

event_state(Msg, State) ->
  eh_event:state(?MODULE, Msg, State).

event_message(Msg, MsgKey, MsgData, CompletedSet) ->
  eh_event:message(?MODULE, Msg, {MsgKey, MsgData, CompletedSet}).

event_message(Msg, MsgKey) ->
  eh_event:message(?MODULE, Msg, MsgKey).

event_data(Msg, DataMsg, Data) ->
  eh_event:data(?MODULE, Msg, DataMsg, Data).

persist_data(#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId}, 
             #eh_update_msg_data{update_data=UpdateData}, 
             #eh_system_state{app_config=AppConfig}=State) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:update(eh_node_state:data_state(State), Timestamp, {ObjectType, ObjectId, UpdateData}),
  eh_query_handler:process_pending(ObjectType, ObjectId, State).

no_persist_data(_, _, State) ->
  State.

reply_to_client(PersistFun,
                #eh_update_msg_key{object_type=ObjectType, object_id=ObjectId}=UMsgKey, 
                #eh_update_msg_data{client_id=ClientId, reference=Ref}=UMsgData,
                State) -> 
  State1 = PersistFun(UMsgKey, UMsgData, State),
  eh_query_handler:reply(ClientId, Ref, eh_query_handler:updated(ObjectType, ObjectId)),
  eh_node_timestamp:update_state_client_reply(UMsgKey, State1).

send_msg(Tag, 
         PersistFun,
         UMsgKey, 
         #eh_update_msg_data{node_id=MsgNodeId}=UMsgData,
         #eh_system_state{successor=Succ, repl_ring=ReplRing}=State) ->
  State1 = PersistFun(UMsgKey, UMsgData, State),
  State2 = eh_node_timestamp:update_state_new_msg(Tag, UMsgKey, UMsgData, State1),
  OriginNodeId = eh_repl_ring:originating_node_id(MsgNodeId, ReplRing),
  gen_server:cast({?EH_SYSTEM_SERVER, Succ}, 
                  {Tag, {UMsgKey, UMsgData, eh_ring_completed_map:get_completed_set(OriginNodeId, State2#eh_system_state.ring_completed_map)}}),
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
  eh_system_util:fold_map(fun(K, V, S) -> process_down_msg(fun persist_data/3, K, V, S) end, State, PreMsgData),
  eh_system_util:fold_map(fun(K, V, S) -> process_down_msg(fun no_persist_data/3, K, V, S) end, State, MsgData),  
  State#eh_system_state{pre_msg_data=eh_system_util:new_map(), msg_data=eh_system_util:new_map(), ring_completed_map=eh_system_util:new_map()};
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
  eh_system_util:fold_map(fun(K, V, S) -> down_fold_fun(PersistRingFun, RingFun, PersistReturnedFun, ReturnedFun, NodeId, ReplRing, K, V, S) end, State, MsgData).

process_down_msg(PersistFun,
                 UMsgKey,
                 UMsgData,
                 State) ->
  reply_to_client(PersistFun, UMsgKey, UMsgData, State).

process_msg(Tag,
            ValidateMsgFun,
            ReturnedMsgFun,
            ReturnedMsgPersistFun,
            ValidMsgFun,
            ValidMsgPersistFun, 
            #eh_update_msg_key{timestamp=MsgTimestamp}=UMsgKey, 
            #eh_update_msg_data{node_id=MsgNodeId}=UMsgData, 
            CompletedSet, 
            State) ->
  DisplayTag = eh_system_util:display_atom_to_list(Tag),
  NewState8 = case eh_node_state:msg_state(State) of
                ?EH_NOT_READY ->
                  event_message(DisplayTag++".invalid_msg", UMsgKey),
                  State;
                _             ->
                  case ValidateMsgFun(UMsgKey, UMsgData, State) of
                    {false, _, NewState1}               ->
                      event_message(DisplayTag++".duplicate_msg", UMsgKey),
                      NewState1;
                    {true, ?EH_RETURNED_MSG, NewState1} ->
                      event_message(DisplayTag++".valid_returned_msg", UMsgKey, UMsgData, CompletedSet),
                      NewState2 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState1),
                      NewState3 = eh_node_timestamp:update_state_completed_set(CompletedSet, NewState2),
                      ReturnedMsgFun(ReturnedMsgPersistFun, UMsgKey, UMsgData, NewState3);
                    {true, _, NewState1}                ->
                      event_message(DisplayTag++".valid_msg", UMsgKey, UMsgData, CompletedSet),
                      NewState2 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState1),
                      NewState3 = eh_node_timestamp:update_state_msg_data(MsgNodeId, CompletedSet, NewState2),
                      ValidMsgFun(ValidMsgPersistFun, UMsgKey, UMsgData, NewState3)
                  end
              end,
  event_state(DisplayTag++".99", NewState8),
  NewState8.


                 




