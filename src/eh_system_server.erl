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


handle_call(_Msg, _From, State) ->
  {reply, ok, State}.


handle_cast(?EH_SETUP_RING, 
            #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  FailureDetector:set(NodeId, ReplRing),
  Timestamp = ReplDataManager:timestamp(),
  NewState2 = State#eh_system_state{timestamp=Timestamp},
  event_state("handle_cast.eh_setup_ring.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_QUERY, {From, Ref, {ObjectType, ObjectId}}}, 
            #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig}=State) ->
  Reply = case eh_system_util:exist_pre_update_msg(ObjectType, ObjectId, PreMsgData) of
            false ->
              ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
              {ok, ReplDataManager:query({ObjectType, ObjectId})};
            true  ->
              {error, {ObjectType, ObjectId, ?EH_BEING_UPDATED}}
          end,
  eh_system_util:reply(From, Ref, Reply),
  {noreply, State};

handle_cast({?EH_UPDATE, {From, Ref, {ObjectType, ObjectId, UpdateData}}}, 
            #eh_system_state{timestamp=Timestamp, msg_data=MsgData, ring_timestamp=RingTimestamp, app_config=AppConfig}=State) ->
  NewTimestamp = Timestamp+1,
  NodeId = eh_system_config:get_node_id(AppConfig),
  UpdateMsg = #eh_update_msg{object_type=ObjectType,
                             object_id=ObjectId,
                             update_data=UpdateData,
                             timestamp=NewTimestamp,
                             client_id=From,
                             node_id=NodeId,
                             reference=Ref},
  event_message("handle_cast.eh_update.00", UpdateMsg),
  NewMsgData = eh_system_util:add_update_msg(UpdateMsg, MsgData),
  NewRingTimestamp = eh_node_timestamp:update_ring_timestamp(?EH_UPDATE_INITIATED_MSG, NewTimestamp, NodeId, RingTimestamp, RingTimestamp),
  NewState1 = State#eh_system_state{timestamp=NewTimestamp, msg_data=NewMsgData, ring_timestamp=NewRingTimestamp},
  NewState2 = case State#eh_system_state.successor of
                undefined ->
                  store_data(UpdateMsg, AppConfig),
                  send_message_client(UpdateMsg, NewState1);
                _Succ     ->
                  send_pre_update_message_successor(UpdateMsg, NewState1)
              end,
  event_state("handle_cast.eh_update.99", NewState2),                           
  {noreply, NewState2};

handle_cast({?EH_PRED_PRE_UPDATE, {#eh_update_msg{timestamp=MsgTimestamp, node_id=MsgNodeId}=UpdateMsg, TrgRingTimestamp}}, 
            #eh_system_state{repl_ring=ReplRing, successor=Succ, app_config=AppConfig}=State) ->
  event_message("handle_cast.eh_pred_pre_update.00", UpdateMsg),
  NewState9 = case valid_pre_update_message(UpdateMsg, State) of
                {false, NewState1} ->
                  NewState1;
                {true,  NewState1} ->
                  NewState2 = update_state_timestamp(?EH_PRED_PRE_UPDATE_MSG, MsgTimestamp, TrgRingTimestamp, NewState1),
                  OriginNodeId = eh_repl_ring:originating_node_id(MsgNodeId, ReplRing),
                  NodeId = eh_system_config:get_node_id(AppConfig),
                  case {NodeId =:= OriginNodeId, Succ} of
                    {true, undefined} -> 
                      store_data(UpdateMsg, AppConfig),
                      send_message_client(UpdateMsg, NewState2);
                    {true,  _}        -> 
                      send_update_message_successor(UpdateMsg, NewState2);
                    {false, _}        ->
                      send_pre_update_message_successor(UpdateMsg, NewState2)
                  end
              end,
  event_state("handle_cast.eh_pred_pre_update.99", NewState9),
  {noreply, NewState9};

handle_cast({?EH_PRED_UPDATE, {#eh_update_msg{timestamp=MsgTimestamp, node_id=MsgNodeId}=UpdateMsg, TrgRingTimestamp}}, 
            #eh_system_state{repl_ring=ReplRing, successor=Succ, app_config=AppConfig}=State) ->
  event_message("handle_cast.eh_pred_update.00", UpdateMsg),
  NewState9 = case valid_update_message(UpdateMsg, State) of
                false ->
                  State;
                true  ->
                  NewState1 = update_state_timestamp(?EH_PRED_UPDATE_MSG, MsgTimestamp, TrgRingTimestamp, State),
                  OriginNodeId = eh_repl_ring:originating_node_id(MsgNodeId, ReplRing),
                  NodeId = eh_system_config:get_node_id(AppConfig),
                  case {NodeId =:= OriginNodeId, Succ =:= undefined} of
                    {false, false} ->
                      send_update_message_successor(UpdateMsg, NewState1);
                    {_, _}         ->
                      send_message_client(UpdateMsg, NewState1)
                  end
              end,
  event_state("handle_cast.eh_pred_update.99", NewState9),
  {noreply, NewState9};

handle_cast(_Msg, State) ->
  {noreply, State}.


handle_info(Msg, #eh_system_state{repl_ring=ReplRing, successor=Succ, app_config=AppConfig}=State) ->
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  NewState = case FailureDetector:detect(Msg) of
               {?EH_NODEDOWN, DownNode} ->
                 event_data("handle_info.01", "failure", {?EH_NODEDOWN, DownNode}),
                 NodeId = eh_system_config:get_node_id(AppConfig),
                 NewReplRing = eh_repl_ring:drop(DownNode, ReplRing),
                 NewSucc = eh_repl_ring:successor(NodeId, NewReplRing),
                 case Succ =:= DownNode of
                   true  ->
                     State1 = process_last_msg_succ(DownNode, State), 
                     send_message_successor(NewSucc, State1);
                   false ->
                     ok
                 end,
                 State#eh_system_state{repl_ring=NewReplRing, successor=NewSucc};
               ok                   ->
                 State 
             end,
  event_state("handle_info.99", NewState),
  {noreply, NewState}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

event_state(Msg, State) ->
  eh_event:state(?MODULE, Msg, State).

event_message(Msg, Message) ->
  eh_event:message(?MODULE, Msg, Message).

event_data(Msg, DataMsg, Data) ->
  eh_event:data(?MODULE, Msg, DataMsg, Data).

store_data(#eh_update_msg{object_type=ObjectType, object_id=ObjectId, update_data=UpdateData, timestamp=Timestamp}, AppConfig) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:update(Timestamp, {ObjectType, ObjectId, UpdateData}).

send_message_client(#eh_update_msg{object_type=ObjectType, object_id=ObjectId, timestamp=Timestamp, client_id=ClientId, reference=Ref}=UpdateMsg, 
                    #eh_system_state{msg_data=MsgData, ring_timestamp=RingTimestamp, app_config=AppConfig}=State) ->
  NewMsgData = eh_system_util:remove_update_msg(UpdateMsg, MsgData),
  NewRingTimestamp = eh_node_timestamp:update_ring_timestamp(?EH_UPDATE_COMPLETED_MSG, Timestamp, eh_system_config:get_node_id(AppConfig), RingTimestamp, RingTimestamp),
  eh_system_util:reply(ClientId, Ref, {ObjectType, ObjectId, ?EH_UPDATED}),
  State#eh_system_state{ring_timestamp=NewRingTimestamp, msg_data=NewMsgData}.

send_pre_update_message_successor(UpdateMsg, 
                                  #eh_system_state{pre_msg_data=PreMsgData, successor=Succ, ring_timestamp=RingTimestamp}=State) ->
  gen_server:cast({?EH_SYSTEM_SERVER, Succ}, {?EH_PRED_PRE_UPDATE, {UpdateMsg, RingTimestamp}}),
  NewPreMsgData = eh_system_util:add_pre_update_msg(UpdateMsg, PreMsgData),
  State#eh_system_state{pre_msg_data=NewPreMsgData, last_msg_succ={?EH_PRED_PRE_UPDATE, UpdateMsg}}.  

send_update_message_successor(UpdateMsg,
                              #eh_system_state{successor=Succ, ring_timestamp=RingTimestamp, app_config=AppConfig}=State) ->
  store_data(UpdateMsg, AppConfig),
  gen_server:cast({?EH_SYSTEM_SERVER, Succ}, {?EH_PRED_UPDATE, {UpdateMsg, RingTimestamp}}),
  NewState1 = eh_system_util:cleanup_msg(State),
  NewPreMsgData2 = eh_system_util:remove_pre_update_msg(UpdateMsg, NewState1#eh_system_state.pre_msg_data),
  NewMsgData2 = eh_system_util:add_update_msg(UpdateMsg, NewState1#eh_system_state.msg_data),
  NewState1#eh_system_state{msg_data=NewMsgData2, pre_msg_data=NewPreMsgData2, last_msg_succ={?EH_PRED_UPDATE, UpdateMsg}}.

send_message_successor(Succ, #eh_system_state{ring_timestamp=RingTimestamp}=State) ->
  {PreUpdate, Update} = eh_system_util:get_successor_message(Succ, State),
  send_message_successor(?EH_PRED_PRE_UPDATE, Succ, RingTimestamp, PreUpdate),
  send_message_successor(?EH_PRED_UPDATE, Succ, RingTimestamp, Update).

send_message_successor(Tag, Succ, RingTimestamp, List) ->
  lists:foreach(fun(UpdateMsg) -> gen_server:cast({?EH_SYSTEM_SERVER, Succ}, {Tag, {UpdateMsg, RingTimestamp}}) end, List).

process_last_msg_succ(DownNode, 
                      #eh_system_state{timestamp=Timestamp, last_msg_succ={Tag, #eh_update_msg{timestamp=Timestamp, node_id=DownNode}=UpdateMsg}}=State) ->
  case Tag of
    ?EH_PRED_PRE_UPDATE ->
      send_update_message_successor(UpdateMsg, State);
    ?EH_PRED_UPDATE     ->
      send_message_client(UpdateMsg, State)
  end; 
process_last_msg_succ(_, State) ->
  State.
update_state_timestamp(Tag, Timestamp, TrgRingTimestamp, 
                       #eh_system_state{ring_timestamp=SrcRingTimestamp, timestamp=SrcTimestamp, app_config=AppConfig}=State) ->
  NewSrcRingTimestamp = eh_node_timestamp:update_ring_timestamp(Tag, Timestamp, eh_system_config:get_node_id(AppConfig), SrcRingTimestamp, TrgRingTimestamp),
  State#eh_system_state{timestamp=max(SrcTimestamp, Timestamp), ring_timestamp=NewSrcRingTimestamp}.

valid_pre_update_message(#eh_update_msg{object_type=ObjectType, object_id=ObjectId, timestamp=Timestamp, client_id=ClientId, node_id=NodeId, reference=Ref}=UpdateMsg, 
                         #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig}=State) ->
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
              case NodeId =:= ENodeId of
                true  ->
                  {false, State};
                false ->
                  ConflictResolver = eh_system_config:get_write_conflict_resolver(AppConfig),
                  ResolvedNodeId = ConflictResolver:resolve(NodeId, ENodeId),
                  case ResolvedNodeId =:= NodeId of
                    true  ->
                      NewPreMsgData = eh_system_util:remove_pre_update_msg(EUpdateMsg, PreMsgData),
                      NewState = State#eh_system_state{pre_msg_data=NewPreMsgData},
                      eh_system_util:reply(EClientId, ERef, {ENodeId, ObjectType, ObjectId, ?EH_BEING_UPDATED}),
                      {true, NewState};
                    false ->
                      eh_system_util:reply(ClientId, Ref, {NodeId, ObjectType, ObjectId, ?EH_BEING_UPDATED}),
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
