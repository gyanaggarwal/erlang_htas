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

-module(eh_event_handler).

-behavior(gen_event).

-export([add_handler/0, delete_handler/0]).

-export([init/1, handle_call/2, handle_info/2, handle_event/2, terminate/2, code_change/3]).

-include("erlang_htas.hrl").

add_handler() ->
  eh_event:add_handler(?MODULE, []).

delete_handler() ->
  eh_event:delete_handler(?MODULE, []).

init(State) ->
  {ok, State}.

terminate(_Args, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

handle_call(_Request, State) ->
  {ok, ok, State}.

handle_info(_Info, State) ->
  {ok, State}.

handle_event({state, {Module, Msg, StateData=#eh_system_state{}}}, State) ->
  NodeId = eh_system_util:get_node_name(StateData#eh_system_state.app_config#eh_app_config.node_id),
  Successor = eh_system_util:get_node_name(StateData#eh_system_state.successor),
  Timestamp = StateData#eh_system_state.timestamp,
  NodeState = eh_node_state:client_state(StateData#eh_system_state.node_state),
  ReplRing = eh_system_util:make_list_to_string(fun eh_system_util:get_node_name/1, StateData#eh_system_state.repl_ring),
  PreMsgData = eh_system_util:make_list_to_string(fun erlang:integer_to_list/1, eh_system_util:get_map_timestamp(StateData#eh_system_state.pre_msg_data)),
  MsgData = eh_system_util:make_list_to_string(fun erlang:integer_to_list/1, eh_system_util:get_map_timestamp(StateData#eh_system_state.msg_data)),
  RCMap = get_ring_completed_map(StateData#eh_system_state.ring_completed_map),
  io:fwrite("[~p] ~p node_state=~p, node_id=~p, repl_ring=~p, successor=~p, timestamp=~p, pre_msg_data=~p, msg_data=~p, ring_completed_map=~p~n~n",
            [Module, Msg, NodeState, NodeId, ReplRing, Successor, Timestamp, PreMsgData, MsgData, RCMap]),
  {ok, State};

handle_event({message, {Module, Msg, {#eh_update_msg_key{object_type=ObjectType, object_id=ObjectId, timestamp=Timestamp}, #eh_update_msg_data{node_id=NodeId}, CompletedSet}}}, State) ->
 RCSet = eh_system_util:make_list_to_string(fun erlang:integer_to_list/1, eh_system_util:get_set_timestamp(CompletedSet)),
 io:fwrite("[~p], ~p node_id=~p, timestamp=~p, object_type=~p, object_id=~p, completed_set=~p~n",
            [Module, Msg, NodeId, Timestamp, ObjectType, ObjectId, RCSet]),
  {ok, State};

handle_event({data, {Module, Msg, DataMsg, Data}}, State) ->
  io:fwrite("[~p] ~p ~p=~p~n", [Module, Msg, DataMsg, Data]),
  {ok, State}.

get_ring_completed_map(RingCompletedMap) ->
  maps:fold(fun(K, V, Acc) -> ring_completed_set(completed_set(K, V), Acc) end, [], RingCompletedMap).

completed_set(NodeId, CompletedSet) ->
  case sets:size(CompletedSet) of
    0 ->
      "";
    _ ->
      eh_system_util:get_node_name(NodeId) ++ "=>" ++ eh_system_util:make_list_to_string(fun erlang:integer_to_list/1, eh_system_util:get_set_timestamp(CompletedSet))
  end.

ring_completed_set(Data, Acc) when length(Data) =:= 0 ->
  Acc;
ring_completed_set(Data, Acc) when length(Acc) =:= 0 ->
  Data;
ring_completed_set(Data, Acc) ->
  Acc ++ "," ++ Data.





  

