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

-export([add_handler/1, delete_handler/0]).

-export([init/1, handle_call/2, handle_info/2, handle_event/2, terminate/2, code_change/3]).

-include("erlang_htas.hrl").

add_handler(AppConfig) ->
  eh_event:add_handler(?MODULE, AppConfig).

delete_handler() ->
  eh_event:delete_handler(?MODULE, []).

init(AppConfig) ->
  File1 = case eh_system_config:get_file_repl_log(AppConfig) of
            standard_io ->
              standard_io;
            FileName ->
              {ok, File} = open(FileName),
              File
          end, 
  {ok, File1}.

terminate(_Args, standard_io) ->
  ok;
terminate(_Args, File) ->
  close(File).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

handle_call(_Request, State) ->
  {ok, ok, State}.

handle_info(_Info, State) ->
  {ok, State}.

handle_event({data_state, {Module, Msg, #eh_data_state{timestamp=Timestamp, data_index_list=DIList}}}, File) ->
  io:fwrite(File, "[~p] ~p timestamp=~p, data_index_list=~p~n", [Module, Msg, Timestamp, DIList]),
  {ok, File};

handle_event({state, {Module, Msg, StateData=#eh_system_state{}}}, File) ->
  NodeId = eh_system_util:get_node_name(StateData#eh_system_state.app_config#eh_app_config.node_id),
  Successor = eh_system_util:get_node_name(StateData#eh_system_state.successor),
  Timestamp = StateData#eh_system_state.timestamp,
  NodeState = eh_node_state:display_state(StateData),
  ReplRing = eh_system_util:make_list_to_string(fun eh_system_util:get_node_name/1, StateData#eh_system_state.repl_ring),
  PreMsgData = list_msg_map(StateData#eh_system_state.pre_msg_data),
  MsgData = list_msg_map(StateData#eh_system_state.msg_data),
  RCMap = list_completed_map(StateData#eh_system_state.ring_completed_map),
  io:fwrite(File, "[~p] ~p node_status=~p, node_id=~p, repl_ring=~p, successor=~p, timestamp=~p, pre_msg_data=~p, msg_data=~p, ring_completed_map=~p~n~n",
            [Module, Msg, NodeState, NodeId, ReplRing, Successor, Timestamp, PreMsgData, MsgData, RCMap]),
  {ok, File};

handle_event({message, {Module, Msg, {UMsgKey, #eh_update_msg_data{node_id=NodeId}, CompletedSet}}}, File) ->
  RCSet = list_msg_set_value(CompletedSet),
  io:fwrite(File, "[~p] ~p message=~p, completed_set=~p~n",
             [Module, Msg, list_msg(UMsgKey, NodeId), RCSet]),
  {ok, File};

handle_event({message, {Module, Msg, UMsgKey}}, File) ->
  io:fwrite(File, "[~p] ~p message=~p~n",
             [Module, Msg, list_msg_key(UMsgKey)]),
  {ok, File};

handle_event({data, {Module, Msg, DataMsg, Data}}, File) ->
  io:fwrite(File, "[~p] ~p ~p=~p~n", [Module, Msg, DataMsg, Data]),
  {ok, File}.

is_listable_value(Value) ->
  is_atom(Value) orelse is_integer(Value) orelse is_float(Value) orelse is_list(Value).

is_listable_msg_key(#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId}) ->
  is_listable_value(Timestamp) andalso is_listable_value(ObjectType) andalso is_listable_value(ObjectId).

list_value(Value) when is_list(Value) ->
  Value;
list_value(Value) when is_atom(Value) ->
  atom_to_list(Value);
list_value(Value) when is_integer(Value) ->
  integer_to_list(Value);
list_value(Value) when is_float(Value) ->
  float_to_list(Value).

list_msg_key(#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId}=UMsgKey) ->
  case is_listable_msg_key(UMsgKey) of
     true  ->
       "{"++list_value(Timestamp)++","++list_value(ObjectType)++","++list_value(ObjectId)++"}";
     false ->
       list_value(Timestamp)
  end.

list_node_msg(NodeId, MsgValue) ->
  eh_system_util:get_node_name(NodeId)++"=>"++MsgValue.

list_msg(UMsgKey, MsgNodeId) ->
  list_node_msg(MsgNodeId, list_msg_key(UMsgKey)).

list_msg_set(Set, MsgNodeId) ->
  list_node_msg(MsgNodeId, list_msg_set_value(Set)).

add_list(List, Acc) when length(Acc) =:= 0 ->
  Acc++List;
add_list(List, Acc) ->
  Acc++","++List.

list_msg_set_value(Set) ->
  eh_system_util:fold_set(fun(MsgKey, Acc) -> add_list(list_msg_key(MsgKey), Acc) end, [], Set).

list_msg_map(Map) ->
  eh_system_util:fold_map(fun(MsgKey, #eh_update_msg_data{node_id=NodeId}, Acc) -> add_list(list_msg(MsgKey, NodeId), Acc) end, [], Map).

list_completed_map(Map) ->
  eh_system_util:fold_map(fun(NodeId, Set, Acc) -> add_list(list_msg_set(Set, NodeId), Acc) end, [], Map).

open(FileName) ->
  file:open(FileName, [write]).

close(File) ->
  file:close(File).







  

