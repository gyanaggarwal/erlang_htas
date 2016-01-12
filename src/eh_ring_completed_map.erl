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

-module(eh_ring_completed_map).

-export([get_ring_completed_set/1,
         get_completed_set/2,
         add_completed_set/3,
         add_msg_key/3,
         subtract_replace_completed_set/3,
         merge_completed_set/3]).

-include("erlang_htas.hrl").

get_ring_completed_set(RingCompletedMap) ->
  maps:fold(fun(_K, V, S) -> eh_system_util:merge_set(V, S) end, sets:new(), RingCompletedMap).

get_completed_set(NodeId, RingCompletedMap) ->
  case maps:find(NodeId, RingCompletedMap) of
    error              ->
      sets:new();
    {ok, CompletedSet} ->
      CompletedSet
  end.

add_completed_set(NodeId, CompletedSet, RingCompletedMap) ->
  eh_system_util:add_map(NodeId, CompletedSet, RingCompletedMap).

add_msg_key(NodeId, MsgKey, RingCompletedMap) ->
  CompletedSet = eh_system_util:add_set(MsgKey, get_completed_set(NodeId, RingCompletedMap)),
  add_completed_set(NodeId, CompletedSet, RingCompletedMap).

subtract_replace_completed_set(NodeId, CompletedSet, RingCompletedMap) ->
  CompletedSet1 = eh_system_util:subtract_set(get_completed_set(NodeId, RingCompletedMap), CompletedSet),
  add_completed_set(NodeId, CompletedSet1, RingCompletedMap).

merge_completed_set(NodeId, DownNodeId, RingCompletedMap) ->
  CompletedSet = eh_system_util:merge_set(get_completed_set(NodeId, RingCompletedMap), get_completed_set(DownNodeId, RingCompletedMap)),
  add_completed_set(NodeId, CompletedSet, RingCompletedMap).
