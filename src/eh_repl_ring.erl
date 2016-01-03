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

-module(eh_repl_ring).

-export([drop/2,
         add/2,
         predecessor/2,
         successor/2,
         originating_node_id/2]).

drop(Node, NodeList) ->
  lists:sort(lists:delete(Node, NodeList)).

add(Node, NodeList) ->
  lists:sort([Node | lists:delete(Node, NodeList)]).

predecessor(Node, NodeList) ->
  NewNodeList = new_node_list(Node, NodeList),
  {L1, L2} = lists:splitwith(fun(N) -> N < Node end, NewNodeList),
  case {length(L1), length(L2)} of
    {0, 0} -> 
      undefined;
    {0, 1} ->
      undefined;
    {0, _} ->
      lists:last(L2);
    {_, _} ->
      lists:last(L1)
  end.

successor(Node, NodeList) ->
  NewNodeList = new_node_list(Node, NodeList),
  {L1, L2} = lists:splitwith(fun(N) -> N =< Node end, NewNodeList),
  case {length(L1), length(L2)} of
    {0, 0} ->
      undefined;
    {1, 0} ->
      undefined;
    {_, 0} ->
      hd(L1);
    {_, _} ->
      hd(L2)
  end.

originating_node_id(Node, NodeList) ->
  case lists:member(Node, NodeList) of
    true  ->
      Node;
    false ->
      predecessor(Node, NodeList)
  end.

new_node_list(Node, NodeList) ->
  case lists:member(Node, NodeList) of
    true  ->
      NodeList;
    false ->
      add(Node, NodeList)
  end.
