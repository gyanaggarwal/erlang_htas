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

-module(eh_test_util).

-export([get_random/1,
         get_random_nth/1,
         get_update_param/1,
         get_node_change/1,
         valid_result/1,
         print_run_status/2]).

-include("erlang_htas_test.hrl").

get_random({Min, Min}) ->
  Min;
get_random({Min, Max}) ->
  random:uniform(Max-Min)+Min.

get_random_nth(List) ->
  N = get_random({0, length(List)}),
  lists:nth(N, List).

get_update_param(#eh_run_state{active_nodes=ActiveNodes}) ->
  {get_random_nth(ActiveNodes), get_random_nth(?OBJECT_TYPE), get_random_nth(?OBJECT_ID), [{get_random_nth(?COLUMNS), get_random_nth(?VALUES)}]}.

get_node_change(#eh_run_state{valid_result=false}=State) ->
  get_node_change(node_nochange, State);
get_node_change(#eh_run_state{test_runs=0, down_nodes=[]}=State) ->
  get_node_change(node_nochange, State);
get_node_change(#eh_run_state{test_runs=0}=State) ->
  get_node_change(node_up, State);
get_node_change(#eh_run_state{active_nodes=ActiveNodes}=State) when length(ActiveNodes) =:= length(?NODE_LIST) ->
  get_node_change(node_down, State);
get_node_change(#eh_run_state{active_nodes=ActiveNodes}=State) when length(ActiveNodes) =:= 1 ->
  get_node_change(node_up, State);
get_node_change(State) ->
  get_node_change(get_random_nth(?NODE_CHANGE), State).

get_node_change(node_nochange, _) ->
  {node_nochnage, node};
get_node_change(node_down, #eh_run_state{active_nodes=ActiveNodes}) ->
  {node_down, get_random_nth(ActiveNodes)};
get_node_change(node_up, #eh_run_state{down_nodes=DownNodes}) ->
  {node_up, get_random_nth(DownNodes)}.

valid_result([]) ->
  true;
valid_result([_H | []]) ->
  true;
valid_result([{_, R0} | Rest]) ->
  lists:all(fun({_, RX}) -> R0 =:= RX end, Rest). 

print(MsgTag, true, RunNum, ActiveNodes, DownNodes) ->
  io:fwrite("run_num=~p succeeded [~p] active_nodes=~p down_nodes=~p~n", [RunNum, MsgTag, ActiveNodes, DownNodes]);
print(MsgTag, false, RunNum, ActiveNodes, DownNodes) ->
  io:fwrite("run_num=~p failed [~p] active_node=~p down_nodes=~p~n", [RunNum, MsgTag, ActiveNodes, DownNodes]).  

print_run_status(MsgTag, #eh_run_state{valid_result=ValidResult, run_num=RunNum, active_nodes=ActiveNodes, down_nodes=DownNodes}) ->
  AN = eh_system_util:make_list_to_string(fun erlang:atom_to_list/1, ActiveNodes),
  DN = eh_system_util:make_list_to_string(fun erlang:atom_to_list/1, DownNodes),
  print(MsgTag, ValidResult, RunNum, AN, DN).