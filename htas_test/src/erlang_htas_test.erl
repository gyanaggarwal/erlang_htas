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

-module(erlang_htas_test).

-export([run/0, 
         setup_ring/1,
         data_entries/1,
         node_change/1,
         validate/1]).

-include("erlang_htas_test.hrl").

run() ->
  random:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
  State = #eh_run_state{active_nodes=?NODE_LIST, test_runs=eh_test_util:get_random(?TEST_RUNS)},
  erlang_htas:setup_ring(?NODE_LIST),
  run_test(State).

run_test(#eh_run_state{valid_result=false, curr_result=CurrResult}) ->
  CurrResult;
run_test(#eh_run_state{test_runs=0, down_nodes=[]}) ->
  ok;
run_test(#eh_run_state{test_runs=TestRuns,
                       run_num=RunNum}=State) ->
  DataEntries = eh_test_util:get_random(?DATA_ENTRIES),
  update(DataEntries, State),
  State1 = State#eh_run_state{test_runs=max(0, TestRuns-1),
                              run_num=RunNum+1},
  State2 = validate_result(data_entries, State1),
  {NodeChange, Node} = eh_test_util:get_node_change(State2),
  State3 = make_node_change(NodeChange, Node, State2),
  timer:sleep(2000),
  State4 = validate_result(NodeChange, State3),
  run_test(State4).
  
update(0, _State) ->
  ok;
update(DataEntries, State) ->
  timer:sleep(?ENTRY_SLEEP_TIME),
  {Node, ObjectType, ObjectId, Columns} = eh_test_util:get_update_param(State),
  erlang_htas:update(Node, ObjectType, ObjectId, Columns, []),
  update(DataEntries-1, State).

make_node_change(node_down, Node, #eh_run_state{active_nodes=ActiveNodes, down_nodes=DownNodes}=State) ->
  erlang_htas:stop(Node, normal),
  State#eh_run_state{active_nodes=lists:delete(Node, ActiveNodes), down_nodes=[Node | DownNodes]};
make_node_change(node_up, Node, #eh_run_state{active_nodes=ActiveNodes, down_nodes=DownNodes}=State) ->
  ActiveNodes1 = [Node | ActiveNodes],
  erlang_htas:add_node(Node, ActiveNodes1),
  State#eh_run_state{active_nodes=ActiveNodes1, down_nodes=lists:delete(Node, DownNodes)};
make_node_change(_, _, State) ->
  State.

validate_result(Tag, #eh_run_state{active_nodes=ActiveNodes}=State) ->
  Result = erlang_htas:data_view(ActiveNodes),
  State1 = State#eh_run_state{curr_result=Result, valid_result=eh_test_util:valid_result(Result)},
  eh_test_util:print_run_status(Tag, State1),
  State1.

setup_ring(NodeList) ->
  erlang_htas:setup_ring(NodeList).

data_entries(NodeList) ->
  random:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
  State = #eh_run_state{active_nodes=NodeList},
  DataEntries = eh_test_util:get_random(?BULK_DATA_ENTRIES),
  update(DataEntries, State).

node_change(NodeList) ->
  random:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
  State = #eh_run_state{active_nodes=NodeList, test_runs=eh_test_util:get_random(?TEST_RUNS)},
  node_change(undefined, State).

node_change(node_nochange, State) ->
  DataEntries = eh_test_util:get_random(?DATA_ENTRIES),
  update(DataEntries, State);
node_change(_, #eh_run_state{test_runs=TestRuns}=State) ->
  timer:sleep(?NODE_SLEEP_TIME),
  {NodeChange, Node} = eh_test_util:get_node_change(State),
  State1 = State#eh_run_state{test_runs=max(0, TestRuns-1)},
  State2 = make_node_change(NodeChange, Node, State1),
  node_change(NodeChange, State2).

validate(NodeList) ->
  Result = erlang_htas:data_view(NodeList),
  case eh_test_util:valid_result(Result) of
    true  ->
      valid;
    false ->
      Result
  end.