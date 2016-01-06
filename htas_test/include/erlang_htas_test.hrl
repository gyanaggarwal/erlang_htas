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

-define(TEST_RUNS,             {20, 40}).
-define(DATA_ENTRIES,          {10, 20}).

-define(NODE_LIST,             ['eh_n1@Gyanendras-MacBook-Pro', 
                                'eh_n2@Gyanendras-MacBook-Pro',
                                'eh_n5@Gyanendras-MacBook-Pro']).

-define(NODE_CHANGE,           [node_up, node_down]).

-define(OBJECT_TYPE,           [person, address, employee]).

-define(OBJECT_ID,             lists:seq(1, 10)).

-define(COLUMNS,               [first_name, last_name, address, gender, education, street, city, state, zip, phone]).
 
-define(VALUES,                lists:seq(1, 100)).

-record(eh_run_state,         {active_nodes             :: list(),
                               down_nodes=[]            :: list(),
                               test_runs=0              :: non_neg_integer(),
                               run_num=0                :: non_neg_integer(),
                               curr_result              :: term(),
                               valid_result=true        :: true | false}).