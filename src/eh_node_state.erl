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

-module(eh_node_state).

-export([pre_update_msg/1,
         update_snapshot/1,
         state/1,
         data_state/1,
         client_state/1,
         display_state/1]).

-include("erlang_htas.hrl").

pre_update_msg(#eh_node_status{update_snapshot=false}=NodeState) ->
  NodeState#eh_node_status{pre_update_msg=true};
pre_update_msg(#eh_node_status{update_snapshot=true}=NodeState) ->
  NodeState#eh_node_status{pre_update_msg=true, state=?EH_STATE_NORMAL}.

update_snapshot(#eh_node_status{pre_update_msg=false}=NodeState) ->
  NodeState#eh_node_status{update_snapshot=true};
update_snapshot(#eh_node_status{pre_update_msg=true}=NodeState) ->
  NodeState#eh_node_status{update_snapshot=true, state=?EH_STATE_NORMAL}.

state(NodeState) ->
  NodeState#eh_node_status{state=?EH_STATE_NORMAL}.

data_state(#eh_node_status{state=?EH_STATE_NORMAL}) ->
  ?EH_STATE_NORMAL;
data_state(#eh_node_status{update_snapshot=true}) ->
  ?EH_STATE_NORMAL;
data_state(_) ->
  ?EH_STATE_TRANSIENT.

client_state(#eh_node_status{state=State}) ->
  State.

display_state(#eh_node_status{state=?EH_STATE_TRANSIENT, update_snapshot=true}) ->
  ?TRANSIENT_DU;
display_state(#eh_node_status{state=?EH_STATE_TRANSIENT, pre_update_msg=true}) ->
  ?TRANSIENT_TU;
display_state(#eh_node_status{state=?EH_STATE_TRANSIENT}) ->
  ?TRANSIENT;
display_state(_) ->
  ?READY.

