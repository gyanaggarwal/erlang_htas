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

-module(eh_node_timestamp).

-export([update_ring_timestamp/5]).

-include("erlang_htas.hrl").

update_ring_timestamp(Tag, Timestamp, NodeId, SrcRingTimestamp, TrgRingTimestamp) ->
  NodeTimestamp = eh_system_util:get_node_timestamp(NodeId, SrcRingTimestamp),
  maps:put(NodeId, update_node_timestamp(Tag, Timestamp, NodeTimestamp), TrgRingTimestamp).

update_node_timestamp(?EH_UPDATE_INITIATED_MSG, Timestamp, #eh_node_timestamp{update_initiated=ETimestamp}=NodeTimestamp) ->
  NodeTimestamp#eh_node_timestamp{update_initiated=max(Timestamp, ETimestamp)};
update_node_timestamp(?EH_UPDATE_COMPLETED_MSG, Timestamp, #eh_node_timestamp{update_completed=ETimestamp}=NodeTimestamp) ->
  NodeTimestamp#eh_node_timestamp{update_completed=max(Timestamp, ETimestamp)};
update_node_timestamp(?EH_PRED_PRE_UPDATE_MSG, Timestamp, #eh_node_timestamp{pred_pre_update=ETimestamp}=NodeTimestamp) ->
  NodeTimestamp#eh_node_timestamp{pred_pre_update=max(Timestamp, ETimestamp)};
update_node_timestamp(?EH_PRED_UPDATE_MSG, Timestamp, #eh_node_timestamp{pred_update=ETimestamp}=NodeTimestamp) ->
  NodeTimestamp#eh_node_timestamp{pred_update=max(Timestamp, ETimestamp)}.

