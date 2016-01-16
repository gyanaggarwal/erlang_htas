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

-module(erlang_htas).

-export([start/0, 
         stop/0,
         stop/2,
         setup_ring/1,
         add_node/2,
         query/3,
         delete/3,
         delete/4,
         update/4,
         update/5,
         multi_update/2,
         data_view/1,
         validate/1]).

-include("erlang_htas.hrl").

start() ->
  application:start(erlang_htas).

stop() ->
  application:stop(erlang_htas).

data_view(NodeList) ->
  {Replies, _} = gen_server:multi_call(NodeList, ?EH_SYSTEM_SERVER, ?EH_DATA_VIEW),
  Replies.

validate(NodeList) ->
  Result = data_view(NodeList),
  case eh_system_util:valid_result(Result) of
    true  ->
      valid;
    false ->
      Result
  end.

setup_ring(NodeList) ->
  gen_server:abcast(NodeList, ?EH_SYSTEM_SERVER, ?EH_SETUP_RING).

add_node(Node, NodeList) ->
  gen_server:abcast(NodeList, ?EH_SYSTEM_SERVER, {?EH_ADD_NODE, {Node, NodeList}}).

stop(Node, Reason) ->
  gen_server:cast({?EH_SYSTEM_SERVER, Node}, {stop, Reason}).

query(Node, ObjectType, ObjectId) ->
  send([Node], ?EH_QUERY, {ObjectType, ObjectId}, ?READ_TIMEOUT).

delete(Node, ObjectType, ObjectId) ->
  send_update(Node, ObjectType, ObjectId, ?STATUS_INACTIVE).

delete(Node, ObjectType, ObjectId, DeleteColumns) ->
  update(Node, ObjectType, ObjectId, [], DeleteColumns).

update(Node, ObjectType, ObjectId, UpdateColumns) ->
  update(Node, ObjectType, ObjectId, UpdateColumns, []).

update(Node, ObjectType, ObjectId, UpdateColumns, DeleteColumns) ->
  Columns = combine_columns(UpdateColumns, DeleteColumns),
  send_update(Node, ObjectType, ObjectId, Columns).

send_update(Node, ObjectType, ObjectId, Columns) ->
  send([Node], ?EH_UPDATE, [{Node, ObjectType, ObjectId, Columns}], ?UPDATE_TIMEOUT).
  
multi_update(NodeList, ObjectList) ->
  {NodeList1, ObjectList1} = multi_combine_columns(NodeList, ObjectList, [], []),
  send(NodeList1, ?EH_UPDATE, ObjectList1, ?UPDATE_TIMEOUT).

send(NodeList, MsgTag, Msg, Timeout) ->
  flush_msg(),
  AppConfig = eh_system_config:get_env(),
  UniqueIdGenerator = eh_system_config:get_unique_id_generator(AppConfig),
  Ref = UniqueIdGenerator:unique_id(),
  gen_server:abcast(NodeList, ?EH_SYSTEM_SERVER, {MsgTag, {self(), Ref, Msg}}),
  receive_msg(Ref, Timeout, NodeList, []).

receive_msg(Ref, Timeout, [Node | RNodeList], Acc) ->
  Reply1 = receive
             {reply, Ref, Reply} ->
               Reply
           after Timeout ->
             eh_query_handler:error_node_down(Node)
           end,
  receive_msg(Ref, Timeout, RNodeList, [Reply1 | Acc]);
receive_msg(_Ref, _Timeout, [], Acc) ->
  Acc.

flush_msg() ->
  receive
    _Msg ->
      ok
    after 0 ->
      ok 
  end.

combine_columns(UpdateColumns, DeleteColumns) ->
  get_columns(lists:reverse(UpdateColumns), get_columns(lists:reverse(DeleteColumns), [])).

get_columns([{Column, Value} | T], Acc) ->
  get_columns(T, [{Column, ?STATUS_ACTIVE, Value} | Acc]);
get_columns([Column | T], Acc) ->
  get_columns(T, [{Column, ?STATUS_INACTIVE} | Acc]);
get_columns([], Acc) ->
  Acc.

multi_combine_columns([N | RNode], [{ObjectType, ObjectId, UpdateColumns} | RObject], NodeList, ObjectList) ->
  Columns = combine_columns(UpdateColumns, []),
  multi_combine_columns(RNode, RObject, [N | NodeList], [{N, ObjectType, ObjectId, Columns} | ObjectList]);
multi_combine_columns([N | RNode], [{ObjectType, ObjectId, UpdateColumns, DeleteColumns} | RObject], NodeList, ObjectList) ->
  Columns = combine_columns(UpdateColumns, DeleteColumns),
  multi_combine_columns(RNode, RObject, [N | NodeList], [{N, ObjectType, ObjectId, Columns} | ObjectList]);
multi_combine_columns([], _, NodeList, ObjectList) ->
  {NodeList, ObjectList};
multi_combine_columns(_, [], NodeList, ObjectList) ->
  {NodeList, ObjectList}.