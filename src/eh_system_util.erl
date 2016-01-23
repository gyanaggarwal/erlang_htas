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

-module(eh_system_util).

-export([get_node_name/1,
         get_node_atom/1,
         get_file_name/3,
         make_list_to_string/2,
         get_update_msg/7,
         new_set/0,
         size_set/1,
         add_set/2,
         remove_set/2,
         merge_set/2,
         subtract_set/2,
         fold_set/3,
         get_set_timestamp/1,
         is_key_set/2,
         new_map/0,
         size_map/1,
         add_map/3,
         remove_map/2,
         find_map/2,
         fold_map/3,
         get_map_timestamp/1,
         is_key_map/2,
         exist_map_msg/3,
         valid_result/1, 
         extract_nodes/2,
         display_atom_to_list/1]).

-include("erlang_htas.hrl").

-spec get_node_name(Node :: atom()) -> string().
get_node_name(Node) ->
  lists:takewhile(fun(X) -> X =/= $@ end, atom_to_list(Node)).

-spec get_node_atom(Node :: atom()) -> atom().
get_node_atom(Node) ->
  list_to_atom(get_node_name(Node)).

-spec get_file_name(NodeName :: string(), DataDir :: string(), FileName :: string() | atom()) -> string() | atom().
get_file_name(_, _, standard_io) ->
  standard_io;
get_file_name(NodeName, DataDir, FileName) ->
  DataDir ++ NodeName ++ FileName.

make_list_to_string(Fun, List) ->
  lists:foldl(fun(N, Acc) -> case length(Acc) of
                               0 -> Acc ++ Fun(N);
                               _ -> Acc ++ "," ++ Fun(N)
                             end end, [], List).

get_update_msg(ObjectType, ObjectId, UpdateData, Timestamp, From, NodeId, Ref) ->
  {#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId},
   #eh_update_msg_data{update_data=UpdateData, client_id=From, node_id=NodeId, reference=Ref}}.

new_set() ->
  sets:new().

size_set(Set) ->
  sets:size(Set).

add_set(Key, Set) ->
  sets:add_element(Key, Set).

remove_set(Key, Set) ->
  sets:del_element(Key, Set).

merge_set(Set1, Set2) ->
  sets:union(Set1, Set2).

subtract_set(Set1, Set2) ->
  sets:subtract(Set1, Set2).

is_key_set(Key, Set) ->
  sets:is_element(Key, Set).

fold_set(Fun, Acc, Set) ->
  sets:fold(Fun, Acc, Set).

get_set_timestamp(Set) ->
  sets:fold(fun(#eh_update_msg_key{timestamp=Timestamp}, Acc) -> [Timestamp | Acc] end, [], Set).

new_map() ->
  maps:new().

size_map(Map) ->
  maps:size(Map).

add_map(Key, Value, Map) ->
  maps:put(Key, Value, Map).

remove_map(Key, Map) ->
  maps:remove(Key, Map).

find_map(Key, Map) ->
  maps:find(Key, Map).

is_key_map(Key, Map) ->
  maps:is_key(Key, Map).

fold_map(Fun, Acc, Map) ->
  maps:fold(Fun, Acc, Map).

get_map_timestamp(Map) ->
  maps:fold(fun(#eh_update_msg_key{timestamp=Timestamp}, _, Acc) -> [Timestamp | Acc] end, [], Map).

exist_map_msg(ObjectType, ObjectId, Map) ->
  maps:fold(fun(#eh_update_msg_key{object_type=XOT, object_id=XOI}, _, Acc) -> Acc orelse (ObjectType =:= XOT andalso ObjectId =:= XOI) end, false, Map).
 
valid_result([]) ->
  true;
valid_result([_H | []]) ->
  true;
valid_result([{_, R0} | Rest]) ->
  lists:all(fun({_, RX}) -> R0 =:= RX end, Rest).

extract_nodes([{Node, _} | Rest], Acc) ->
  extract_nodes(Rest, [Node | Acc]);
extract_nodes([], Acc) ->
  Acc.

display_atom_to_list(Atom) ->
  ListAtom = atom_to_list(Atom),
  lists:sublist(ListAtom, 4, length(ListAtom)). 










