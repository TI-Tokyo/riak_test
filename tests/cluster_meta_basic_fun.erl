%% -------------------------------------------------------------------
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
%%
%% -------------------------------------------------------------------
-module(cluster_meta_basic_fun).

-export([list_resolver/2]).

list_resolver(X1, X2) when is_list(X2), is_list(X1) ->
    lists:usort(X1 ++ X2);
list_resolver(X1, X2) when is_list(X2) ->
    lists:usort([X1 | X2]);
list_resolver(X1, X2) when is_list(X1) ->
    lists:usort(X1 ++ [X2]);
list_resolver(X1, X2) ->
    lists:usort([X1, X2]).