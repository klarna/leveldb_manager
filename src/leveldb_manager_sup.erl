%%%----------------------------------------------------------------
%%% File        : leveldb_manager_sup.erl
%%% Author      : Mikael Pettersson <mikael.pettersson@klarna.com>
%%% Description : Supervisor for leveldb_manager instances
%%%
%%% Copyright (c) 2014-2015 Klarna AB
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%----------------------------------------------------------------

-module(leveldb_manager_sup).
-behaviour(supervisor).

%% public entry points
-export([ start_link/0
        , start_manager/3]).

%% private entry points (supervisor callbacks)
-export([init/1]).

%%%----------------------------------------------------------------

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, {{simple_one_for_one, 10, 10},
        [{ undefined
         , {leveldb_manager, start_link, []}
         , transient
         , 5000
         , worker
         , [leveldb_manager]}]}}.

start_manager(Name, Path, Options) ->
  %% Clients call leveldb_manager:open/3 to create leveldb instances.
  %% That in turn calls this function, which uses the supervisor framework
  %% to call leveldb_manager:start_link/4 (see init/1 above) to create
  %% a new gen_server and link that to this supervisor.
  {ok, _Pid} = supervisor:start_child(?MODULE, [Name, Path, Options]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
