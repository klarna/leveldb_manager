%%%----------------------------------------------------------------
%%% File        : leveldb_manager.erl
%%% Author      : Mikael Pettersson <mikael.pettersson@klarna.com>
%%% Description : Allows leveldb instances to be temporary offlined
%%%
%%% Copyright (c) 2014-2020 Klarna Bank AB
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
%%% Background:
%%%
%%% - We MUST close the handle to a leveldb instance during shapshots,
%%% otherwise the snapshot may contain inconsistent data.
%%%
%%% - Taking a snapshot is fast, so we want to block leveldb accesses
%%% during the snapshot, not fail them.
%%%
%%% The leveldb manager wraps leveldb accesses and:
%%%
%%% - keeps track of which processes have in-progress accesses,
%%% - allows the leveldb instance to be taken offline when idle,
%%% - blocks new accesses when the instance is offline, and
%%% - resumes blocked accesses when the instance becomes online.
%%%
%%% Implementation notes:
%%%
%%% The manager implements a standard multiple-readers single-writer lock,
%%% identifying "reader" with "any leveldb access" and "writer" with
%%% "offlining the leveldb".
%%%
%%% The manager is implemented as a gen_server, primarily because that
%%% provides the needed synchronisation for its own state updates.
%%% (Traditional implementations use atomic arithmetic on integers,
%%% atomic compare-and-swap on integers, and OS primitives for blocking
%%% and waking threads.  Using a gen_server is much easier, though the
%%% inter-process communication causes higher latencies.)
%%%
%%% Blocking is done by recording the blocked process and returning
%%% {noreply, ...} to the gen_server glue.  gen_server:reply/2 unblocks.
%%%
%%% Leveldb API wrappers ensure that each leveldb op goes through a
%%% proper "lock", "op", "unlock" sequence.
%%%
%%% A process is monitored while holding an exclusive lock.  If it
%%% terminates before unlocking, the {'DOWN', ...} message is used to
%%% force-unlock that process' lock.  A process holding a shared lock
%%% is not monitored; instead, a processs wanting an exclusive lock
%%% will check the Pids of shared lock holders, and reap the dead ones.
%%%
%%% A shared lock is taken entirely outside of the manager's gen_server,
%%% as long as the lock is not contended.  The fast path adds the
%%% requesting process' Key (Pid or {iterator, Pid}) to the manager's ETS
%%% table, and then reads the Handle and Writer from it.  If the Writer
%%% is absent, the lock is granted.  Otherwise the slow path is invoked
%%% via a call to the gen_server.  The slow path removes the Key and
%%% delays replying until the exclusive lock is no longer held, at which
%%% point the process tries again.
%%%
%%% Releasing a held shared lock is always done outside of the gen_server,
%%% by erasing the lock holder's Key from the ETS table.
%%%
%%% This is a variant of Dekker's algorithm for mutual exclusion.  The
%%% main differences are:
%%%
%%% 1. Instead of implementing mutual exclusion between two processes, it
%%%    implements mutual exclusion between one writer and a group of readers.
%%%    Each reader has its own "intent" variable (its Pid's presence in the
%%%    ETS table), and the writer derives the combined intent of all readers
%%%    from the size of the ETS table.
%%%
%%% 2. There is no "turn" variable, instead the writer has precendence, and
%%%    new readers back off in case of contention.  This does not affect
%%%    safety, only liveness, and liveness follows from the fact that writers
%%%    are extremely infrequent.
%%%
%%% 3. Blocked readers do not spin-loop, instead they back off and wait on
%%%    a condition variable.  When the writer releases the lock it signals
%%%    that condition variable, allowing the readers to try again.
%%%----------------------------------------------------------------

-module(leveldb_manager).

-behaviour(gen_server).

%% leveldb wrappers ensuring correct lock/op/unlock sequences
-export([ open/2
        , get/3
        , put/4
        , delete/3
        , write/3
        , iterator/2
        , iterator/3
        , iterator_move/2
        , iterator_close/1
        , destroy/2]).

%% public entry points
-export([ start/2
        , start/3
        , suspend_before_snapshot/1
        , resume_after_snapshot/1
        , get_all/0
        , get_path/1
        , set_path/2
        , close/1]).

%% private entry point (supervisor callback)
-export([ start_link/3]).

%% private entry points (gen_server callbacks)
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3]).

%%%----------------------------------------------------------------

open(Path, Options) ->
  Name = list_to_atom(filename:basename(Path)),
  start(Name, Path, Options).

get(Mgr, Key, Opts) ->
  Val = eleveldb:get(get_handle(Mgr), Key, Opts),
  put_handle(Mgr),
  Val.

put(Mgr, Key, Data, Opts) ->
  Val = eleveldb:put(get_handle(Mgr), Key, Data, Opts),
  put_handle(Mgr),
  Val.

delete(Mgr, Key, Opts) ->
  Val = eleveldb:delete(get_handle(Mgr), Key, Opts),
  put_handle(Mgr),
  Val.

write(Mgr, Updates, Opts) ->
  Val = eleveldb:write(get_handle(Mgr), Updates, Opts),
  put_handle(Mgr),
  Val.

iterator(Mgr, Opts) ->
  %% the put_iterator/1 is in iterator_close/1
  {ok, IRef} = eleveldb:iterator(get_iterator(Mgr), Opts),
  {ok, {Mgr, IRef}}.

iterator(Mgr, Opts, KO) ->
  %% the put_iterator/1 is in iterator_close/1
  {ok, IRef} = eleveldb:iterator(get_iterator(Mgr), Opts, KO),
  {ok, {Mgr, IRef}}.

iterator_move({_Mgr, IRef}, Loc) ->
  eleveldb:iterator_move(IRef, Loc).

iterator_close({Mgr, IRef}) ->
  Val = eleveldb:iterator_close(IRef),
  put_iterator(Mgr),
  Val.

destroy(Path, Opts) ->
  %% this wrapper is only needed to cover the entire eleveldb API used
  %% from mnesia_eleveldb; there's no need to modify eleveldb semantics
  %% for destroy
  eleveldb:destroy(Path, Opts).

%%%----------------------------------------------------------------

start(Name, Path) ->
  start(Name, Path, default_options()).

start(Name, Path, Options) ->
  {ok, _Pid} = leveldb_manager_sup:start_manager(Name, Path, Options),
  {ok, Name}.

default_options() ->
  [ {create_if_missing, true}
  , {use_bloomfilter, true}
  ].

start_link(Name, Path, Options) ->
  %% Note: this function executes in the context of the supervisor,
  %% so if state/1 creates a new table it's owned by the supervisor.
  StateIsOld = state(Name),
  gen_server:start_link({local, Name}, ?MODULE,
                        [StateIsOld, Name, Path, Options], []).

%% Pids with read locks are recorded as {Key} tuples in the ETS table.
%% For single-operation accesses (get/put/etc but not iterators), Key = Pid.
%% For iterators, Key = {iterator, Pid}.

-define(iterator(Pid), {iterator, Pid}).

get_handle(Mgr) ->
  get_handle(Mgr, self(), read_lock).

get_handle(Mgr, Key, SlowPathReq) ->
  %% try fast-path
  Ets = Mgr,
  ets:insert(Ets, {Key}),
  %% order matters here: reading the Writer must be last
  Handle = ets:lookup_element(Ets, handle, 2),
  case ets:lookup_element(Ets, writer, 2) of
    [] -> % no active or pending writer: got it
      Handle;
    _ -> % an active or pending writer: fall back to slow-path
      case robust_call(Mgr, SlowPathReq) of
        false -> get_handle(Mgr, Key, SlowPathReq); % see do_write_unlock/2
        NewHandle -> NewHandle
      end
  end.

put_handle(Mgr) ->
  put_handle(Mgr, self()).

put_handle(Mgr, Key) ->
  Ets = Mgr,
  ets:delete(Ets, Key).

get_iterator(Mgr) ->
  get_handle(Mgr, ?iterator(self()), read_lock_iterator).

put_iterator(Mgr) ->
  put_handle(Mgr, ?iterator(self())).

suspend_before_snapshot(Mgr) ->
  robust_call(Mgr, write_lock).

resume_after_snapshot(Mgr) ->
  robust_call(Mgr, write_unlock).

get_all() ->
  Children =
    try supervisor:which_children(leveldb_manager_sup)
    catch exit:{noproc, _} -> [] end,
  lists:map(fun({undefined, Pid, worker, [?MODULE]}) -> Pid end,
            Children).

get_path(Mgr) ->
  robust_call(Mgr, get_path).

set_path(Mgr, Path) ->
  robust_call(Mgr, {set_path, Path}).

close(Mgr) ->
  robust_call(Mgr, stop).

%% If the gen_server crashes we want to give the supervisor
%% a decent chance to restart it before failing our calls.

robust_call(Mgr, Req) ->
  robust_call(Mgr, Req, 99). % (99+1)*100ms = 10s

robust_call(Mgr, Req, 0) ->
  gen_server:call(Mgr, Req, infinity);
robust_call(Mgr, Req, Retries) ->
  try
    gen_server:call(Mgr, Req, infinity)
  catch exit:{noproc, _} ->
    timer:sleep(100),
    robust_call(Mgr, Req, Retries - 1)
  end.

%%%----------------------------------------------------------------

-define(nr_static_keys, 5).
%% Number of static keys stored in the ETS table. The static keys must
%% always be present in the table. On top of them the table will also
%% contain each read lock holder process' pid as a key.
%%
%% By knowing the number of static keys the number of held read locks
%% can be cheaply calculated from the size of the ETS table.

state_init(Name, Path, Options) ->
  Handle = [],
  Pending = [],
  Writer = [],
  ets:insert(Name,
             [ {path, Path}             % path to leveldb instance
             , {options, Options}       % leveldb open options
             , {handle, Handle}         % leveldb handle if online, [] otherwise
             , {pending, Pending}       % From list of pending readers
             , {writer, Writer}]),      % {From,MonRef} of writer, [] otherwise
  ?nr_static_keys = ets:info(Name, size),       % assert
  Name.

state_new(Name) ->
  ets:new(Name,
          [ set
          , public
          , named_table
          , {keypos, 1}
          , {write_concurrency, false}
          , {read_concurrency, false}]).

%% Find existing ETS table with given name and return true,
%% otherwise create it and return false.
%% This MUST execute in the context of the supervisor, in
%% order for new ETS tables to be owned by it.
state(Name) ->
  case ets:info(Name, keypos) of
    undefined ->
      state_new(Name),
      false;
    _ ->
      true
  end.

state_delete(Name) ->
  ets:delete(Name).

state_get_pending(Name) -> ets:lookup_element(Name, pending, 2).
state_get_options(Name) -> ets:lookup_element(Name, options, 2).
state_get_writer(Name) -> ets:lookup_element(Name, writer, 2).
state_get_handle(Name) -> ets:lookup_element(Name, handle, 2).
state_get_path(Name) -> ets:lookup_element(Name, path, 2).

state_set_pending(Name, Pending) -> ets:insert(Name, {pending, Pending}), Name.
state_set_writer(Name, Writer) -> ets:insert(Name, {writer, Writer}), Name.
state_set_handle(Name, Handle) -> ets:insert(Name, {handle, Handle}), Name.
state_set_path(Name, Path) -> ets:insert(Name, {path, Path}), Name.

pid_to_reader_key(Pid) -> Pid.
pid_to_iterator_key(Pid) -> ?iterator(Pid).

state_get_readers(Name, PidToKey) ->
  ets:select(Name, [{ _MatchHead = {PidToKey('$1')}
                    , _Guards    = [{is_pid, '$1'}]
                    , _Result    = ['$_']
                    }]).

state_get_readers(Name) -> state_get_readers(Name, fun pid_to_reader_key/1).
state_get_iterators(Name) -> state_get_readers(Name, fun pid_to_iterator_key/1).

state_add_reader(Name, Key) -> ets:insert(Name, {Key}), Name.

state_remove_reader(Name, Key) -> ets:delete(Name, Key), Name.

state_has_readers(Name) ->
  ets:info(Name, size) > ?nr_static_keys.

%%%----------------------------------------------------------------

init([StateIsOld, State0, Path, Options]) ->
  State =
    case StateIsOld of
      true ->
        restart(State0);
      false ->
        leveldb_online(state_init(State0, Path, Options))
    end,
  {ok, State}.

restart(State) ->
  repair_writer(State).

repair_writer(State) ->
  case state_get_writer(State) of
    [] ->
      State;
    {WFrom = {WPid, _Unique}, _MonRef} ->
      MonRef = erlang:monitor(process, WPid),
      Writer = {WFrom, MonRef},
      state_set_writer(State, Writer)
  end.

handle_call(Req, From, State) ->
  case Req of
    read_lock ->
      handle_read_lock(From, State);
    read_lock_iterator ->
      handle_read_lock_iterator(From, State);
    write_lock ->
      handle_write_lock(From, State);
    write_unlock ->
      handle_write_unlock(From, State);
    get_path ->
      handle_get_path(State);
    {set_path, Path} ->
      handle_set_path(State, Path);
    stop ->
      handle_stop(State);
    _ ->
      {reply, {bad_call, Req}, State}
  end.

handle_cast(_Req, State) ->
  {noreply, State}.

handle_info(Info, State) ->
  case Info of
    {'DOWN', _MonRef, process, Pid, _Info2} ->
      handle_down(Pid, State);
    {reaper, MonRef} ->
      handle_reaper(MonRef, State);
    _ ->
      {noreply, State}
  end.

terminate(_Reason, _State = []) -> ok;
terminate(_Reason, State) ->
  state_delete(leveldb_offline(State)).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%----------------------------------------------------------------

handle_read_lock(RFrom, State) ->
  do_read_lock(RFrom, State, fun pid_to_reader_key/1).

handle_read_lock_iterator(RFrom, State) ->
  do_read_lock(RFrom, State, fun pid_to_iterator_key/1).

do_read_lock(RFrom = {RPid, _Unique}, State0, PidToKey) ->
  Key = PidToKey(RPid),
  %% first undo the failed fast-path attempt
  State = state_remove_reader(State0, Key),
  %% then do the slow-path
  case state_get_writer(State) of
    [] -> % no active or pending writer: take it
      Handle = state_get_handle(State),
      {reply, Handle, state_add_reader(State, Key)};
    _ -> % an active or pending writer: wait for it to leave
      Pending = [RFrom | state_get_pending(State)],
      {noreply, state_set_pending(State, Pending)}
  end.

handle_write_lock(WFrom = {WPid, _Unique}, State0) ->
  case state_get_writer(State0) of
    [] -> % no active or pending writer
      MonRef = erlang:monitor(process, WPid),
      State1 = state_set_writer(State0, {WFrom, MonRef}),
      case state_has_readers(State1) of
        false -> % no active readers: take it
          {reply, ok, leveldb_offline(State1)};
        true ->  % more active readers: wait for them to leave
          schedule_reaper(MonRef),
          {noreply, State1}
      end;
    _ -> % an active or pending writer: fail
      {reply, {error, busy}, State0}
  end.

schedule_reaper(MonRef) ->
  erlang:send_after(timer:seconds(1), self(), {reaper, MonRef}).

handle_reaper(MonRef, State0) ->
  State1 = reaper(State0),
  NewState =
    case state_get_writer(State1) of
      {WFrom, MonRef} -> % pending writer (it didn't die while waiting)
        case state_has_readers(State1) of
          false -> % no active readers: wake the writer
            %% take the write lock, then wake the writer
            State2 = leveldb_offline(State1),
            gen_server:reply(WFrom, ok),
            State2;
          true -> % more active readers: reschedule
            schedule_reaper(MonRef),
            State1
        end;
      _ -> % writer no longer pending: nothing to do
        State1
    end,
  {noreply, NewState}.

reaper(State) ->
  lists:foldl(fun reap_iterator/2,
              lists:foldl(fun reap_reader/2,
                          State,
                          state_get_readers(State)),
              state_get_iterators(State)).

reap_reader({Pid}, State) -> reap_reader(Pid, Pid, State).

reap_iterator({Key = ?iterator(Pid)}, State) -> reap_reader(Pid, Key, State).

reap_reader(Pid, Key, State) ->
  case is_process_alive(Pid) of
    true ->
      State;
    false ->
      state_remove_reader(State, Key)
  end.

handle_write_unlock({WPid, _Unique}, State) ->
  case state_get_writer(State) of
    {{WPid, _Unique2}, MonRef} ->
      {reply, ok, do_write_unlock(MonRef, State)};
    _ ->
      error_logger:error_msg(
        "Leveldb write-unlock without holding write lock: ~p",
        [WPid]),
      {reply, {error, nolock}, State}
  end.

do_write_unlock(MonRef, State0) ->
  erlang:demonitor(MonRef, [flush]),
  State1 = leveldb_online(State0),
  State2 = state_set_writer(State1, []),
  case state_get_pending(State2) of
    [] -> % no pending readers: nothing to do
      State2;
    Pending -> % pending readers: wake them
      %% we could wake each pending reader with an already-held
      %% read lock, but that would complicate the code and isn't
      %% strictly necessary: just have the readers re-try their
      %% read-lock paths; see get_handle/3
      lists:foreach(fun (RFrom) -> gen_server:reply(RFrom, false) end,
                    Pending),
      state_set_pending(State2, [])
  end.

handle_get_path(State) ->
  {reply, state_get_path(State), State}.

handle_set_path(State, Path) ->
  {reply, ok, state_set_path(State, Path)}.

handle_stop(State) ->
  state_delete(leveldb_offline(State)),
  unregister(_Name = State),
  {stop, normal, ok, []}.

handle_down(Pid, State0) ->
  NewState =
    case state_get_writer(State0) of
      {{Pid, _Unique}, MonRef} ->
        do_write_unlock(MonRef, State0);
      _ ->
        State0
    end,
  {noreply, NewState}.

%%%----------------------------------------------------------------

leveldb_online(State) ->
  case state_get_handle(State) of
    [] ->
      {ok, Handle} = eleveldb:open(state_get_path(State),
                                   state_get_options(State)),
      state_set_handle(State, Handle);
    _ ->
      State
  end.

leveldb_offline(State) ->
  case state_get_handle(State) of
    [] ->
      State;
    Handle ->
      eleveldb:close(Handle),
      state_set_handle(State, [])
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
