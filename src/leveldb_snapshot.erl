%%%----------------------------------------------------------------
%%% File        : leveldb_snapshot.erl
%%% Author      : Mikael Pettersson <mikael.pettersson@klarna.com>
%%% Description : Creates a snapshot of a leveldb database
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

-module(leveldb_snapshot).

-export([snapshot/1]).

%% Given Path pointing to a closed leveldb database, create
%% a snapshot in Path/snapshot-<timestamp> by copying the
%% top-level log and manifest files, creating mirror sst_<N>
%% directories, and hardlinking <M>.sst files into the mirror
%% directories.
%%
%% Returns {ok, "snapshot-<timestamp>"} on success.
%%
%% When this returns the leveldb database can be reopened,
%% and the snapshot can be archived or copied, after which
%% it should be deleted with "rm -rf".  Note: files in the
%% snapshot may only be read or unlinked, NEVER written to.
%%
%% This approach to taking a snapshot with hardlinks and then
%% reopening relies on the fact that leveldb does not modify
%% .sst files after they are created: they are only read, or
%% unlinked when compaction makes them obsolete.  (I did not
%% invent it, several Google hits suggest this approach.)
%%
%% The location of the snapshot directory is hardcoded
%% since hardlinks only work within a single file system.
%%
%% PRE: The leveldb database in Path MUST be closed.
%% PRE: The file system in Path MUST support hardlinks.

snapshot(Path) ->
  SnapName = snapname(),
  SnapPath = filename:join(Path, SnapName),
  {ok, Files} = file:list_dir(Path),
  ok = file:make_dir(SnapPath),
  SubDirs = toplevel(Path, SnapPath, Files, []),
  subdirs(Path, SnapPath, SubDirs),
  {ok, SnapPath}.

%% Process top-level leveldb files:
%% - copy manifest and log files
%% - accumulate and return list of sst_<N> subdirs
%% - ignore other files (e.g. old snapshots)

toplevel(Path, SnapPath, [File | Files], SubDirs) ->
  case classify(Path, File) of
    copy ->
      copy(Path, SnapPath, File),
      toplevel(Path, SnapPath, Files, SubDirs);
    subdir ->
      toplevel(Path, SnapPath, Files, [File | SubDirs]);
    ignore ->
      toplevel(Path, SnapPath, Files, SubDirs)
  end;
toplevel(_Path, _SnapPath, [], SubDirs) ->
  SubDirs.

copy(Path, SnapPath, File) ->
  {ok, _ByteCount} =
    file:copy(filename:join(Path, File),
              filename:join(SnapPath, File)).

%% Process each sst_<N> leveldb subdir:
%% - mkdir a shadow in the snapshot directory
%% - populate the shadow with hard links to all files in the original

subdirs(Path, SnapPath, [SubDir | SubDirs]) ->
  ok = file:make_dir(filename:join(SnapPath, SubDir)),
  {ok, Files} = file:list_dir(filename:join(Path, SubDir)),
  hardlink(Path, SnapPath, SubDir, Files),
  subdirs(Path, SnapPath, SubDirs);
subdirs(_Path, _SnapPath, []) ->
  ok.

hardlink(Path, SnapPath, SubDir, [File | Files]) ->
  ok = file:make_link(filename:join([Path, SubDir, File]),
                      filename:join([SnapPath, SubDir, File])),
  hardlink(Path, SnapPath, SubDir, Files);
hardlink(_Path, _SnapPath, _SubDir, []) ->
  ok.

%% Classify a leveldb top-level file as copy, subdir, or ignore.

classify(Path, File) ->
  classify(Path, File,
           [ {"^.*\\.log$", copy}
           , {"^CURRENT$", copy}
           , {"^MANIFEST-", copy}
           , {"^LOG$", copy}
           , {"^sst_[0-9]$", subdir}
           , {"^snapshot-", ignore}]).

classify(Path, File, [{RE, Class} | Clauses]) ->
  case re:run(File, RE) of
    {match, _} -> Class;
    nomatch -> classify(Path, File, Clauses)
  end;
classify(Path, File, []) ->
  %% We used to ignore unknown top-level files, but if application code
  %% stores additional state there, it needs to be included in snapshots.
  %% Unknown top-level directories are ignored.
  case filelib:is_dir(filename:join(Path, File)) of
    true -> ignore;
    false -> copy
  end.

%% Generate a file name "snapshot-YYYY-MM-DD-HH:MM:SS"
%% based on os:timestamp().

snapname() ->
  {{Year, Month, Day}, {Hour, Minute, Second}} =
    calendar:now_to_universal_time(os:timestamp()),
  lists:flatten(
    io_lib:format("snapshot-~4w-~2..0w-~2..0w-~2..0w:~2..0w:~2..0w",
                  [Year, Month, Day, Hour, Minute, Second])).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
