# Changelog

All notable changes to this project will be documented in this file.

Older entries have been generated from github releases.
New entries aim to adhere to the format proposed by [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## Changed

- For local basf2 versions, change how hash for `basf2_release` Parameter is calculated. Now use basf2 functionality to get the version, to be consistent with the output of `basf2 --version`. The new hash encodes both the local and central basf2 release, the basf2 function [`getCommitID`](https://github.com/belle2/basf2/blob/1c972b2c89ef11f38ee2f5ea8eb562dde0637155/framework/io/include/RootIOUtilities.h#L77-L84). When basf2 is not set up, print warning before returning `"not_set"`. Thanks to @GiacomoXT in [#193](https://github.com/nils-braun/b2luigi/issues/193).

  **Warning:** If you use local basf2 versions, that is your `basf2_release` is a git hash, this will change your b2luigi target output paths. This means that tasks that were marked _complete_, might suddenly not be _complete_ anymore after updating to this release. A workaround is to check for the new expected path via `python3 <steering_fname>.py --show_output` and rename the `git_hash=<…>` directory.

- Apply `max_events` Parameter not by changing the environment singleton, but instead forward it to `basf2.process` call. This should hopefully not affect the behaviour in practice. Also by @GiacomoXT in [#193](https://github.com/nils-braun/b2luigi/issues/193)

- Refactor the basf2 related examples to use more idiomatic, modern basf2 code, e.g. using `basf2.Path()` instead of `basf2.create_path()`. . Also by @GiacomoXT in [#193](https://github.com/nils-braun/b2luigi/issues/193)

## Fixed

- Fix example `SimulationTask` task in `basf2_chain_example.py`, which probably wasn't working as it was missing the Geometry module. Also by @GiacomoXT in [#193](https://github.com/nils-braun/b2luigi/issues/193)


**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.9.1...main

## [0.9.1] - 2023-03-20

### Fixed

- Fix circular import [#188](https://github.com/nils-braun/b2luigi/issues/188)

### Added

- Add the ability to pass a custom hashing function to parameters via the `hash_function` keyword argument. The function must take one argument, the value of the parameter. It is up to the user to ensure unique strings are created. [#189](https://github.com/nils-braun/b2luigi/pull/189)
- **gbasf2**: Switch to the `--new` flag in `gb2_ds_get` which downloads files significantly faster than previously. Gbasf2 release v5r6 (November 2022) is required. [#190](https://github.com/nils-braun/b2luigi/pull/190).

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.9.0...v0.9.1

## [0.9.0] - 2023-03-20

### Fixed

- **gbasf2**: Fix bug introduced in [#181](https://github.com/nils-braun/b2luigi/pull/181) when generating basf2 queries with just a simple `.root` extension, raising a wrong false positive errors. Now moved splitting functionality into separate function and added extensive unit tests. Thanks @schmitca for reporting [#184](https://github.com/nils-braun/b2luigi/pull/184).

### Added

- `task_iterator` now returns a unique list of tasks. The task graph is a DAG which is traversed through recursion in `task_iterator` like a tree. If multiple tasks had the same task as a requirement (i.e. multiple nodes share a child), it was returned multiple times in the task iterator. This results in performance improvements when checking the requirements. [#186](https://github.com/nils-braun/b2luigi/pull/186). Thanks [@MarcelHoh](https://github.com/MarcelHoh) for the initial PR.

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.8.2...v0.9.0

## [0.8.2] - 2023-01-13

### Fixed

- **gbasf2**: Fix gbasf2 glob queries (e.g. for downloading) for basf2 output files with multiple extensions, e.g. `<file>.udst.root` `<file>.mdst.root`. [#181](https://github.com/nils-braun/b2luigi/pull/181). Thanks [@schmitca](https://github.com/schmitca) for reporting, reviewing and testing.

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.8.1...v0.8.2

## [0.8.1] - 2022-11-14

### Fixed

- **gbasf2**: Fix `ioctl` error in `gb2_proxy_init` by reading in password via `b2luigi` and then supplying password to that command directly, instead of letting `gb2_proxy_init` handle the password prompt. [#172](https://github.com/nils-braun/b2luigi/pull/172) @bilokin

### Added

- **gbasf2**: Add `gbasf2_proxy_group` and `gbasf2_project_lpn_path` parameters to switch between gbasf2 groups. [#175](https://github.com/nils-braun/b2luigi/pull/175) @bilokin
- add automatic "_needs changelog_" PR labeller as github workflow [#166](https://github.com/nils-braun/b2luigi/pull/166)

### Changed

- Update `pre-commit` hooks. Most notably for the developers, update the `flake8` syntax and style-checker to version 5.0.4, which might change slightly what style is accepted. This should also fix [an issue](https://github.com/python/importlib_metadata/issues/406) with the old flake8 version not being compatible with the latest version of `importlib_meta`, which the pre-commit flake8 hook in the github actions to fail. In the process also migrated the pre-commit config format to the new layout.

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.7.6...v0.8.1

## [0.7.6] - 2022-01-22

### Fixed

- **htcondor**: Make `HTCondorProcess.get_job_status` a method again instead. It was turned into a property accidentally in #158. See issue #164 @eckerpatrick and PR @165 @mschnepf.

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.7.5...v0.7.6

## [0.7.5] - 2022-01-21

### Added

- **htcondor**: Do up to 3 retries for getting job status with `condor_q` #155
- **gbasf2**: Add caching and unit tests to `get_dirac_user` #156
- Add @mschnepf to [the contributors](https://github.com/nils-braun/b2luigi/blob/main/docs/index.rst#the-team) for #158
- Some minor documentation improvements #151 and typo fix in help message #153.

### Fixed

- **gbasf2**: Adapt to new file name for gbasf2 setup file (`setup` → `setup.sh`) #160
- **gbasf2**: Ensure proxy is initalized before running `get_proxy_info` to get dirac user #156
- **htcondor**: Don't fail when htcondor job status is `suspended` or `transferring_output` #158. Thanks to @mschnepf 🙇.

### Changed

- **gbasf2**: Use `retry2` package for retrying getting of gbasf2 project status instead of my own recursive loop #161

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.7.4...v0.7.5

## [0.7.4] - 2021-11-03

### Added

- Add a `CHANGELOG.md` file in addition to the release notes on github

### Fixed

- **gbasf2**: Fix moving of downloaded datasets with multiple datablocks (subs) #150
- **gbasf2**: If an error happens during proxy initialization, there was an error raised, but the `stderr` argument was wrong, which was fixed in #149

### Changed

- **gbasf2**: `get_unique_lfns` in some cases returned a set and in some cases a list. Changed it to always return sets.

## [0.7.3] - 2021-10-21

Small patch release for the gbasf2 process adding tests and better error checks for subprocess to make future debugging of problems like e.g. #138 easier

### Added

- Check output of `gb2_proxy_init` for errors by @meliache in https://github.com/nils-braun/b2luigi/pull/142
  - if `gb2_proxy_init` fails due to a wrong certificate password, re-run the command until the user enters a correct password
  - raises a `CalledProcessError` when there is any other error string in the stdout of `gb2_proxy_init`. Since that script doesn't exit with errorcodes in case of errors, otherwise errors could go unnoticed and resulted errors in later commands, such as when using `gb2_proxy_info`. Tracking down which command originally failed might be some work, so this should make debugging much easiert.
- Don't subpress `CalledProcessError` in `get_proxy_info`
- Add unit tests for `setup_dirac_proxy` and `get_proxy_info` by mocking possible outputs of `gb2_proxy_info` and `gb2_proxy_init`.

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.7.2...v0.7.3

## [0.7.2] - 2021-10-20

### Added

- Test `ignore_additional_command_line_args` option by @meliache in https://github.com/nils-braun/b2luigi/pull/128
- Add Moritz Baur and Artur Gottman to contributors list in documentation by @meliache in https://github.com/nils-braun/b2luigi/pull/133, https://github.com/nils-braun/b2luigi/pull/137

### Fixed

- Fix gb2_proxy_init error due to wrong HOME from gbasf2 setup script by @meliache in https://github.com/nils-braun/b2luigi/pull/141

### Changed

- Remove unused `Gbasf2Process` helper method to capture failed files from stdout by @meliache in https://github.com/nils-braun/b2luigi/pull/136

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.7.1...v0.7.2

## [0.7.1] - 2021-10-11

### Added

- Added `inherits_without` decorator to enable inheritance of everything except chosen parameters from task by @sognetic in https://github.com/nils-braun/b2luigi/pull/106
- tasks can return all input or all outputfiles with a single call by @anselm-baur in https://github.com/nils-braun/b2luigi/pull/111
- Allow for gbasf2 projects with multiple output `sub<xy>` directories by @meliache in https://github.com/nils-braun/b2luigi/pull/122

### Fixed

- Bugfix: Don't use deprecated exception messsage property by @meliache in https://github.com/nils-braun/b2luigi/pull/120
- Fix gbasf2 batch example for new basf2 releases: import ROOT by @meliache in https://github.com/nils-braun/b2luigi/pull/123
- Ignore flake8 error for unused ROOT import by @meliache in https://github.com/nils-braun/b2luigi/pull/124

### Changed

- Give instances of `MasterTask` in doc examples less sensitive names by @meliache in https://github.com/nils-braun/b2luigi/pull/118
- Replace parsing of gb2 command output with DIRAC API calls by @philiptgrace in https://github.com/nils-braun/b2luigi/pull/121
- Use latest gbasf2 release on cvmfs as default install directory by @meliache in https://github.com/nils-braun/b2luigi/pull/126
- Improve dirac proxy validity time handling by @philiptgrace in https://github.com/nils-braun/b2luigi/pull/127
- For gbasf2 download with `gb2_ds_get`, use new `--failed_lfns` option to get file with LFNs for which download failed instead of parsing stdout.
  by @ArturAkh in https://github.com/nils-braun/b2luigi/pull/132

## New Contributors

- @sognetic made their first contribution in https://github.com/nils-braun/b2luigi/pull/106

**Full Changelog**: https://github.com/nils-braun/b2luigi/compare/v0.6.7...v0.7.1

## [0.6.6] - 2021-07-14

#### Added

- Improved gbasf2 download from grid. In particular, when re-trying a download, only re-download files which have previously failed. Store failed files in a `failed_files.txt`. The downside is that this relies on command output parsing which might break between releases. If errors occur, this can be worked around by removing the `failed_files.txt`, triggering a full re-download.

## [0.6.7] - 2021-07-14

#### Added

- Set progress bar in central scheduler for tasks executed as a gbasf2 project
  showing what percentage of jobs in the project is done and display the total numbers in the status.

#### Changed

- Fix gbasf2 download retry issues for new gbasf2 releases
- Fix in gbasf2 batch for memory error caused by changes in the ROOT from basf2 externals v10 (affects latest light releases).

## [0.6.5] - 2021-04-29

### Fixed

#97 Gbasf2 Bugfix: Fix download for failed files

## [0.6.4] - 2021-04-29

### Fixed

#95 Fix to show correct version number for `b2luigi.__version__ `
Minor patch release but I decided to release this early so that users can use the version number to validate that they are using the latest release.

## [0.6.3] - 2021-04-27

This release features small quality-of-life improvements and fixes for the gbasf2 batch, so I decide to make it a minor release.

Since we're still major release 0, instead of SemVer I think I will be creating minor releases for significant changes to luigi themselves and where all users should read the release notes and patch release for small patches that come out shortly after a release or when I do small non-api-breaking changes to individual batches only, which only affect users of that batch and don't really change b2luigi itself.

### Gbasf2 Batch

#### Features

- #75 allow defining grid input LFNs via text files with `gbasf2_input_dslist` setting, analogous to `gbasf2 --input_dslist`
- #91: Several improvements of gbasf2 handling (thanks to @ArturAkh)
  - possibility to add input datafiles with `gbasf2_input_datafiles` option, which will be downloaded from SE's in addition. This is useful in case the sandbox files exceed 10 MB.
  - improved rescheduling: instead of performing it for each single failed job separately, perform it at once. Keeping track of n_retries is still maintained in the implementation of this pull request.
  - improved downloading of datasets: in case of failed downloads only the ones which are failed, are downloaded, based on a collection of LFNs from created from `gb2_ds_get` stdout.
- more unit tests for more stability in the future and getting a handle on growing complexity.

#### Fixes

- #91: Several improvements of gbasf2 handling
  - fix of RuntimeError ---> RuntimeWorking conversion: first argument of `warnings.warn` should be a string. Otherwise, getting a uncatched TypeError, followed by a PipeError of luigi.
  - added an improved handling of the `JobStatus` for `Done` jobs, since in some (rare) cases, `JobStatus` is set to `Done`, while `ApplicationStatus` is not `Done` (in particular, has an Upload error for output file).

### Meta

- #92 Fix CI shield on github

## [0.6.2] - 2021-03-31

- #88 Bugfix gbasf2 dataset download where failed download raises runtime error instead of intended warning, thanks to @philiptgrace for finding and fixing this.

## [0.6.1] - 2021-03-24

Upps, release v0.6.0 was mistakenly missing two PR's, #79 and #81, since I added the tag at the HEAD of the last branch that I merged and that branch didn't contain those PR's yet. into that release (#79), but that branch wasn't rebased to the head of main and didn't contain the LSF bugfix PR #81 and the gbasf2 feature PR #77 for supporting global tags. So this patch release includes those PRs and also it includes a fix to our PyPi publishing workflow (#82).

### Fixed

- #81: Bugfix in LSF batch code for getting settings
- #82: Fix missing depency in github workflow for automatic publishing to PyPi

### Added

- #77: the global tags have been also added to the sub-set of the basf2-state that is pickled and send to the grid. Remember, the gbasf2 batch wrapper just pickle the basf2 path and sends this to grid, so everything that is saved in the basf2 state is not transferred. In the previous release we already added pickling the basf2 variable aliases separately, now the global tags have also been added. If you have ideas how to handle this more generally, feel free to contribute via issue #35

## [0.6.0] - 2021-03-22

### Added

- use github actions / workflows for CI and PyPi deployment #78

  Code-coverage tests automated and enforced with `codecov` to encourage writing unittests. This already resulted in some new unittests for the `htcondor` batch :)

- New optional `job_name` setting for assigning human-readable names for groups of jobs in **LSF** and **HTCondor** batches. This is useful when checking job statuses by hand. See documentation for more. #76, #79

- #55 Optional to only pass known command line arguments, usueful in scripting if you want to pass additional command line args that should be forwarded to the script instead of being used by b2luigi

- #70 Users can now add a `dry_run` method to their tasks which will be called during dry-run, e.g. if the b2luigi steering file is executed with `python3 <b2luigi_file_name>.py --dry-run`

### Fixed

- Adapt download of job outputs to new gbasf2 v5 output directory structure by adding `/sub00` to LFN's #57. Caveats are:S

  - In future releases gbasf2 will split the outputs of large projects into multiple `sub<xy>` directories, but this isn't done as of now. These other subdirectories are not supported yet, but I created issue #80 as a reminder

  - The output of `mdst`/`udst` files is moved into subdirectories deeper in the hierarchie. We don't support that yet either. I have to think about whether I can figure out in a smart way what the output is or if the user should provide some additional info. Best would be to do it in parallel to what gbasf2 does. See issue #58 for more, help is welcome.

### Changed

- More **stable downloads** with `gb2_ds_get`

  When I started developing the gbasf2 wrapper, I expected that the failing of downloads will be a rare exception, but I realized that it is the norm and adapted the code to handle that more gracefully.

  - #72 if one job download fails, this doesn't raise a full exception anymore, so all the other tasks continue to run/download their outputs. The only thing that happens is that this particular task is marked a `failed`

  - **downloaded datasets persist after failure** #67: If a download fails, the partially downloaded dataset remains in a directory with the `.partial` ending next to the expected output directory. On the one hand this ensures that b2luigi doesn't prematurely mark a task as completed until all job outputs in a gbasf2 project downloaded completely. The `.partial` directory is only renamed to the final output directory, which b2luigi uses as a completeness target, once all jobs have been downloaded. On the other hand, keeping the partial downloads means that the download doesn't have to start from scratch everytime that you re-run a failed task. So, if a gbasf2 task failed downloading, you can just re-run the task and it will re-run the download of the missing outputs in your `.partial directory`

  - #62: Option to disable automatic log download from gird via `gbasf2_download_logs` setting. Logs are useful for debugging and reproducibility and I think they should always be stored in addition to the data itself. However, for gbasf2 it can take quite a while to download logs, so sometimes if in a hurry it can be useful disabling them and just looking them up online with the dirac web app if you need them.

## [0.5.1] - 2021-03-22

### Added

- New luigi release 3 as dependency. This drops python2 support in luigi, which we didn't have anyway in b2luigi, so there should be no backwards incompatibility issues. On the plus side, this solves a dependency conflict with jupyter due to different required `tornado` versions
- Allow dashes and underscores (`_`, `-` in `gbasf2`) project names #45
- allow variable aliases #40

### Fixed

- fix issue with `core.utils.get_filename()` in jupyter #34
- fix code in some basf2 examples to work with newer basf2 releases #37, #38, #37
- fix logic bug in setting `gbasf2_additional_params` #43
- modified time parsing that recognizes dirac proxy validity times > 24h #46
- workaround gbasf2 wildcard bug #41
- for dirac proxy handling, replace gbasf2 command string-parsing with direct communication with DIRAC Api via sub-script #51. Intended as an feature, but I think this also fixed a bug with a newer gbasf2 release

### Changed

- default branch is now `main` #39

## [0.5.0] - 2020-05-23

### Deprecated

- deprecate some settings (#22)

### Fixed

- corrected path to decfile for new structure in basf2 release-04 (#23)

### Added

- Adding option to provide userdefined location of the task executable. This can be used analog to the optional task attribute . (#25)
- Soft wrapper for gbasf2 as a b2luigi BatchProcess (#32)

### Changed

- Warning if forward slash in parameter (#27)
- change link to documentaion from latest to stable (#29)
- additional requirements structure (#30)

## [0.4.4] - 2019-10-29

Features in this release:

- small bugfixes with envs and basf2 tasks (@nils-braun)

## [0.4.3] - 2019-10-28

Features in this release:

- Added documentation
- Re-add an old feature for log files, will soon be deprecated.

## [0.4.2] - 2019-10-28

Features in this release:

- Fixed a problem with basf2 module importing (@nils-braun)
- Better handling for filesystems (#21) (@nils-braun)
  Started supporting file copy mechanisms in htcondor, do only create folders when needed, better relative path handling.

## [0.4.1] - 2019-10-25

Features in this release:

- Added relevant authors in docu (Nils Braun)
- Fixed travis config (Nils Braun)

## [0.3.2] - 2019-10-25

Features in this release:

- Fixed required versions of packages (#15) (@nils-braun)

## [0.4.0] - 2019-10-25

Features in this release:

- Batch Improvements (#20) (@nils-braun):
  Generalize and simplify the batch setup and the dispatch method.
  Updated and added a lot of documentation.
  Please see the docu or the examples to check out the new ways
  to setup the batch environment.

- HTCondor support (#19) (@welschma):
  Added long-needed support for HTCondor batch systems.
  Building block for #20.

- Added Community Documents (@nils-braun)

- Fix serialized parameters for basf2 tasks (#18) (@elimik31):
  Fixed problems after refactoring in basf2 tasks

- Fix for get_basf2_git_hash to work with new basf2 tools (#17) (@elimik31)
  Check for the correct release name or head
