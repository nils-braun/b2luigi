"""
Test helper functions for :py:class:`Gbasf2Process`.

Utilities that require a running gbasf2 or Dirac environment will not net
tested in this test case, only the functions that can be run independently.
"""

import unittest
from collections import namedtuple
from contextlib import redirect_stderr
from io import StringIO
from os.path import join
from pathlib import Path
from subprocess import CalledProcessError
from unittest import mock
import tempfile
from typing import Iterable
import shutil

from b2luigi.batch.processes.gbasf2 import (
    _get_lfn_upto_reschedule_number,
    get_proxy_info,
    get_unique_lfns,
    lfn_follows_gb2v5_convention,
    query_lpns,
    setup_dirac_proxy,
    _move_downloaded_dataset_to_output_dir,
    get_dirac_user,
)

# first test utilities for working with logical file names on the grid


class TestLFNFollowsGbasf2V5Convention(unittest.TestCase):
    def test_newstyle_lfn_is_true(self):
        lfn = "Upsilon4SBpcandee_00000_job181817516_03.root"
        self.assertTrue(lfn_follows_gb2v5_convention(lfn))

    def test_oldstyle_lfn_is_false(self):
        lfn = "my_ntuple_0001.root"
        self.assertFalse(lfn_follows_gb2v5_convention(lfn))


class TestLFNUptoRescheduleNumber(unittest.TestCase):
    def test_strings_equal_camelcase_lfn(self):
        lfn = "Upsilon4SBpcandee_00000_job181817516_03.root"
        self.assertEqual(
            _get_lfn_upto_reschedule_number(lfn), "Upsilon4SBpcandee_00000_job181817516"
        )

    def test_strings_equal_snakecase_lfn(self):
        lfn = "my_ntuple_name_00000_job181817516_03.root"
        self.assertEqual(
            _get_lfn_upto_reschedule_number(lfn), "my_ntuple_name_00000_job181817516"
        )

    def test_oldstyle_lfn_raises_error(self):
        lfn = "my_ntuple_0001.root"
        with self.assertRaises(ValueError):
            _get_lfn_upto_reschedule_number(lfn)


class TestGetUniqueLFNS(unittest.TestCase):
    def setUp(self):
        # list of logical path names on the grid that only differe by the reschedule number (last two digits)
        self.lfns_with_duplicates = [
            "Upsilon4SBpcandee_00000_job181817516_00.root",
            "Upsilon4SBpcandee_00001_job181817517_00.root",
            "Upsilon4SBpcandee_00002_job181817518_00.root",
            "Upsilon4SBpcandee_00003_job181817519_00.root",
            "Upsilon4SBpcandee_00004_job181817520_00.root",
            "Upsilon4SBpcandee_00005_job181817521_00.root",
            "Upsilon4SBpcandee_00006_job181817522_00.root",
            "Upsilon4SBpcandee_00007_job181817523_00.root",
            "Upsilon4SBpcandee_00008_job181817524_00.root",
            "Upsilon4SBpcandee_00009_job181817525_00.root",
            "Upsilon4SBpcandee_00010_job181817526_00.root",
            "Upsilon4SBpcandee_00011_job181817527_00.root",
            "Upsilon4SBpcandee_00012_job181817528_00.root",
            "Upsilon4SBpcandee_00012_job181817528_00.root",
            "Upsilon4SBpcandee_00012_job181817528_02.root",
            "Upsilon4SBpcandee_00013_job181817529_00.root",
            "Upsilon4SBpcandee_00014_job181817530_00.root",
            "Upsilon4SBpcandee_00015_job181817531_00.root",
            "Upsilon4SBpcandee_00016_job181817532_00.root",
            "Upsilon4SBpcandee_00017_job181817533_00.root",
            "Upsilon4SBpcandee_00017_job181817533_01.root",
            "Upsilon4SBpcandee_00017_job181817533_02.root",
            "Upsilon4SBpcandee_00018_job181817534_01.root",
            "Upsilon4SBpcandee_00019_job181817535_00.root",
        ]
        # create same input but with more underscores in initial root file name (snake case)
        # to test whether the string splitting logic is stable in that case
        self.lfns_with_duplicates_snake_case = [
            lfn.replace("Upsilon4SBpcandee", "u4s_Bp_cand_")
            for lfn in self.lfns_with_duplicates
        ]
        self.unique_lfns = {
            "Upsilon4SBpcandee_00000_job181817516_00.root",
            "Upsilon4SBpcandee_00001_job181817517_00.root",
            "Upsilon4SBpcandee_00002_job181817518_00.root",
            "Upsilon4SBpcandee_00003_job181817519_00.root",
            "Upsilon4SBpcandee_00004_job181817520_00.root",
            "Upsilon4SBpcandee_00005_job181817521_00.root",
            "Upsilon4SBpcandee_00006_job181817522_00.root",
            "Upsilon4SBpcandee_00007_job181817523_00.root",
            "Upsilon4SBpcandee_00008_job181817524_00.root",
            "Upsilon4SBpcandee_00009_job181817525_00.root",
            "Upsilon4SBpcandee_00010_job181817526_00.root",
            "Upsilon4SBpcandee_00011_job181817527_00.root",
            "Upsilon4SBpcandee_00012_job181817528_02.root",
            "Upsilon4SBpcandee_00013_job181817529_00.root",
            "Upsilon4SBpcandee_00014_job181817530_00.root",
            "Upsilon4SBpcandee_00015_job181817531_00.root",
            "Upsilon4SBpcandee_00016_job181817532_00.root",
            "Upsilon4SBpcandee_00017_job181817533_02.root",
            "Upsilon4SBpcandee_00018_job181817534_01.root",
            "Upsilon4SBpcandee_00019_job181817535_00.root",
        }
        self.unique_lfns_snake_case = {
            lfn.replace("Upsilon4SBpcandee", "u4s_Bp_cand_") for lfn in self.unique_lfns
        }

    def test_sets_equal(self):
        unique_lfns = get_unique_lfns(self.lfns_with_duplicates)
        self.assertSetEqual(unique_lfns, self.unique_lfns)

    def test_count_equal(self):
        unique_lfns = get_unique_lfns(self.lfns_with_duplicates)
        self.assertCountEqual(unique_lfns, self.unique_lfns)

    def test_sets_equal_underscore_in_file_name(self):
        unique_lfns_snake_case = get_unique_lfns(self.lfns_with_duplicates_snake_case)
        self.assertSetEqual(unique_lfns_snake_case, self.unique_lfns_snake_case)

    def test_sets_equal_input_as_set(self):
        unique_lfns = get_unique_lfns(set(self.lfns_with_duplicates))
        self.assertSetEqual(unique_lfns, self.unique_lfns)

    def test_sets_equal_underscore_in_file_name_input_as_set(self):
        unique_lfns_snake_case = get_unique_lfns(
            set(self.lfns_with_duplicates_snake_case)
        )
        self.assertSetEqual(unique_lfns_snake_case, self.unique_lfns_snake_case)


# In many of the tests we mock ``run_with_gbasf2``, which is used to run
# ``gb2_proxy_init``. Let's define a pseudo process class with stdout and
# stderr for that and define some common output messages of gb2_proxy_init
MockProcess = namedtuple("mock_process", ["stdout", "stderr"])


class TestSetupDiracProxy(unittest.TestCase):

    success_msg = "Succeed with return value:\n0"
    error_msg = "Error: Operation not permitted ( 1 : )"
    wrong_pw_msg = (
        "Generating proxy..." "Enter Certificate password:" "Bad passphrase"
    ) + f"\n{error_msg}"

    @mock.patch("b2luigi.batch.processes.gbasf2.getpass")
    @mock.patch("b2luigi.batch.processes.gbasf2.run_with_gbasf2")
    @mock.patch("b2luigi.batch.processes.gbasf2.get_proxy_info")
    def test_dont_setup_when_proxy_alive(
        self, mock_get_proxy_info, mock_run_with_gbasf2, mock_getpass
    ):
        mock_get_proxy_info.return_value = {"secondsLeft": 999}
        setup_dirac_proxy()
        # check that gb2_proxy_init was never called via subprocess
        mock_getpass.return_value = 'pwd'
        self.assertEqual(mock_run_with_gbasf2.call_count, 0)

    @mock.patch("b2luigi.batch.processes.gbasf2.getpass")
    @mock.patch("b2luigi.batch.processes.gbasf2.run_with_gbasf2")
    @mock.patch("b2luigi.batch.processes.gbasf2.get_proxy_info")
    def test_setup_proxy_on_0_seconds(self, mock_get_proxy_info, mock_run_with_gbasf2, mock_getpass):
        # force setting up of new proxy
        mock_get_proxy_info.return_value = {"secondsLeft": 0}
        mock_getpass.return_value = 'pwd'
        mock_run_with_gbasf2.return_value = MockProcess(self.success_msg, "")
        setup_dirac_proxy()
        self.assertEqual(mock_run_with_gbasf2.call_count, 1)

    @mock.patch("b2luigi.batch.processes.gbasf2.getpass")
    @mock.patch("b2luigi.batch.processes.gbasf2.run_with_gbasf2")
    @mock.patch("b2luigi.batch.processes.gbasf2.get_proxy_info")
    def test_setup_proxy_when_no_proxy_info(
        self, mock_get_proxy_info, mock_run_with_gbasf2, mock_getpass
    ):
        # pretend proxy is not initalized yet, then get_proxy_info raises CalledProcessError
        mock_get_proxy_info.side_effect = CalledProcessError(
            1, ["gb2_proxy_info", "-g", "belle"]
        )
        mock_run_with_gbasf2.return_value = MockProcess(self.success_msg, "")
        setup_dirac_proxy()
        self.assertEqual(mock_run_with_gbasf2.call_count, 1)

    @mock.patch("b2luigi.batch.processes.gbasf2.getpass")
    @mock.patch("b2luigi.batch.processes.gbasf2.run_with_gbasf2")
    @mock.patch("b2luigi.batch.processes.gbasf2.get_proxy_info")
    def test_retry_on_wrong_password(self, mock_get_proxy_info, mock_run_with_gbasf2, mock_getpass):
        # force setting up of new proxy
        mock_get_proxy_info.return_value = {"secondsLeft": 0}

        # mock run_with_gbasf2 to first return wrong PW message a couple of times, before returning successfully
        return_processes = [
            MockProcess(self.wrong_pw_msg, ""),
            MockProcess("", self.wrong_pw_msg),
            MockProcess(self.success_msg, ""),
        ]
        mock_run_with_gbasf2.side_effect = return_processes

        stderr = StringIO()
        with redirect_stderr(stderr):
            setup_dirac_proxy()
        self.assertListEqual(
            stderr.getvalue().splitlines(),
            [
                "Wrong certificate password, please try again.",
                "Wrong certificate password, please try again.",
            ],
        )
        self.assertEqual(mock_run_with_gbasf2.call_count, len(return_processes))

    @mock.patch("b2luigi.batch.processes.gbasf2.getpass")
    @mock.patch("b2luigi.batch.processes.gbasf2.run_with_gbasf2")
    @mock.patch("b2luigi.batch.processes.gbasf2.get_proxy_info")
    def test_raises_error_when_errormsg_in_stdout(
        self, mock_get_proxy_info, mock_run_with_gbasf2, mock_getpass
    ):
        mock_get_proxy_info.return_value = {"secondsLeft": 0}
        # check that gb2_proxy_init was never called via subprocess
        mock_run_with_gbasf2.return_value = MockProcess(self.error_msg, "")
        with self.assertRaises(CalledProcessError):
            setup_dirac_proxy()

    @mock.patch("b2luigi.batch.processes.gbasf2.run_with_gbasf2")
    def test_get_proxy_info_doesnt_suppress_called_process_error(
        self, mock_run_with_gbasf2
    ):
        mock_run_with_gbasf2.side_effect = CalledProcessError(
            1, ["gb2_proxy_info", "-g", "belle"]
        )
        with self.assertRaises(CalledProcessError):
            get_proxy_info()
        self.assertEqual(mock_run_with_gbasf2.call_count, 1)


class TestQueryLPNs(unittest.TestCase):
    @mock.patch("b2luigi.batch.processes.gbasf2.run_with_gbasf2")
    def test_single_file_list(self, mock_run_with_gbasf2):
        output = (
            '["/belle/Data/release-05-01-24/DB00001454/SkimP12x1/prod00019339/e0008/4S/r02547/'
            'hadron/11180500/udst/sub00/udst_000001_prod00019339_task72546000001.root"]\n'
        )
        mock_run_with_gbasf2.return_value = MockProcess(output, "")
        lpns = query_lpns("mock query", "mock user")
        self.assertListEqual(
            lpns,
            [
                join(
                    "/belle/Data/release-05-01-24/DB00001454/SkimP12x1/prod00019339/e0008/4S/r02547/",
                    "hadron/11180500/udst/sub00/udst_000001_prod00019339_task72546000001.root",
                )
            ],
        )

    @mock.patch("b2luigi.batch.processes.gbasf2.run_with_gbasf2")
    def test_type_error_when_json_output_not_list(self, mock_run_with_gbasf2):
        wrong_type_output = '{"hello": "world"}\n'
        mock_run_with_gbasf2.return_value = MockProcess(wrong_type_output, "")
        with self.assertRaises(TypeError):
            query_lpns("mock query", "mock user")


class TestMoveDownloadedDatasetToOutputDir(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _create_mock_project_download_dir(
        self, project_name: str, root_files_per_sub: Iterable[int] = (0,)
    ) -> Path:
        downloaded_project_path = self.test_dir / project_name
        downloaded_project_path.mkdir(exist_ok=True)
        for sub_idx, n_files in enumerate(root_files_per_sub):
            assert sub_idx < 100, "There  can't be more than 100 subs"
            # create sub<xy>-directory
            sub_dir = downloaded_project_path / f"sub{sub_idx:02d}"
            sub_dir.mkdir(exist_ok=False)
            # create dummy root files in sub<xy> directory:
            for file_idx in range(n_files):
                root_fpath = sub_dir / f"sub{sub_idx:02d}_file{file_idx:04d}.root"
                root_fpath.touch(exist_ok=False)
        return downloaded_project_path

    def test_moving_of_multiple_subs(self):
        root_files_per_sub = [10, 10, 10]
        project_path = self._create_mock_project_download_dir(
            project_name="multiple_subs", root_files_per_sub=root_files_per_sub
        )
        project_root_fnames = [
            root_fpath.name for root_fpath in project_path.glob("sub*/*.root")
        ]

        output_path = self.test_dir / "multiple_subs_output.root"
        _move_downloaded_dataset_to_output_dir(
            project_path.as_posix(), output_path.as_posix()
        )
        output_root_fnames = [
            root_fpath.name for root_fpath in output_path.glob("*.root")
        ]

        # check that all root files have been moved to output directory
        self.assertCountEqual(output_root_fnames, project_root_fnames)
        # Test the number of output files is what we expect
        self.assertEqual(len(output_root_fnames), sum(root_files_per_sub))
        # check that input directory is empty after move
        self.assertListEqual(list(project_path.glob("sub*/*.root")), [])

    def test_error_when_a_sub_is_empty(self):
        root_files_per_sub = [10, 0, 10]
        project_path = self._create_mock_project_download_dir(
            project_name="empty_subs", root_files_per_sub=root_files_per_sub
        )
        output_path = self.test_dir / "empty_subs_output.root"
        with self.assertRaises(RuntimeError):
            _move_downloaded_dataset_to_output_dir(
                project_path.as_posix(), output_path.as_posix()
            )


class TestGetDiracUser(unittest.TestCase):

    @mock.patch("b2luigi.batch.processes.gbasf2.get_proxy_info")
    @mock.patch("b2luigi.batch.processes.gbasf2.setup_dirac_proxy")
    def test_get_dirac_user_gets_user(self, _, mock_get_proxy_info):
        mock_get_proxy_info.return_value = {
            "username": "testuser"}
        self.assertEqual(get_dirac_user(), "testuser")

    @mock.patch("b2luigi.batch.processes.gbasf2.get_proxy_info")
    @mock.patch("b2luigi.batch.processes.gbasf2.setup_dirac_proxy")
    def test_get_dirac_user_ensures_proxy_initialized(self, mock_setup_dirac_proxy, mock_get_proxy_info):
        mock_get_proxy_info.return_value = {"username": "testuser"}
        get_dirac_user()
        self.assertEqual(mock_setup_dirac_proxy.call_count, 1)
