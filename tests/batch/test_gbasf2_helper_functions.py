"""
Test helper functions for :py:class:`Gbasf2Process`.

Utilities that require a running gbasf2 or Dirac environment will not net
tested in this test case, only the functions that can be run independently.
"""

import unittest

from b2luigi.batch.processes.gbasf2 import (_get_lfn_upto_reschedule_number, get_unique_lfns,
                                            lfn_follows_gb2v5_convention)

# first test utilities for working with logical file names on the grid


class TestLFNFollowsGbasf2V5Convention(unittest.TestCase):
    def test_newstyle_lfn_is_true(self):
        lfn = 'Upsilon4SBpcandee_00000_job181817516_03.root'
        self.assertTrue(lfn_follows_gb2v5_convention(lfn))

    def test_oldstyle_lfn_is_false(self):
        lfn = 'my_ntuple_0001.root'
        self.assertFalse(lfn_follows_gb2v5_convention(lfn))


class TestLFNUptoRescheduleNumber(unittest.TestCase):
    def test_strings_equal_camelcase_lfn(self):
        lfn = 'Upsilon4SBpcandee_00000_job181817516_03.root'
        self.assertEqual(_get_lfn_upto_reschedule_number(lfn), 'Upsilon4SBpcandee_00000_job181817516')

    def test_strings_equal_snakecase_lfn(self):
        lfn = 'my_ntuple_name_00000_job181817516_03.root'
        self.assertEqual(_get_lfn_upto_reschedule_number(lfn), 'my_ntuple_name_00000_job181817516')

    def test_oldstyle_lfn_raises_error(self):
        lfn = 'my_ntuple_0001.root'
        with self.assertRaises(ValueError):
            _get_lfn_upto_reschedule_number(lfn)


class TestGetUniqueLFNS(unittest.TestCase):
    def setUp(self):
        # list of logical path names on the grid that only differe by the reschedule number (last two digits)
        self.lfns_with_duplicates = [
            'Upsilon4SBpcandee_00000_job181817516_00.root',
            'Upsilon4SBpcandee_00001_job181817517_00.root',
            'Upsilon4SBpcandee_00002_job181817518_00.root',
            'Upsilon4SBpcandee_00003_job181817519_00.root',
            'Upsilon4SBpcandee_00004_job181817520_00.root',
            'Upsilon4SBpcandee_00005_job181817521_00.root',
            'Upsilon4SBpcandee_00006_job181817522_00.root',
            'Upsilon4SBpcandee_00007_job181817523_00.root',
            'Upsilon4SBpcandee_00008_job181817524_00.root',
            'Upsilon4SBpcandee_00009_job181817525_00.root',
            'Upsilon4SBpcandee_00010_job181817526_00.root',
            'Upsilon4SBpcandee_00011_job181817527_00.root',
            'Upsilon4SBpcandee_00012_job181817528_00.root',
            'Upsilon4SBpcandee_00012_job181817528_00.root',
            'Upsilon4SBpcandee_00012_job181817528_02.root',
            'Upsilon4SBpcandee_00013_job181817529_00.root',
            'Upsilon4SBpcandee_00014_job181817530_00.root',
            'Upsilon4SBpcandee_00015_job181817531_00.root',
            'Upsilon4SBpcandee_00016_job181817532_00.root',
            'Upsilon4SBpcandee_00017_job181817533_00.root',
            'Upsilon4SBpcandee_00017_job181817533_01.root',
            'Upsilon4SBpcandee_00017_job181817533_02.root',
            'Upsilon4SBpcandee_00018_job181817534_01.root',
            'Upsilon4SBpcandee_00019_job181817535_00.root',
        ]
        # create same input but with more underscores in initial root file name (snake case)
        # to test whether the string splitting logic is stable in that case
        self.lfns_with_duplicates_snake_case = [
            lfn.replace("Upsilon4SBpcandee", "u4s_Bp_cand_") for lfn in self.lfns_with_duplicates
        ]
        self.unique_lfns = {
            'Upsilon4SBpcandee_00000_job181817516_00.root',
            'Upsilon4SBpcandee_00001_job181817517_00.root',
            'Upsilon4SBpcandee_00002_job181817518_00.root',
            'Upsilon4SBpcandee_00003_job181817519_00.root',
            'Upsilon4SBpcandee_00004_job181817520_00.root',
            'Upsilon4SBpcandee_00005_job181817521_00.root',
            'Upsilon4SBpcandee_00006_job181817522_00.root',
            'Upsilon4SBpcandee_00007_job181817523_00.root',
            'Upsilon4SBpcandee_00008_job181817524_00.root',
            'Upsilon4SBpcandee_00009_job181817525_00.root',
            'Upsilon4SBpcandee_00010_job181817526_00.root',
            'Upsilon4SBpcandee_00011_job181817527_00.root',
            'Upsilon4SBpcandee_00012_job181817528_02.root',
            'Upsilon4SBpcandee_00013_job181817529_00.root',
            'Upsilon4SBpcandee_00014_job181817530_00.root',
            'Upsilon4SBpcandee_00015_job181817531_00.root',
            'Upsilon4SBpcandee_00016_job181817532_00.root',
            'Upsilon4SBpcandee_00017_job181817533_02.root',
            'Upsilon4SBpcandee_00018_job181817534_01.root',
            'Upsilon4SBpcandee_00019_job181817535_00.root',
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
        unique_lfns_snake_case = get_unique_lfns(set(self.lfns_with_duplicates_snake_case))
        self.assertSetEqual(unique_lfns_snake_case, self.unique_lfns_snake_case)
