#!/usr/bin/env python2

"""
Script that uses gbasf2 code to produce job status dictionary.  Can only be
executed in a gbasf2 environment with python2.  It is therefore called by the
gbasf2 batch process in a subprocess.

Adapted from some example code by Michel Villanueva in
https://questions.belle2.org/question/7463/best-way-to-programatically-check-if-gbasf2-job-is-done/
"""
from __future__ import print_function

import argparse
import os
import json

from BelleDIRAC.gbasf2.lib.job.information_collector import InformationCollector
from BelleDIRAC.Client.helpers.auth import userCreds


@userCreds
def get_job_status_dict(project_name, user_name):
    if user_name is None:
        user_name = os.getenv("BELLE2_USER")
    login = [user_name, 'belle']
    newer_than = '1970-01-01'
    projects = [project_name]
    info_collector = InformationCollector()
    result = info_collector.getAllJobsInProjects(
        projects, date=newer_than, login=login, statuses={'Status': ['all']})
    project_items = result['Value'][projects[0]]
    status = info_collector.getJobSummary(project_items)
    if not status['OK']:
        raise Exception('info_collector returned with status', status["OK"])
    return status['Value']


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--project', type=str, required=True, help="gbasf2 project name")
    parser.add_argument('-u', '--user', type=str, default=None, help="grid username")
    args = parser.parse_args()
    job_status_dict = get_job_status_dict(args.project, args.user)
    print(json.dumps(job_status_dict))
