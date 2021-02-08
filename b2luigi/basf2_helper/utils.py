import os

import git


def get_basf2_git_hash():
    basf2_release = os.getenv("BELLE2_RELEASE")

    if basf2_release == "head" or basf2_release is None:
        basf2_release_location = os.getenv("BELLE2_LOCAL_DIR")

        if basf2_release_location:
            return git.Repo(basf2_release_location).head.object.hexsha
        return "not_set"

    return basf2_release
