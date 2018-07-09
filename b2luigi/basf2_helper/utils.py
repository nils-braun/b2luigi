import os

import git


def get_basf2_git_hash():
    basf2_release = os.getenv("BELLE2_RELEASE")

    assert basf2_release

    if basf2_release == "head":
        basf2_release_location = os.getenv("BELLE2_LOCAL_DIR")

        assert basf2_release_location
        return git.Repo(basf2_release_location).head.object.hexsha

    return basf2_release