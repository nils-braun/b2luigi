import os


def get_basf2_git_hash():
    basf2_release = os.getenv("BELLE2_RELEASE")

    if basf2_release == "head" or basf2_release is None:

        try:
            from basf2.version import get_version
            basf2_hash = get_version
        except ImportError:
            from basf2.version import version
            basf2_hash = version

        basf2_release = basf2_hash()

    return basf2_release
