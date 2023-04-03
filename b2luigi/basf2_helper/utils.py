import os


def get_basf2_git_hash():
    """Return name of basf2 release or if local basf2 is used its version hash.

    The version is equivalent to the version returned by ``basf2 --version``.

    Returns ``\"not set\"``, if no basf2 release is set and basf2 cannot be imported.
    """
    basf2_release = os.getenv("BELLE2_RELEASE")

    if basf2_release not in ("head", None):
        return basf2_release
    # else if BASF2_RELEASE environment variable is not set, get version hash of
    # local basf2
    try:
        import basf2.version
        # try both get_version method and version attribute, one of which
        # should work depending on basf2 release
        if hasattr(basf2.version, "get_version"):
            return basf2.version.get_version()
        return basf2.version.version
    except ImportError:
        return "not_set"
