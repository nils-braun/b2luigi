.. _installation-label:

Installation
============

1.  Setup your basf2 environment. You can use a local environment (installed on your machine) or a release on cvmfs.
    For example, run:

    .. code-block:: bash

        source /cvmfs/belle.cern.ch/tools/b2setup prerelease-02-00-00c

2.  Install b2luigi from gitlab directly into your basf2 environment.

    a.  If you are using a central release from cvmfs, you need to do this into your user folder:

    .. code-block:: bash

        pip3 install --user git+https://github.com/nils-braun/b2luigi.git

    b.  If you have a local installation, you can also use the normal setup command

    .. code-block:: bash

        pip3 install git+https://github.com/nils-braun/b2luigi.git

This will automatically also install `luigi` into your current environment.
Please make sure to always setup basf2 correctly before using `b2luigi`.

Now you can go on with the :ref:`quick-start-label`.