.. _installation-label:

Installation
============

1.  Setup your local environment. If you are using basf2: You can use a local environment (installed on your machine)
    or a release on cvmfs.
    For example, run:

    .. code-block:: bash

        source /cvmfs/belle.cern.ch/tools/b2setup prerelease-02-00-00c

    Or you setup the virtual environment of your project:

    .. code-block:: bash

        source venv/bin/activate

2.  Install b2luigi from pipy into your environment.

    a.  If you have a local installation, you can use the normal setup command

    .. code-block:: bash

        pip3 install b2luigi


    b.  If this fails because you do not have write access to where your virtual environment lives, you can also install
        b2luigi locally:

    .. code-block:: bash

        pip3 install --user b2luigi


    This will automatically also install `luigi` into your current environment.
    Please make sure to always setup your environment (e.g. basf2) correctly before using `b2luigi`.

Now you can go on with the :ref:`quick-start-label`.