.. _installation-label:

Installation
============

This installation description is for the general user. If you are using the Belle II software, see below:

1.  Setup your local environment.
    For example, run:

    .. code-block:: bash

        source venv/bin/activate

2.  Install b2luigi from pipy into your environment.

    a.  If you have a local installation, you can use the normal setup command

    .. code-block:: bash

        python -m pip install b2luigi


    b.  If this fails because you do not have write access to where your virtual environment lives, you can also install
        b2luigi locally:

    .. code-block:: bash

        python -m pip install --user b2luigi

    This will automatically also install `luigi` into your current environment.
    Please make sure to always setup your environment correctly before using `b2luigi`.

Now you can go on with the :ref:`quick-start-label`.


b2luigi and Belle II
---------------------

1.  Setup your local environment. You can use a local environment (installed on your machine) or a release on cvmfs.
    For example, run:

    .. code-block:: bash

        source /cvmfs/belle.cern.ch/tools/b2setup prerelease-02-00-00c

    Or you setup your local installation

    .. code-block:: bash

        cd release-directory
        source tools-directory/b2setup

2.  Install b2luigi from pipy into your environment.

    a.  If you have a local installation, you can use the normal setup command

    .. code-block:: bash

        python -m pip install b2luigi --upgrade


    b.  If you are using an installation from cvmfs, you need to add the ``user`` flag.

    .. code-block:: bash

        python -m pip install --user b2luigi --upgrade


.. attention::
    The examples in this documentation are all shown with calling ``python``,
    assuming this refers to the *Python 3* executable of their (virtual) environment.
    In some systems and e.g. basf2 environments, ``python`` refers to Python 2
    (not supported by b2luigi). Then, ``python3`` should be used instead.

Please also have a look into the :ref:`basf2-example-label`.
