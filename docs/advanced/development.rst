.. _development-label:

Development and TODOs
=====================

You want to help developing ``b2luigi``? Great! Have your github account ready and let's go!


Local Development
-----------------

You want to help developing ``b2luigi``? Great! Here are some first steps to help you dive in:

1.  Make sure you uninstall ``b2luigi`` if you have installed if from pypi

    .. code-block:: bash

        python -m pip uninstall b2luigi

2.  Clone the repository from github

    .. code-block:: bash

        git clone https://github.com/nils-braun/b2luigi

3.  ``b2luigi`` is not using ``setuptools`` but the newer (and better) flit_ as a a builder.
    Install it via

    .. code-block:: bash

        python -m pip [ --user ] install flit

    You can now install ``b2luigi`` from the cloned git repository in development mode:

    .. code-block:: bash

        flit install -s

    Now you can start hacking and your changes will be immediately available to you.

4. Install `pre-commit`_, which automatically checks your code

    .. code-block:: bash

        python -m pip [ --user ] install pre-commit
        pre-commit install  # install the pre-commit hooks
        pre-commit  # run pre-commit manually, checks all staged ("added") changes

   In particular, the python files are checked with `flake8`_ for syntax and
   `PEP 8`_ style errors. I recommend using an IDE or editor which
   automatically highlights errors with flake8 or a similar python linter (e.g.
   pylint).

5. We use the unittest_ package for testing some parts of the code. All tests reside in the
   ``tests/`` sub-directory. To run all tests, run the command

    .. code-block:: bash

        python -m unittest

   in the root of ``b2luigi`` repository. If you add some functionality, try to add some tests for it.

6.  The documentation is hosted on `readthedocs`_ and build automatically on every commit to main.
    You can (and should) also build the documentation locally by installing ``sphinx``

    .. code-block:: bash

        python -m pip [ --user ] install sphinx sphinx-autobuild

    And starting the automatic build process in the projects root folder

    .. code-block:: bash

        sphinx-autobuild docs build

    The autobuild will rebuild the project whenever you change something. It displays a URL where to find
    the created docs now (most likely http://127.0.0.1:8000).
    Please make sure the documentation looks fine before creating a pull request.

7.  Add a summary of your changes to the ``[Unreleased]`` section of the ``CHANGELOG.md``.

8.  If you are a core developer and want to release a new version:

    a.  Make sure all changes are committed and merged on main
    b.  Use the `bump-my-version`_ package to update the version in the python file ``b2luigi/__init__.py`` as well
        as the git tag. ``flit`` will automatically use this.

        .. code-block:: bash

            bump-my-version [patch|minor|major]

    c.  Push the new commit and the tags

        .. code-block:: bash

            git push
            git push --tags

    d.  Update the ``CHANGELOG.md`` following the `Keep a Changelog`_ format.

    e.  Create a new `release`_ on github, with the description copied from the ``CHANGELOG.md``.

    f. Check that the new release had been published to PyPi, which should happen automatically via
       github `actions`_. Alternatively, you can also manually publish a release via

        .. code-block:: bash

            flit publish


Open TODOs
----------

For a list of potential features, improvements and bugfixes see the `github issues`_. Help is
welcome, so feel free to pick one, e.g. with the ``good first issue`` or ``help wanted`` tags.

.. _flit: https://pypi.org/project/flit/
.. _github issues: https://github.com/nils-braun/b2luigi/issues
.. _unittest: https://docs.python.org/3/library/unittest.html
.. _readthedocs: https://readthedocs.org
.. _pre-commit: https://pre-commit.com
.. _flake8: https://flake8.pycqa.org
.. _PEP 8: https://www.python.org/dev/peps/pep-0008/
.. _bump-my-version: https://github.com/callowayproject/bump-my-version
.. _release: https://github.com/nils-braun/b2luigi/releases
.. _actions: https://github.com/nils-braun/b2luigi/actions
.. _Keep a Changelog: https://keepachangelog.com/en/1.0.0/
