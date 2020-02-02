b2luigi
=======

.. image:: https://readthedocs.org/projects/b2luigi/badge/?version=latest&style=flat-sphare
           :target: https://b2luigi.readthedocs.io/en/stable/
.. image:: https://badge.fury.io/py/b2luigi.svg
           :target: https://badge.fury.io/py/b2luigi
.. image:: https://coveralls.io/repos/github/nils-braun/b2luigi/badge.svg?branch=master
           :target: https://coveralls.io/github/nils-braun/b2luigi?branch=master
.. image:: https://travis-ci.org/nils-braun/b2luigi.svg?branch=master
           :target: https://travis-ci.org/nils-braun/b2luigi


``b2luigi`` is a helper package constructed around ``luigi``, that helps you schedule working packages (so called tasks)
locally or on the batch system.
Apart from the very powerful dependency management system by ``luigi``, ``b2luigi`` extends the user interface
and has a build-in support for the queue systems, e.g. LSF.

You can find more information in the `documentation <https://b2luigi.readthedocs.io/en/latest/>`_.

Please note, that most of the core features are handled by ``luigi``, so you might want to have a look into
the `luigi documentation <https://luigi.readthedocs.io/en/latest/>`_.

If you find any bugs or want to add a feature or improve the documentation, please send me a pull request!

This project is in still beta. Please be extra cautious when using in production mode.
