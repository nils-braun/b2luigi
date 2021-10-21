b2luigi
=======

.. image:: https://img.shields.io/readthedocs/b2luigi
           :target: https://b2luigi.readthedocs.io/en/stable/
.. image:: https://img.shields.io/github/license/nils-braun/b2luigi
           :target: https://github.com/nils-braun/b2luigi/blob/main/LICENSE
.. image:: https://img.shields.io/codecov/c/github/nils-braun/b2luigi?logo=codecov
           :target: https://codecov.io/gh/nils-braun/b2luigi
.. image:: https://img.shields.io/github/workflow/status/nils-braun/b2luigi/Test%20Default%20Branch?logo=github
           :target: https://github.com/nils-braun/b2luigi/actions
.. image:: https://img.shields.io/pypi/v/b2luigi?logo=pypi
           :target: https://pypi.python.org/pypi/b2luigi/


``b2luigi`` is a helper package constructed around ``luigi``, that helps you schedule working packages (so called tasks)
locally or on the batch system.
Apart from the very powerful dependency management system by ``luigi``, ``b2luigi`` extends the user interface
and has a build-in support for the queue systems, e.g. LSF.

You can find more information in the `documentation <https://b2luigi.readthedocs.io/en/stable/>`_.

Please note, that most of the core features are handled by ``luigi``, so you might want to have a look into
the `luigi documentation <https://luigi.readthedocs.io/en/latest/>`_.

If you find any bugs or want to add a feature or improve the documentation, please send me a pull request!

This project is in still beta. Please be extra cautious when using in production mode.

To get notified about new features, (potentiall breaking) changes, bugs and
their fixes, I recommend using the ``watch`` button on github to get
notifications for new releases and/or issues or to subscribe the `releases feed
<https://github.com/nils-braun/b2luigi/releases.atom>`_ (requires no github
account, just a `feed reader <https://en.wikipedia.org/wiki/Web_feed>`_.
