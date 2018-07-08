b2luigi
=======


``b2luigi`` is a helper package constructed around ``luigi``, that helps you schedule working packages (so called tasks)
locally or on the batch system.
Apart from the very powerful dependency management system by ``luigi``, ``b2luigi`` extends the user interface
and has a build-in support for the queue systems, e.g. LSF.

You can find more information in the :ref:`quick-start-label`.

Please note, that most of the core features are handled by ``luigi``, so you might want to have a look into
the `luigi documentation`_.

If you find any bugs or want to improve the documentation, please send me a merge request on github_.

This project is in beta. Please be extra cautious when using in production mode.
You can help me by working with one of the todo items described in :ref:`todo-label`.

Content
=======

.. toctree::
    :maxdepth: 2

    usage/installation
    usage/quickstart
    usage/batch
    advanced/examples
    advanced/faq
    advanced/todo
    advanced/development


.. _github: https://github.com/nils-braun/b2luigi
.. _`luigi documentation`: http://luigi.readthedocs.io/en/stable/