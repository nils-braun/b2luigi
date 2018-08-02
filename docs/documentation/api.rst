.. _`api-documentation-label`:

API Documentation
=================

``b2luigi`` summarizes different topics to help you in your everyday task 
creation and processing.
Most important is the :meth:`b2luigi.process` function, which lets you run
arbitrary task graphs on the batch.
It is very similar to ``luigi.build``, but lets you hand in additional parameters
for steering the batch execution.

Top-Level Function
------------------

.. autofunction:: b2luigi.process

Super-hero Task Classes
-----------------------

If you want to use the default ``luigi.Task`` class or any derivative of it,
you are totally fine. 
No need to change any of your scripts!
But if you want to take advantage of some of the recipies we have developed
to work with large luigi task sets, you can use the drop in replacements
from the ``b2luigi`` package.
All task classes (except the :class:`b2luigi.DispatchableTask`) are superclasses of
a ``luigi`` class.
As we import ``luigi`` into ``b2luigi``, you just need to replace

.. code-block:: python

    import luigi

with

.. code-block:: python

    import b2luigi as luigi

and you will have all the functionality of ``luigi`` and ``b2luigi``
without the need to change anything!

.. autoclass:: b2luigi.Task
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: b2luigi.ExternalTask
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: b2luigi.WrapperTask
    :members:
    :undoc-members:
    :show-inheritance:

.. autofunction:: b2luigi.dispatch

.. autoclass:: b2luigi.DispatchableTask
    :members: process
    :show-inheritance:

Settings
--------

.. autofunction:: b2luigi.get_setting
.. autofunction:: b2luigi.set_setting


Other functions
---------------

.. toctree::
    :maxdepth: 1

    b2luigi.core.utils
    b2luigi.batch
    b2luigi.basf2_helper