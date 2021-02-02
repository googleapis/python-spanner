Table Admin
===========

After creating an :class:`~google.cloud.spanner_v1.database.Database`, you can
interact with individual tables for that instance.


List Tables
-----------

To iterate over all existing tables for an database, use its
:meth:`~google.cloud.spanner_v1.database.Database.list_tables` method:

.. code:: python

    for table in database.list_tables():
        # `table` is a `Table` object.

This method yields :class:`~google.cloud.spanner_v1.table.Table` objects.


Constructing a Table
--------------------

A table object can be created by using the
:class:`~google.cloud.spanner_v1.table.Table` constructor. Since table
operations are executed via SQL, a
:class:`~google.cloud.spanner_v1.database.Database` instance is required.

.. code:: python

    table = google.cloud.spanner_v1.table.Table(
       "my_table_id", database
    )

Getting the Table Schema
------------------------

Use the :meth:`~google.cloud.spanner_v1.table.Table.get_schema` method to
inspect the columns of a table as a list of
:class:`~google.cloud.spanner_v1.types.Field` objects.

.. code:: python

    for field in table.get_schema():
        # `field` is a `Field` object.
