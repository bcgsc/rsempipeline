Setting up development mode
--------------------

.. code-block:: bash

   git clone git@github.com:bcgsc/rsempipeline.git
   
   cd rsempipeline

   virtualenv venv

   . venv/bin/activate

   pip install pytest pytest-cov mock testfixtures

   python setup.py develop

For more information, see `setuptools page <https://pythonhosted.org/setuptools/setuptools.html#develop-deploy-the-project-source-in-development-mode>`__.


Adding unit tests
--------------------

Add them to ``tests`` directory

Running test
--------------------

.. code-block:: bash

   py.test --cov=rsempipeline --cov-report=html tests/

For coverage information, see index.html in ``htmlcov``.


``transferred_GSMs.txt`` vs ``transferred.COMPLETE``
-------------------------------------------------------------------------------
Used transferred_GSMs.txt instead of a transferred.COMPLETE for each GSM is
because I thought it would be cumbersome to qsub each GSM in python after
transfer is complete for each GSM. But it may still work and would be more
granular to transfer and qsub each GSM in Python. In the current
implementation, the qsub doesn't happen till the transfer is assured
successful, and rsync is idempotent before qsub happens.
