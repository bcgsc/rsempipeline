Setting up development mode
--------------------

.. code-block:: bash

   git clone git@github.com:bcgsc/rsempipeline.git
   
   cd rsempipeline

   virtualenv venv

   . venv/bin/activate

   pip install -U setuptools pip

   python setup.py develop

   # Run test, for coverage information, see index.html in ``htmlcov``.
   py.test --cov=rsempipeline --cov-report=html tests/

For more information, see `setuptools page <https://pythonhosted.org/setuptools/setuptools.html#develop-deploy-the-project-source-in-development-mode>`__.

Add unit tests
--------------------

Add new tests to the ``tests`` directory


``transferred_GSMs.txt`` vs ``transferred.COMPLETE``
-------------------------------------------------------------------------------
Used transferred_GSMs.txt instead of a transferred.COMPLETE for each GSM is
because I thought it would be cumbersome to qsub each GSM in python after
transfer is complete for each GSM. But it may still work and would be more
granular to transfer and qsub each GSM in Python. In the current
implementation, the qsub doesn't happen till the transfer is assured
successful, and rsync is idempotent before qsub happens.

