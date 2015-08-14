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

