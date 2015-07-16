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

Running test (28% coverage as of 2015-06-29)
--------------------

.. code-block:: bash

   py.test --cov=rsempipeline --cov-report=html tests/

For coverage information, see index.html in ``htmlcov``.
