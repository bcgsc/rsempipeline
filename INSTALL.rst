Installation
--------------------

On localhost
^^^^^^^^^^^^^^^^^^^^

Please install the following packages from their source or use your
favouriate package manager, and put their executables in ``PATH``.

- `Python-2.7.x <https://www.python.org/download/releases/2.7/>`_
- `wget <http://ftp.gnu.org/gnu/wget/>`_ (in case ascp fails, wget will try to download the same file from a different url)
- `Aspera ascp <http://download.asperasoft.com/download/docs/ascp/2.6/html/index.html>`_
- fastq-dump from `SRA Toolkit <http://www.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?view=software>`_
- `bowtie <http://bowtie-bio.sourceforge.net/index.shtml>`_ (dependency of
  RSEM) and `RSEM <http://deweylab.biostat.wisc.edu/rsem/>`_ (only if you're
  going to run rsem step no localhost, as well)

Then install the pipeline,

.. code-block:: bash

    $ pip install virtualenv
    $ virtualenv <DIR>
    $ source <DIR>/bin/activate
    $ # current
    $ pip install pip install git+https://github.com/bcgsc/rsem-pipeline.git@setuptools#egg=rsem-pipeline
    $ # future
    $ # pip install pip install git+https://github.com/bcgsc/rsem-pipeline.git#egg=rsem-pipeline

On remote cluster
^^^^^^^^^^^^^^^^^^^^

Please install the following packages

- `bowtie <http://bowtie-bio.sourceforge.net/index.shtml>`_ (dependency of
  RSEM) and `RSEM <http://deweylab.biostat.wisc.edu/rsem/>`_ 


