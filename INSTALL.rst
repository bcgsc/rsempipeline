Installation
--------------------

On localhost
^^^^^^^^^^^^^^^^^^^^

Please install the following programs use your favouriate package manager or
from source, and put their executables in ``PATH``.

- `Python-2.7.x <https://www.python.org/download/releases/2.7/>`_
- `wget <http://ftp.gnu.org/gnu/wget/>`_ (in case ascp fails, wget will try to download the same file from a different url)
- `Aspera ascp <http://download.asperasoft.com/download/docs/ascp/2.6/html/index.html>`_
- fastq-dump from `SRA Toolkit <http://www.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?view=software>`_
- [optional] If you need to run rsem on the localhost, as well. For example,
  when doing the analysis on a small number of GSMs for testing purpose, install
  `bowtie <http://bowtie-bio.sourceforge.net/index.shtml>`_ (dependency of
  RSEM) and `RSEM <http://deweylab.biostat.wisc.edu/rsem/>`_, too.

Then install the pipeline,

.. code-block:: bash

    $ pip install virtualenv
    $ virtualenv <DIR>
    $ source <DIR>/bin/activate
    $ pip install git+https://github.com/bcgsc/rsempipeline.git#egg=rsempipeline

On remote cluster
^^^^^^^^^^^^^^^^^^^^

Please install the following packages

- `bowtie <http://bowtie-bio.sourceforge.net/index.shtml>`_ (dependency of
  RSEM)
- `RSEM <http://deweylab.biostat.wisc.edu/rsem/>`_ 

Example for setting up cron jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

. code-block::

*/30 * * * *  RSEM_DIR=/path/to/data/batchx; . /path/to/venv/bin/activate; cd ${RSEM_DIR};  rp-run -v 1 -s soft/* -i GSE_species_GSM.csv -T gen_qsub_script -j 7  --qsub_template 0_submit_genesis.jinja2 &> ~/rp-run.log
*/20 * * * *  RSEM_DIR=/path/to/data/batchx; . /path/to/venv/bin/activate; cd ${RSEM_DIR};  rp-transfer -s soft/* -i GSE_species_GSM.csv &> ~/rp-transfer.log

