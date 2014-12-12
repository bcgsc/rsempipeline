Preprocessing
--------------------

Preprocessing is done by ``rp-prep``, which has three sub-commands:

- ``find-dup``
- ``gen-csv``
- ``get-soft``

Type ``rp-prep -h`` for more help.


1. Generate ``GSE_GSM.csv`` in the following format. Note: don't include the
   header row; space after ``;`` will be ignored. Lines starting with ``#`` are
   skipped as comments.

   ::

      GSExxxxx,GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx
      GSExxxxx,GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx
      ...

   The following entries will be labeld as invalid.

   ::

      GSE45732, xGSM1113298
      a,b,c

2. Detect duplicated GSMs in an individual GSE. e.g.

   ::

       rp-prep find-dup -f GSE_GSM.csv

   If there are duplicates,, please remove them from ``GSE_GSM.csv``, and rerun
   ``rp-prep find-dup`` to confirm no duplicates are left.

3. Generate ``GSE_species_GSM.csv``. Species information is obtained by
   downloading and parsing the webpage for each GSM on `GEO website
   <http://www.ncbi.nlm.nih.gov/geo/>`__. e.g.

   ::

       rp-prep gen-csv -f GSE_GSM.csv --nt 8

   Check if any species is out of interest in the generated
   ``GSE_species_GSM.csv``. One way to do so is

   ::

       cat GSE_species_GSM.csv | tr ' ' '_' | tr ',' ' '| awk '{print $2}' | sort | uniq

   If there are species out of interest, make sure to remove the corresponding
   GSEs and GSMs from the ``GSE_GSM.csv``, and rerun ``rp-prep gen-csv`` with
   the updated ``GSE_GSM.csv`` to regenerate ``GSE_species_GSM.csv``.

4. Download soft files for all GSEs. The soft
   file contains all metadata about a particular GSE. The structure and content
   of SOFT format can be found `here
   <http://www.ncbi.nlm.nih.gov/geo/info/soft.html#format>`_. e.g.:

   ::

       rp-dup get-soft -f GSE_GSM.csv --outdir dir_to_batchx


Pipeline Setup:
------------------------

1. Prepare the ``rp_config.yml``, an example cofig file can be found in
   ``rsem_pipeline/conf/share``.


2. Automate the pipeline run with two cron jobs (``rp-run``, ``rp-transfer``,
   respectively) that look like the following examples. ``venv`` is the name of
   the virtual environment created during installation, ``soft`` is where softs
   files are located:

   ::

       */15 * * * *  . path/to/venv/bin/activate; cd path/to/top_outdir; rp-run -s soft/* -i GSE_species_GSM.csv -T gen_qsub_script -j 7  --qsub_template 0_submit_genesis.jinja2

   ::

       */20 * * * *  . path/to/venv/bin/activate; cd path/to/top_outdir; rp-transfer -s soft/* -i GSE_species_GSM.csv

3. Reference for running the pipeline mannually (not recommended)

   - Download sra files and convert them to fastq.gz files:

   ::

      rp-run -s path/to/soft/* -i GSE_species_GSM.csv -T sra2fastq  -j 7

   - Utilizing existing fastq.gz files by touching them only without reruning
     the sra2fastq task in the pipeline

   ::

      rp-run -s path/to/soft/* -i GSE_species_GSM.csv -T gen_qsub_script -j 7 --touch_files_only

   - Force to rerun tasks (e.g. ``gen_qsub_script``, when the template has been
     updated after last run of ``gen_qsub_script``)

   ::

      rp-run -s path/to/soft/* -i GSE_species_GSM.csv  --qsub_template 0_submit_genesis.jinja2 -T gen_qsub_script -j 7 --forced_tasks gen_qsub_script 

   - Fixing died GSMs locally on big memory nodes
     
   ::

      rp-run -s path/to/soft/* -i 'GSE43631 GSM1067318 GSM1067319' -T rsem -j 2


4. Set up a web application to monitor the progress. This step needs a separate
   package ``rsem_report``, which is built with `Django
   <https://www.djangoproject.com/>`_, and planned to be merged into this
   package in the future. The setup instruction will come later.
