rsempipeline
========================

(The pipeline is currently being packaged and buggy, and will be fixed in 2015,
Jan).

rsempipeline is a pipeline for analyzing GEO data using `RSEM
<http://deweylab.biostat.wisc.edu/rsem/>`_. Typical analysis process is as
follows:

The input to the pipeline are mainly from two resources,

- soft files for all Series (aka. GSE)
- A GSE_species_GSM.csv file which contains a list of all interested Samples
  (aka. GSM) to be processed

Three steps are included in the pipeline:

1. Download the sra files for all GSMs from `GEO website
   <http://www.ncbi.nlm.nih.gov/geo/>`_ using aspc or wget (in case the first
   fail). aspc and wget use different urls which are linked to duplicates of the
   same file.

2. sra files are converted to fastq.gz files using fastq-dump from `SRA Toolkit
   <http://www.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?view=software>`_

3. Run rsem-calculate-expression from `RSEM
   <http://deweylab.biostat.wisc.edu/rsem/>`_ package with all fastq.gz files
   for all GSMs

The pipeline is designed to run the first two steps (computationally cheap) on
a local computer. Step 3 (computational intensive) is run on a computer cluster
(e.g. genesis, westgrid cluster).

Typically, about 100 GSEs and a few thousands of GSMs are picked by our
collaborators and grouped into a batch. Step 1 and 2 are done on in a
sub-batch-by-sub-batch fashion where all GSMs are processed in parallel until
finished. The way each sub-batch of GSMs are selected according to their file
sizes (mainly sra and resultant fastq.gz files) and currently available disk
space on the localhost as specified in a config file. After the first two step,
a submission script will be generated for each GSM, and at Step 3 a new job
will be submitted to the cluster for processing the GSM using RSEM. A control
mechanism has also implemented to avoid overuse the cluster resources such as
compute nodes and disk space. The first two steps are run by the command
``rp-run`` while the job script generation and job submission is handled by the
command ``rp-transfer``.

..
   It will create all folders for all GSMs according to a designated structure,
   i.e. ``<GSE>/<Species>/<GSM>``, and then fetch information of the sra files for
   each GSM from `NCBI FTP server <ftp://ftp-trace.ncbi.nlm.nih.gov/>`_ "NCBI FTP
   server"), and then save it to a file named `sras_info.yaml` in each GSM
   directory. The fetching process will take a while depending on how many GSMs to
   be processed.

..
   3. It will filter the samples generated from Step 1 and generate a sublist of
   samples that will be processed right away based on the sizes of sra files and
   estimated fastq.gz files (~1.5x) as well as the sizes available to use as
   specified in the ``rp_config.yml`` (mainly ``LOCAL_MAX_USAGE``,
   ``LOCAL_MIN_FREE``). Processed files will be saved to a file named
   ``sra2fastqed_GSMs.txt``.

..

For installation and usage instructions, please refer to ``INSTALL.rst`` and
``USAGE.rst``.

If you have found any bugs, questions, comments, please contact Zhuyi Xue
(zxue@bcgsc.ca).
