# DESCRIPTION of the analysis process

1. The code will read in from -s/--soft and -i/--isamp and generate a list of
samples to be processed.

2. It will create all folders for all GSMs according to a designated structure,
i.e. <GSE>/<Species>/<GSM>, and then fetch information of the sra files for
each GSM from [NCBI FTP server](ftp://ftp-trace.ncbi.nlm.nih.gov/ "NCBI FTP
server"), and then save it to a file named `sras_info.yaml` in each GSM
directory. The fetching process will take a while depending on how many GSMs to
be processed.

3. It will filter the samples generated from Step 1 and generate a sublist of
samples that will be processed right away based on the sizes of sra files and
estimated fastq.gz files (~1.5x) as well as the sizes available to use as
specified in the `rsem_pipeline_config.yaml` (mainly `LOCAL_MAX_USAGE`,
`LOCAL_MIN_FREE`). Processed files will be saved to a file named
`sra2fastqed_GSMs.txt`.

4. `rsem_cron_transfer` will transfer the GSMs in sra2fastqed_GSMs.txt to a
remote cluster and do rsem analysis there. transferred GSMs will be saved to
`transferred_GSMs.txt`


# INSTALLATION:

required softwares,
python-2.7, virtualenv, pip, ascp, wget, fastq-dump

```
$cd rsem_pipeline
$virtualenv venv
$. venv/bin/activate
$pip install git+https://github.com/bcgsc/rsem_pipeline.git@setuptools#egg=rsem_pipeline
```

# Preprocessing Steps:

(The scripts during the preprocessing steps are in the `preprocessing_tools `folder.)


1. **Make `GSE_GSM.csv`**
    - Generate `GSE_GSM.csv` in the following format. Note: don't include the
	header row; space after ; will be ignored. Lines starting with # indicate
	comments.

	```
	GSExxxxx,GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx
	GSExxxxx,GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx
	...
	```
	The code in the next step will check if the names of GSE and GSM are valid
	or not. For example, the following entries will be labeld as invalid.
	```
	GSE45732, xGSM1113298
	a,b,c
	```

2. **Detect duplicated GSMs**
    - Run `detect_duplicate_GSMs.py` against `GSE_GSM.csv` to check for
	possible duplications of GSMs within one GSE. Example command:

	```
	python detect_duplicate_GSMs.py -f dir_to_batchx/GSE_GSM.csv
	```

    - If there are duplicated GSMs detected in Step 2, please remove them from
	the `GSE_GSM.csv`, and rerun `detect_duplicate_GSMs.py` to make sure no
	duplications are left.

3. **Make `GSE_species_GSM.csv`**
    - Run `gen_GSE_species_GSM_csv.py` against the `GSE_GSM.csv` to generate
	`GSE_species_GSM.csv`. Species information is fetched from the webpage of
	each GSM on [GEO website](http://www.ncbi.nlm.nih.gov/geo/ "GEO website"),
	a html directory will be created to keep records of all htmls
	downloaded. Example command (check `-h` for more help):

	```
	python gen_GSE_species_GSM_csv.py -f dir_to_batchx/GSE_GSM.csv --nt 8
	```

    - Check if any species is out of interest in the generated
	`GSE_species_GSM.csv`. One way to do so is

    ```
	cat GSE_species_GSM.csv | tr ' ' '_' | tr ',' ' '| awk '{print $2}' | sort | uniq
	```
		
    - If there are species out of interest (from Step 4 & 5), remove the
	corresponding GSEs and GSMs from the `GSE_GSM.csv`.

    - Rerun `gen_GSE_species_GSM_csv.py` against the updated `GSE_GSM.csv` to
	generate an updated version of `GSE_species_GSM.csv`.

4. **Download soft files**
    - Run download_soft.py against updated `GSE_GSM.csv` to download soft
	files to a soft directory. Example commnad:

	```
	python download_soft.py -f GSE_GSM.csv  --out_dir dir_to_batchx/
	```
			
5. **Remove htmls (optional)**
    - Remove htmls of unwanted GSEs in the htmls directory to save some
	space. [Optional]
			
# Running the pipeline:

1. **Prepare the configuration file**
    * prepare a `rsem_pipeline_config.yaml` in path/to/top_outdir based on the
      `rsem_pipeline_config.yaml.template` that comes with this package.


2. **Automate the pipeline via cron jobs**
    * Create two cron jobs like the following, assuming `venv` is the virtual
      environment, and `soft/*` and `GSE_species_GSM.csv` are both located in
      `top_outdir`:

	```
	*/15 * * * *  . path/to/venv/bin/activate; cd path/to/top_outdir; python path/to/rsem_pipeline.py -s soft/* -i GSE_species_GSM.csv  -T gen_qsub_script -j 7  --qsub_template 0_submit_genesis.jinja2
	```
	```
	*/20 * * * *  . path/to/venv/bin/activate; cd path/to/top_outdir; python path/to/rsem_transfer.py -s soft/* -i GSE_species_GSM.csv
	```
	
3. **Reference for running the pipeline mannually**
	* Download sra files and convert them to fastq.gz files:

	```
	rsem_pipeline.py -s path/to/soft/* -i GSE_species_GSM.csv -T sra2fastq  -j 7
	```

	* Utilizing existing fastq.gz files by touching them only without reruning the sra2fastq task in the pipeline

	```
	rsem_pipeline.py -s path/to/soft/* -i GSE_species_GSM.csv -T gen_qsub_script -j 7 --touch_files_only
	```

	* Force to rerun tasks (e.g. gen_qsub_script, when the template has been updated after last run of gen_qsub_script)

	```
	rsem_pipeline.py -s path/to/soft/* -i GSE_species_GSM.csv  --qsub_template 0_submit_genesis.jinja2 -T gen_qsub_script -j 7 --forced_tasks gen_qsub_script 
	```

	* Fixing died GSMs locally on big memory nodes

	```
	rsem_pipeline.py -s path/to/soft/* -i 'GSE43631 GSM1067318 GSM1067319' -T rsem -j 2
	```

# Setting up a web application to monitor the progress
    * This step needs a separate package `rsem_report`, which is built with
      [Django](https://www.djangoproject.com/ "Django"). The setup instruction
      will come later.
