# Preprocessing Steps:

(The scripts during the preprocessing steps are in the preprocessing_tools folder.)

1. Generate `GSE_GSM.csv` in the following format 
 
	```
	GSExxxxx,GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx
	GSExxxxx,GSMxxxxxxx; GSMxxxxxxx; GSMxxxxxxx
	...
	```
	
2. Run `gen_GSE_species_GSM_csv.py` against `GSE_GSM.csv` to generate
`GSE_species_GSM.csv`, and check if any species is out of interest. Species
information is fetched from the webpage of each GSM on
[GEO website](http://www.ncbi.nlm.nih.gov/geo/ "GEO website"), a html directory
will be created to keep records of all htmls downloaded. Example command (check
`-h` for more help):

	```
	python gen_GSE_species_GSM_csv.py -f dir_to_batchx/GSE_GSM.csv --nt 18
	```
	
3. Run `detect_duplicate_GSMs.py` against `GSE_GSM.csv` to check for possible
duplications of GSMs within one GSE. Example command:

	```
	python detect_duplicate_GSMs.py -f dir_to_batchx/GSE_GSM.csv
	```
		
4. Remove GSEs from species that are out of interest (from Step 2) and
duplicated GSMs (from Step 3) from `GSE_GSM.csv`.

5. Rerun `gen_GSE_species_GSM_csv.py` against the updated `GSE_GSM.csv` to
generate an updated version of `GSE_species_GSM.csv`.

6. Rerun `detect_duplicate_GSMs.py` against the updated `GSE_GSM.csv` to make
sure no duplications are left.

7. Run download_soft.py against updated `GSE_species_GSM.csv` to download soft
files to a soft directory. Example commnad:

	```python download_soft.py -f GSE_species_GSM.csv  --out_dir dir_to_batchx/```
			
8. Remove htmls of unwanted GSEs in the htmls directory to save some
space. [Optional]
			
# Running the pipeline to generate fastq.gz files:

(further work is underway to automate this step without exhausting the disk space)

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

# Running rsem:

* prepare a `rsem_pipeline_config.yaml`  in /local/path/to/top_outdir based on the `rsem_pipeline_config.yaml.template` that comes with this package

* create a cron job in the following pattern
*/15 * * * * . /local/path/to/rsem_pipeline/venv/bin/activate; cd /local/path/to/top_outdir; python /local/path/to/rsem_pipeline/rsem_cron_transfer.py


# Setting up a web server to monitor the progress

* This step needs a separate package `rsem_report`, which is built with [Django](https://www.djangoproject.com/, "Django")
