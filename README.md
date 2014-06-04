Steps:

1. Prepare GSE\_GSM\_species.csv, check it to see its
format. GSE\_GSM\_species.json will be created to save time for parsing again.

2. You could run GSE\_GSM\_species\_stats.py to check simple stats (optional)

3. Run download\_soft.py to download all soft files

4. Run rsem\_pipline.py on all soft files

Todo:
1. write interface to apollo
2. Furnish test\_rsem\_pipeline.py
3. isolate hardcoded parts, num\_of\_threads, gen_cmds, etc.

```python rsem_pipeline.py -s sample_data/soft/GSE24455_family.soft.subset sample_data/soft/GSE35213_family.soft.subset sample_data/soft/GSE50599_family.soft.subset -f sample_data/sample_GSE_GSM_species.csv  --host-to-run local -n 7 --tasks download```
```python rsem_pipeline.py -s sample_data/soft/GSE24455_family.soft.subset sample_data/soft/GSE35213_family.soft.subset sample_data/soft/GSE50599_family.soft.subset -d "GSE24455 GSM602557 GSM602558; GSE35213 GSM863770" -o some_outdir --host-to-run local --tasks download ```
