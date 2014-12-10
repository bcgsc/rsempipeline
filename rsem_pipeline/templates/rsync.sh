#! /bin/bash

cd {{local_top_outdir}}

echo "hostname: $(hostname)"
echo "current dir: ${PWD}"

set -euo pipefail

GSMS_TO_TRANSFER="\
{% for gsm in gsms_to_transfer %}{% if loop.last %}{{gsm}}{% else %}{{gsm}} \
{% endif %}{% endfor %}"

dest_parent={{username}}@{{hostname}}:{{remote_top_outdir}}

echo "Job started at: $(date)"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

# -R is important for creating the same directory hierachy on remote host
cmd="rsync -R -r -a -v -h --stats --progress $GSMS_TO_TRANSFER $dest_parent --include='*.fastq.gz' --include='0_submit.sh' --exclude='GSM*[0-9]/*'"
echo "$cmd"
eval "$cmd"

# RC: return code
RSYNC_RC=$?
echo "rsync returncode: $RSYNC_RC"

if [ "$RSYNC_RC" -eq 0 ]; then
    echo 'do submission'
    ssh -l {{username}} {{hostname}} \
	". ~/.bash_profile; gsms_to_transfer=\"${GSMS_TO_TRANSFER}\"; cd {{remote_top_outdir}};" \
	'
        pwd=${PWD}
        for gsm in ${gsms_to_transfer}; do
	    cd ${gsm}
            qsub 0_submit.sh
            cd ${pwd}
        done
        '
fi

# remove fastq.gz files after transfer to save spaces
if [ "$RSYNC_RC" -eq 0 ]; then
    for i in ${GSMS_TO_TRANSFER}; do
	find $i -name '*.fastq.gz' -exec rm -fv '{}' ';'
	find $i -name '*.sra' -exec rm -fv '{}' ';'
    done
fi

echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "Job ended at:   $(date)"
