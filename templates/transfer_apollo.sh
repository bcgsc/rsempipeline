#! /bin/bash

#$ -S /bin/bash
#$ -j y
#$ -q thosts.q
#$ -N {{job_name}}
#$ -o {{local_top_outdir}}
#$ -P transfer
#$ -m eas
#$ -M zxue@bcgsc.ca

cd {{local_top_outdir}}

echo "hostname: $(hostname)"
echo "temp dir: $TMPDIR"
echo ${PWD}

set -euo pipefail

GSMS_TO_TRANSFER="\
{% for gsm in gsms_to_transfer %}{% if loop.last %}{{gsm}}{% else %}{{gsm}} \
{% endif %}{% endfor %}"

source="$GSMS_TO_TRANSFER"
dest_parent=zxue@genesis.bcgsc.ca:{{remote_top_outdir}}

echo "Job started at: $(date)"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

echo "From: $source"
echo "To:   $dest_parent"

# -R is important for creating the same directory hierachy on remote host
cmd="rsync -R -r -a -v -h --stats --progress $source $dest_parent"
echo "$cmd"
eval "$cmd"

echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "Job ended at:   $(date)"

echo 'do submission'
if [ $? -eq 0 ]; then
    ssh -l zxue genesis \
	"gsms_to_transfer=\"${GSMS_TO_TRANSFER}\"; cd {{remote_top_outdir}};" \
	'
        pwd=${PWD}
        for gsm in ${gsms_to_transfer}; do
	    cd ${gsm}
            qsub 0_submit.sh
            cd ${pwd}
        done
        '
fi

