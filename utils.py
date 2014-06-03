import os
import glob
import logging
import logging.config
import re
import subprocess
import csv
import json

import settings as S
logger = logging.getLogger('utils')

def backup_file(f):
    if os.path.exists(f):
        dirname = os.path.dirname(f)
        basename = os.path.basename(f)
        count = 1
        rn_to = os.path.join(
            dirname, '#' + basename + '.{0}#'.format(count))
        while os.path.exists(rn_to):
            count += 1
            rn_to = os.path.join(
                dirname, '#' + basename + '.{0}#'.format(count))
        logger.info("BACKING UP {0} to {1}".format(f, rn_to))
        os.rename(f, rn_to)
        return rn_to
        logger.info("BACKUP FINISHED")


def touch(fname, times=None):
    with file(fname, 'a'):
        os.utime(fname, times)


def read_csv_species_as_key(infile):
    """
    @param infile: input file, usually named GSE_GSM_species.csv
    """
    input_data = {}
    with open(infile, 'rb') as inf:
        csvreader = csv.reader(inf, delimiter='\t')
        for (gse, gsm, species) in csvreader:
            if species in input_data:
                if gse in input_data[species]:
                    input_data[species][gse].append(gsm)
                else:
                    input_data[species][gse] = [gsm]
            else:
                input_data[species] = {gse:[gsm]}
    return input_data

def read_csv_gse_as_key(infile):
    """
    @param infile: input file, usually named GSE_GSM_species.csv
    """
    input_data = {}
    with open(infile, 'rb') as inf:
        csvreader = csv.reader(inf, delimiter='\t')
        for (gse, gsm, species) in csvreader:
            if gse in input_data:
                input_data[gse].append(gsm)
            else:
                input_data[gse] = [gsm]
    return input_data

def gen_input_csv_data(input_csv, key):
    """
    generate input data with the specified data structure as shown below

    @param key: species or gse
    """
    
    # data structure of input_data with key=species
    # input_data = {
    #     'human': {gse1: [gsm1, gsm2, gsm3, ...],
    #               gse2: [gsm1, gsm2, gsm3, ...],
    #               gse3: [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     'mouse': {gse1: [gsm1, gsm2, gsm3, ...],
    #               gse2: [gsm1, gsm2, gsm3, ...],
    #               gse3: [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     'rat':   {gse1: [gsm1, gsm2, gsm3, ...],
    #               gse2: [gsm1, gsm2, gsm3, ...],
    #               gse3: [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     }

    # data structure of input_data with key=gse, species info ignored
    # input_data = {
    #     'gse1': {[gsm1, gsm2, gsm3, ...],
    #              [gsm1, gsm2, gsm3, ...],
    #              [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     'gse2': {[gsm1, gsm2, gsm3, ...],
    #              [gsm1, gsm2, gsm3, ...],
    #              [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     'gse3':  [gsm1, gsm2, gsm3, ...],
    #              [gsm1, gsm2, gsm3, ...],
    #              [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     }

    json_file = get_cache(input_csv, key)
    if cache_usable(json_file, input_csv):
        with open(json_file, 'rb') as inf:
            input_data = json.load(inf)
    else:
        if key == 'species':
            input_data = read_csv_species_as_key(input_csv)
        else:
            input_data = read_csv_gse_as_key(input_csv)
        with open(json_file, 'wb') as opf:
            json.dump(input_data, opf)
    return input_data


def get_cache(input_file, key):
    """cache the content of input_file as as json file for fast retrieval"""
    dirname, basename = os.path.split(input_file)
    if key == 'species':
        return gen_cache_file(dirname, basename, 'species_as_key.json')
    elif key == 'gse':
        return gen_cache_file(dirname, basename, 'gse_as_key.json')


def gen_cache_file(dirname, basename, suffix):
    return os.path.join(
        dirname, '.{0}'.format(
            # withuot lstrip('.'), there can be to dots, e.g.
            # ..GSE_GSM_species_GSE24455_GSM602557_human.gse_as_key.json,
            # which is not pretty
            basename.lstrip('.').replace('csv', suffix)))
    

def cache_usable(cache_file, *ref_files):
    f_cache_usable = True
    if os.path.exists(cache_file):
        logger.info('{0} exists'.format(cache_file))
        if cache_up_to_date(cache_file, *ref_files):
            logger.info('{0} is up to date. '
                        'reading outputs from cache'.format(cache_file))
        else:
            logger.info('{0} is outdated'.format(cache_file))
            f_cache_usable = False
    else:
        logger.info('{0} doesn\'t exist'.format(cache_file))
        f_cache_usable = False
    return f_cache_usable


def cache_up_to_date(cache_file, *ref_files):
    for _ in ref_files:
        if (os.path.getmtime(cache_file) < os.path.getmtime(_) or
            # ctime: e.g. when renaming test.bk to test changes information in
            # inode
            os.path.getctime(cache_file) < os.path.getctime(_)):
            return False
    return True


def gen_sample_msg_id(sample):
    """
    used as an id to identify a particular sample for each logging message
    """
    return '{0} ({2}/{3}) of {1}'.format(
        sample.name, sample.series.name, 
        sample.index, sample.series.num_passed_samples())


def gen_sra_msg_id(sra):
    sample = sra.sample
    series = sample.series
    return '{0} ({1}/{2}) of {3} ({4}/{5}) of {6}'.format(
        sra.name, sra.index, sample.num_sras(), 
        sample.name, sample.index, series.num_passed_samples(),
        series.name)


def execute(cmd, msg_id='', flag_file=None):
    logger.info('executing CMD: {0}'.format(cmd))
    try:
        returncode = subprocess.call(cmd, shell=True, executable="/bin/bash")
        if returncode != 0:
            logger.error(
                '{0}, started, but then failed with returncode: {1}. '
                'CMD "{2}"'.format(msg_id, returncode, cmd))
        else:
            if flag_file is not None:
                touch(flag_file)
    except OSError, err:
        logger.exception(
            '{0}, failed to start, raising OSError {1}. '
            'CMD: "{2}"'.format(msg_id, err, cmd))


def gen_completion_stamp(key, stamp_dir):
    """
    @param key: the key to identify the type of stamp. e.g. FASTQ
    @param stamp_dir: where this stamp is to be created
    """
    return os.path.join(stamp_dir, '{0}.COMPLETE'.format(key))


def gen_rsem_cmd(sample):
    """return a command as a list"""
    fastq_gz_input, avail_fastq_gz = gen_fastq_gz_input(sample)
    if fastq_gz_input is None:
        cmd = 'echo problematic fastq_gz_input: {0}'.format(avail_fastq_gz)
    else:
        cmd = [
            'rsem-calculate-expression', # 1.2.5
            '-p 2',
            '--time',
            '--no-bam-output',
            '--bowtie-chunkmbs 256',
            # could also be found in the PATH
            # '--bowtie-path', '/home/zxue/Downloads/rchiu_Downloads/bowtie-1.0.0',
            fastq_gz_input,
            S.SPECIES_INFO[sample.organism]['index'],
            os.path.join(sample.outdir, sample.name),
            '1>{outdir}/rsem.log'.format(outdir=sample.outdir),
            '2>{outdir}/align.stat'.format(outdir=sample.outdir)
        ]
        cmd = ' '.join(cmd)
    # REF:
    # http://stackoverflow.com/questions/23522539/how-to-use-subprocess-with-multiple-multiple-stdin-from-zcat/23523350?noredirect=1#23523350
    return ['/bin/bash', '-c', cmd]

def gen_fastq_gz_input(sample):
    avail_fastq_gz = glob.glob(os.path.join(sample.outdir, '*.fastq.gz'))
    gz_RE = re.compile('SRR\d+\.fastq\.gz')
    gz1_RE = re.compile('SRR\d+\_1\.fastq\.gz')
    gz2_RE = re.compile('SRR\d+\_2\.fastq\.gz')
    fastq_gz_single = sorted([_ for _ in avail_fastq_gz if gz_RE.search(_)])
    fastq_gz1 = sorted([_ for _ in avail_fastq_gz if gz1_RE.search(_)])
    fastq_gz2 = sorted([_ for _ in avail_fastq_gz if gz2_RE.search(_)])
    
    if fastq_gz1 and fastq_gz2 and not fastq_gz_single:
        fastq_gz_input = (
            "--paired-end <(/bin/zcat {0}) <(/bin/zcat {2})".format(
                ' '.join(fastq_gz1, fastq_gz2)))
    elif not fastq_gz1 and not fastq_gz2 and fastq_gz_single:
        fastq_gz_input = "<(/bin/zcat {0})".format(' '.join(fastq_gz_single))
    else:
        fastq_gz_input = None
    return fastq_gz_input, avail_fastq_gz
