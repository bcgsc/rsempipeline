#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""example run of this script:
python collect_rsem_progress_data.py \
  -d rsem_output_dir [rsem_output_dir2, rsem_output_dir3, ...] \
  --qstat-cmd 'qstat -xml -u zxue'
"""

import os
import sys
import re
import json
import argparse

import subprocess

import xml.etree.ElementTree as xml

def get_qstat_data(qstat_cmd):
    proc = subprocess.Popen(qstat_cmd, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    # stdout: the raw_xml data wanted 
    return stdout

def get_jobs_from_qstat_data(host, qstat_cmd):
    """
    :param qstat_cmd: should be a list of strings
    """
    raw_xml = get_qstat_data(qstat_cmd)
    if not raw_xml:
        return None
    xml_data = xml.fromstring(raw_xml)
    if host == 'genesis':
        return analyze_genesis_queue(xml_data)
    elif host == 'apollo':
        # because apollo also uses SGE as genesis, so that output has the same
        # format
        return analyze_genesis_queue(xml_data)
    elif host == 'nestor':
        return analyze_nestor_queue(xml_data)
    else:
        raise ValueError('analysis for qstat data from {0} [{1}] has not been '
                         'not implemented yet'.format(host, qstat_cmd))


def analyze_genesis_queue(xml_data):
    queued_gsms, running_gsms = [], []
    # xpath: finding job_list recursively
    for job in xml_data.findall('.//job_list'):
        job_name = job.find('JB_name').text.strip()
        gse_gsm_search = re.search(
            '(?P<GSM>GSM\d+)\_(?P<GSE>GSE\d+)', job_name)
        if not gse_gsm_search:
            continue
        # the reason gse information should be there as well is because some
        # GSMs belong to two GSEs and their job status could be different
        gsm = '{0}-{1}'.format(
            gse_gsm_search.group('GSE'), gse_gsm_search.group('GSM'))
        state = job.get('state')
        if state == 'running':
            running_gsms.append(gsm)
        elif state == 'pending':
            queued_gsms.append(gsm)
    return running_gsms, queued_gsms


def analyze_nestor_queue(xml_data):
    queued_gsms, running_gsms = [], []
    queues = xml_data.findall('queue')
    for queue in queues:
        for job in queue.findall('job'):
            job_name = job.get('JobName')
            gse_gsm_search = re.search(
                '(?P<GSM>GSM\d+)\_(?P<GSE>GSE\d+)', job_name)
            if not gse_gsm_search:
                continue
            gsm = '{0}-{1}'.format(
                gse_gsm_search.group('GSE'), gse_gsm_search.group('GSM'))
            state = job.get('State')
            if state == 'Running':
                running_gsms.append(gsm)                
            elif state == 'Idle' or state == 'BatchHold':
                queued_gsms.append(gsm)                
    return running_gsms, queued_gsms


def collect_report_data_per_dir(
    dir_to_walk, running_gsms, queued_gsms, options):
    """
    :param running_gsms: running gsms determined from qstat output
    :param queued_gsms: queued_gsms determined from qstat output
    """

    # collect the files that will be produced by rsem immediately after its run
    # starts
    RE_failed_signal_file = re.compile('align\.stats')
    
    res = {}
    for root, dirs, files in os.walk(os.path.abspath(dir_to_walk)):
        # gse_path: e.g. path/to/<GSExxxxxx>/<species>
        gse_path, gsm = os.path.split(root)
        species = os.path.basename(gse_path)
        gse = os.path.basename(os.path.dirname(gse_path))
        if re.search('GSM\d+$', gsm) and re.search('GSE\d+$', gse):
            rsem_output_dir = os.path.dirname(os.path.dirname(gse_path))
            if not rsem_output_dir in res:
                res[rsem_output_dir] = {}
            if gse not in res[rsem_output_dir]:
                res[rsem_output_dir][gse] = {}
            dd = res[rsem_output_dir][gse]

            if species not in dd:
                dd[species] = {}
            dd = res[rsem_output_dir][gse][species]

            dd[gsm] = {}
            # for the format of gsm_query, please refer to how queued_gsms or
            # running_gsms are constructed in analyze_<clustername>_queue
            # functions
            gsm_query = '{0}-{1}'.format(gse, gsm)
            if gsm_query in queued_gsms:
                dd[gsm].update(status='queued')
            elif gsm_query in running_gsms:
                dd[gsm].update(status='running')
            elif options.flag_file in files:
                # e.g. rsem.COMPLIETE exists, meaning passed
                dd[gsm].update(status='passed')
            elif any(map(RE_failed_signal_file.search, files)):
                # if it doesn't have rsem.COMPLETE, not in queue, and running,
                # but with failed_signal_file, then it's a failed job
                dd[gsm].update(status='failed')
            else:
                # otherwise, the job probably hasn't started yet
                dd[gsm].update(status='none')
    return res


def main():
    options = parse_args()
    host = options.host
    qstat_cmd = options.qstat_cmd.split()
    try:
        running_gsms, queued_gsms = get_jobs_from_qstat_data(host, qstat_cmd)
    except TypeError:           # qstat or showq error
        return json.dumps({})

    dirs_to_walk = options.dirs
    report_data = {}
    for dir_to_walk in dirs_to_walk:
        res = collect_report_data_per_dir(
            dir_to_walk, running_gsms, queued_gsms, options)
        report_data.update(res)

    # from pprint import pprint as pp
    # pp(report_data)
    sys.stdout.write(json.dumps(report_data))

def parse_args():
    parser = argparse.ArgumentParser(description='report progress of GSE analysis')
    parser.add_argument(
        '--host', required=True, choices=['genesis', 'nestor', 'apollo'],
        help="the host where progress data is collected")
    parser.add_argument(
        '--qstat-cmd', required=True,
        help="shell command to fetch xml from qstat output, e.g. 'qstat -xml -u username'")
    parser.add_argument(
        '-d', '--dirs', required=True, nargs='+',
        help='''
The directory where GSE and GSM folders are located, must follow the hierachy like
rsem_output/
|-- GSExxxxx
|   `-- homo_sapiens
|       |-- GSMxxxxxxx
|       |-- GSMxxxxxxx
|-- GSExxxxx
|   `-- mus_musculus
|       |-- GSMxxxxxxx
'''),
    parser.add_argument(
        '--flag_file', default='rsem.COMPLETE',
        help='The name of the file whose existance signify compleition of a GSM analysis')
    options = parser.parse_args()
    return options
    

if __name__ == "__main__":
    main()
