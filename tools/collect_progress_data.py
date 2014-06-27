#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import json
import argparse

import subprocess

import xml.etree.ElementTree as xml

# class GSEReportItem(object):
#     def __init__(self, gse_report_data):
#         """
#         :param gse_report_data: a dict with the following keys
#         """
#         self.name = gse_report_data['name']
#         self.path = gse_report_data['path']
#         self.passed_gsms = sorted(gse_report_data['passed_gsms'])
#         self.not_passed_gsms = sorted(gse_report_data['not_passed_gsms'])

#         self.passed = not self.not_passed_gsms
#         self.num_passed_gsms = len(self.passed_gsms)
#         self.num_not_passed_gsms = len(self.not_passed_gsms)

# def write_report(report_data):
#     gse_report_items = []
#     for gse in sorted(report_data.keys()):
#         gse_ri = GSEReportItem(report_data[gse])
#         gse_report_items.append(gse_ri)        

#     from jinja2 import Environment, PackageLoader
#     env = Environment(loader=PackageLoader('gen_progress_report', '.'))
#     template = env.get_template('progress_report.template.html')
#     content = template.render(gse_report_items=gse_report_items)
#     with open('progress_report.html', 'wb') as opf:
#         opf.write(content)

def get_qstat_data(qstat_cmd):
    proc = subprocess.Popen(qstat_cmd, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    # stdout: the raw_xml data wanted 
    return stdout

def get_jobs_from_qstat_data(qstat_cmd):
    """
    :param qstat_cmd: should be a list of strings
    """
    raw_xml = get_qstat_data(qstat_cmd)
    xml_data = xml.fromstring(raw_xml)
    queued_gsms, running_gsms = [], []
    # xpath: finding job_list recursively
    for job in xml_data.findall('.//job_list'):
        state = job.get('state')
        job_name = job.find('JB_name').text.strip()
        gsm_search = re.search('GSM\d+', job_name)
        if not gsm_search:
            continue
        gsm = gsm_search.group()
        if state == 'running':
            running_gsms.append(gsm)
        elif state == 'pending':
            queued_gsms.append(gsm)
    return running_gsms, queued_gsms

def collect_report_data_per_dir(dir_to_walk, report_data,
                                running_gsms, queued_gsms,
                                options):
    """
    :param running_gsms: running gsms determined from qstat output
    :param queued_gsms: queued_gsms determined from qstat output
    """
    for root, dirs, files in os.walk(os.path.abspath(dir_to_walk)):
        gse_path, gsm = os.path.split(root)
        gse = os.path.basename(gse_path)
        if re.search('GSM\d+$', gsm) and re.search('GSE\d+$', gse):
            if gse not in report_data:
                _ = report_data[gse] = {}
                _['name'] = gse
                _['path'] = [gse_path]
                _['passed_gsms'] = []
                _['failed_gsms'] = []
                _['queued_gsms'] = []
                _['running_gsms'] = []
            else:
                # Since GSE may contain GSMs from multiple species
                if gse_path not in report_data[gse]['path']:
                    report_data[gse]['path'].append(gse_path)

            if options.flag_file in files:  # passed
                report_data[gse]['passed_gsms'].append(gsm)
            else:               # not passed
                if gsm in queued_gsms:
                    report_data[gse]['queued_gsms'].append(gsm)
                elif gsm in running_gsms:
                    report_data[gse]['running_gsms'].append(gsm)
                else:
                    # if it's not in queue, and unfinished, assume it's a
                    # failed job, if it hasn't started running, the folder
                    # shouldn't be walked.
                    report_data[gse]['failed_gsms'].append(gsm)

def main():
    options = parse_args()
    running_gsms, queued_gsms = get_jobs_from_qstat_data(options.qstat_cmd)

    dirs_to_walk = options.dirs
    report_data = {}
    for dir_to_walk in dirs_to_walk:
        collect_report_data_per_dir(
            dir_to_walk, report_data, 
            running_gsms, queued_gsms,            
            options)

    sys.stdout.write(json.dumps(report_data))

def parse_args():
    parser = argparse.ArgumentParser(description='report progress of GSE analysis')
    parser.add_argument(
        '--qstat-cmd', default='qstat -xml -u zxue'.split(), nargs='+',
        help='shell command to fetch xml from qstat output')
    parser.add_argument(
        '-d', '--dirs', required=True, nargs='+',
        help='''
The directory where GSE and GSM folders are located, must follow the hierachy like
GSE24455/
|-- GSM602557
|-- GSM602558
|-- GSM602559
|-- GSM602560
|-- GSM602561
'''),
    parser.add_argument(
        '--flag_file', default='rsem.COMPLETE',
        help='The name of the file whose existance signify compleition of a GSM analysis')
    options = parser.parse_args()
    return options
    

if __name__ == "__main__":
    main()

