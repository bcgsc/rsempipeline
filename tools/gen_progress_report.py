#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import json
import argparse

class GSEReportItem(object):
    def __init__(self, gse_report_data):
        """
        :param gse_report_data: a dict with the following keys
        """
        self.name = gse_report_data['name']
        self.path = gse_report_data['path']
        self.passed_gsms = sorted(gse_report_data['passed_gsms'])
        self.not_passed_gsms = sorted(gse_report_data['not_passed_gsms'])

        self.passed = not self.not_passed_gsms
        self.num_passed_gsms = len(self.passed_gsms)
        self.num_not_passed_gsms = len(self.not_passed_gsms)

def write_report(report_data):
    gse_report_items = []
    for gse in sorted(report_data.keys()):
        gse_ri = GSEReportItem(report_data[gse])
        gse_report_items.append(gse_ri)        

    from jinja2 import Environment, PackageLoader
    env = Environment(loader=PackageLoader('gen_progress_report', '.'))
    template = env.get_template('progress_report.template.html')
    content = template.render(gse_report_items=gse_report_items)
    with open('progress_report.html', 'wb') as opf:
        opf.write(content)

def collect_report_data_per_dir(dir_to_walk, report_data, options):
    for root, dirs, files in os.walk(os.path.abspath(dir_to_walk)):
        gse_path, gsm = os.path.split(root)
        gse = os.path.basename(gse_path)
        if re.search('GSM\d+$', gsm) and re.search('GSE\d+$', gse):
            if not json:
                sys.stderr.write('working on {0}\n'.format(root))
            if gse not in report_data:
                _ = report_data[gse] = {}
                _['name'] = gse
                _['path'] = [gse_path]
                _['not_passed_gsms'] = []
                _['passed_gsms'] = []
            else:
                # Since GSE may contain GSMs from multiple species
                if gse_path not in report_data[gse]['path']:
                    report_data[gse]['path'].append(gse_path)

            if options.flag_file in files:  # passed
                report_data[gse]['passed_gsms'].append(gsm)
            else:               # not passed
                report_data[gse]['not_passed_gsms'].append(gsm)

def main():
    options = parse_args()
    dirs_to_walk = options.dirs

    report_data = {}
    for dir_to_walk in dirs_to_walk:
        collect_report_data_per_dir(dir_to_walk, report_data, options)
    
    if options.json:
        sys.stdout.write(json.dumps(report_data))
    else:
        write_report(report_data)

def parse_args():
    parser = argparse.ArgumentParser(description='report progress of GSE analysis')
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
    parser.add_argument(
        '--json', action='store_true',
        help='dump json string to stdout instead of formatting the report')
    options = parser.parse_args()
    return options
    

if __name__ == "__main__":
    main()
