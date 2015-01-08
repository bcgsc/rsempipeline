"""
An incomplete parser for parsing the soft file downloaded from NCBI GEO. By
incomplete, it means not all information is considered. Instead, only the
information that is relevant to rsem analysis is checked
"""

import os
import re

import logging
logger = logging.getLogger(__name__)

from rsempipeline.utils.objs import Series, Sample

def update(current_sample, label, value, interested_organisms):
    """
    @param sample: current sample
    @param label: label of a given line in a soft file
    @param value: value of a given line in a soft file
    @interested_organisms: only organism that is in organisms is interested
    """
    sample = current_sample
    if label.startswith('!Sample_organism_ch'):
        if value in interested_organisms:
            sample.organism = value
        else:
            logger.debug('discarding sample {0} of {1} for '
                         '!Sample_organism: {2}'.format(
                             sample.name, sample.series.name, value))
            return None
    elif label.startswith('!Sample_supplementary_file_'):
        if value.startswith(
            'ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/'):
            sample.url = value
            # cannot discard specific to how soft file is formatted
    elif label == '!Sample_type':
        if value != 'SRA':
            logger.debug('discarding sample {0} of {1} for '
                         '!Sample_type: {2}'.format(
                             sample.name, sample.series.name, value))
            return None
    elif label == '!Sample_library_strategy':
        # some RNA-Seq data may still be labeled as OTHER, e.g. GSMs in
        # GSE52043
        if value not in ['RNA-Seq', 'OTHER']:
            logger.debug('discarding sample {0} of {1} for '
                         '!Sample_library_stragegy: {2}'.format(
                             sample.name, sample.series.name, value))
            return None
    elif label == '!Sample_instrument_model':
        if any(map(lambda x: x in value, [
                '454 GS', 'AB SOLiD', 'AB 5500xl Genetic Analyzer',
                'AB 5500 Genetic Analyzer',
        ])):
            logger.debug('discarding sample {0} of {1} for '
                         '!Sample_instrument_model: {2}'.format(
                             sample.name, sample.series.name, value))
            return None
    elif label == '!Sample_library_source':
        if value.lower() != 'transcriptomic':
            logger.debug('discarding sample {0} of {1} for '
                         '!Sample_library_source: {2}'.format(
                             sample.name, sample.series.name, value))
            return None
    return current_sample


def parse(soft_file, interested_organisms):
    """Parse the soft file"""
    def append_passed_sample(current_sample, series, index):
        if current_sample:
            if current_sample.is_info_complete():
                current_sample.index = index
                series.passed_samples.append(current_sample)
                index += 1
            else:
                logger.warn(
                    'info incomplete for current sample, '
                    'name: {0}; organism: {1}; url: {2}'.format(
                        current_sample.name, current_sample.organism,
                        current_sample.url))
        return index

    logger.info("Parsing file: {0} ...".format(soft_file))

    # try to extract the series name from the soft filename
    series_name_search = re.search(r'GSE\d+', soft_file)
    if series_name_search is not None:
        series_name_from_file = series_name_search.group()

    # Assume one GSE per soft file
    index = 1
    current_sample = None
    with open(soft_file, 'rb') as inf:
        for line in inf:
            label, value = [_.strip() for _ in line.split('=')]
            if label == '^SERIES':
                series = Series(value, os.path.abspath(soft_file))
                if series.name != series_name_from_file:
                    msg = ('series contained in the soft file doesn\'t match '
                           'that in the filename: {0} != {1}'.format(
                               series, series_name_from_file))
                    raise ValueError(msg)
            elif label == '^SAMPLE':
                index = append_passed_sample(current_sample, series, index)
                current_sample = Sample(name=value, series=series)
                series.samples.append(current_sample)

            if current_sample:
                current_sample = update(current_sample, label, value,
                                        interested_organisms)
        # append the last sample
        append_passed_sample(current_sample, series, index)

    logger.info("{0}: {1}/{2} samples passed".format(
        series.name, series.num_passed_samples(), series.num_samples()))
    logger.info('=' * 30)
    return series
