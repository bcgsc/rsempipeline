"""Lists objects used in rsempipeline.py"""

import os

class Series(object):
    """The object that corresponds to a GSE/Series"""

    def __init__(self, name, soft_file=''):
        # name: e.g. GSE46224
        self.name = name
        # only passed samples goes here, i.e. sample.is_info_complete return
        # True
        self.passed_samples = []
        self.samples = []       # all samples
        # this soft_file where this series belongs
        self.soft_file = soft_file

    def add_sample(self, sample):
        self.samples.append(sample)

    def add_passed_sample(self, sample):
        self.add_sample(sample)
        self.passed_samples.append(sample)

    def num_samples(self):
        """return the number of total samples for this Series"""
        return len(self.samples)

    def num_passed_samples(self):
        """
        return the number of passed (i.e. qualified after checking in
        soft_parser.py) samples
        """
        return len(self.passed_samples)

    def __str__(self):
        return "{0} (passed samples: {1}/{2})".format(
            self.name, self.num_passed_samples(), self.num_samples())

    def __repr__(self):
        return self.__str__()


class Sample(object):
    """The object that corresponds to a GSM/Sample"""

    def __init__(self, name, series, index=0, organism=None, url=None):
        """
        @params index: index of passed sample, 1-based, 0 means not indexed
        """
        self.name = name
        # the series in which the sample is in
        self.series = series
        # index of all samples in the self.series.passed_samples
        self.index = index
        # organism defaults to None due to the format of soft file
        self.organism = organism
        self.url = url
        # self.sras = []

    def is_info_complete(self):
        """
        see if the is information of this sample is complete, by which it means
        its name, organism and url all exist
        """
        return self.name and self.organism and self.url

    def gen_outdir(self, outdir):
        """
        Generate the output directory where all downloads and analysis
        results of this particular sample are to going to be
        """
        if self.is_info_complete():
            # dir hirarchy: <GSE>/<species>/<GSM>
            self.outdir = os.path.join(
                outdir,
                self.series.name,
                self.organism.lower().replace(' ', '_'),
                self.name)
            return self.outdir
        else:
            raise ValueError('{0} is not information complete, make sure none '
                             'of its name, organism and url is None'.format(self))

    def msg_id(self):
        """
        used as an id to identify a particular sample for each logging message
        """

    # def num_sras(self):
    #     return len(self.sras)

    def __str__(self):
        return '<{0} ({2}/{3}/{4}) of {1}>'.format(
            self.name, self.series.name, self.index,
            self.series.num_passed_samples(),
            self.series.num_samples())

    def __repr__(self):
        return self.__str__()

# KEPT FOR REFERENCE ONLY 2014-10-21
# class SRA(object):
#     def __init__(self, name, path, sample, index=0):
#         self.name, self.path = name, path
#         self.srr_id = os.path.splitext(self.name)[0]
#         self.location = os.path.join(path, name)
#         self.sample = sample
#         self.index = index

#     def __repr__(self):
#         return self.location
