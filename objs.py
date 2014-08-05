import os
from utils import gen_sample_msg_id

class GEORecord(object):
    '''
    A GEORecord correspond to one line in the xlsx/csv file provided by the
    collaborator
    '''
    def __init__(self, GEO_number, sample_number, 
                 samples, species, platform, title):
        self.GSE = self.GEO_number = GEO_number
        self.num_used_samples, self.num_total_samples = [
            int(_.strip()) for _ in sample_number.split('/')]
        self.GSMs = self.samples = [
            # .strip gets rid of the ';' at the ends
            _.strip() for _ in samples.strip(';').split(';')]
        self.species = species.split(';')
        self.platform = platform.split()
        self.title = title

    def __repr__(self):
        return '{0} {1}'.format(self.GSE, ', '.join(self.species))


class Series(object):
    def __init__(self, name, soft_file=''):
        # name: e.g. GSE46224
        self.name = name
        # only passed samples goes here, i.e. sample.is_info_complete return
        # True
        self.passed_samples = []
        self.samples = []       # all samples
        # this soft_file where this series belongs
        self.soft_file = soft_file
        
    def num_passed_samples(self):
        return len(self.passed_samples)

    def num_samples(self):
        return len(self.samples)

    def __str__(self):
        return "{0} (passed: {1}/{2})".format(
            self.name, self.num_passed_samples(), self.num_samples())

    def __repr__(self):
        return self.__str__()


class Sample(object):
    def __init__(self, name, series, index=0, organism=None, url=None):
        """
        @params index: index of passed sample, 1-based, 0 means not indexed
        """
        self.name = name
        self.series = series  # the series in which the sample is in
        self.index = index    # index of all samples in the self.series.passed_samples
        self.organism = organism
        self.url = url
        # self.sras = []

    def is_info_complete(self):
        return self.name and self.organism and self.url
    
    def gen_outdir(self, outdir):
        """
        Generate the output directory where all downloads and analysis
        results of this particular sample are to going to be
        """
        # dir hirarchy: <GSE>/<species>/<GSM>
        self.outdir = os.path.join(
            outdir,
            self.series.name,
            self.organism.lower().replace(' ', '_'),
            self.name)
        return self.outdir

    # def num_sras(self):
    #     return len(self.sras)

    def __str__(self):
        return  '<{0}>'.format(gen_sample_msg_id(self))

    def __repr__(self):
        return self.__str__()

# class SRA(object):
#     def __init__(self, name, path, sample, index=0):
#         self.name, self.path = name, path
#         self.srr_id = os.path.splitext(self.name)[0]
#         self.location = os.path.join(path, name)
#         self.sample = sample
#         self.index = index

#     def __repr__(self):
#         return self.location
