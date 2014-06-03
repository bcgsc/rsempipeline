import sys
import csv

"""
generate simple stats for GSE_GSM_species.py. e.g.

species        # GSEs          # GSMs         
Homo sapiens    13              377            
Mus musculus    1               11             
"""

def main(infile):
    dd = {}
    with open(infile, 'rb') as inf:
        csvreader = csv.reader(inf, delimiter='\t')
        for (gse, gsm, species) in csvreader:
            if species in dd:
                if gse in dd[species]:
                    dd[species][gse].append(gsm)
                else:
                    dd[species][gse] = [gsm]
            else:
                dd[species] = {gse:[gsm]}

    print '''
NOTE: the GSEs may contain GSMs for different species, so the
total number of GSEs is not a simple sum of GSEs of species.
'''

    print '{0:20s} {1:15s} {2:15s}'.format('species', '# GSEs', '# GSMs')
    all_gses, all_gsms = set(), set()
    for species in dd:
        gses_per_species = dd[species].keys()
        gsms_per_species = []
        all_gses.update(gses_per_species)
        for gse in dd[species]:
            gsms_per_species.extend(dd[species][gse])
            all_gsms.update(gsms_per_species)

        print '{0:20s} {1:<15d} {2:<15d}'.format(
            species, len(gses_per_species), len(gsms_per_species))
    print 'Total # GSEs:\t{0}'.format(len(all_gses))
    print 'Total # GSMs:\t{0}'.format(len(all_gsms))
    
if __name__ == "__main__":
    try:
        main(sys.argv[1])
    except IndexError:
        print "gUsage: python2.7.x GSE_GSM_species_stats.py csv_file (e.g. GSE_GSM_species.csv)"
