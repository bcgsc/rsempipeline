import csv

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

def gen_data_from_csv(input_csv):
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

    input_data = read_csv_species_as_key(input_csv)
    return input_data
