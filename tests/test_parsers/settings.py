# two passed samples and one unpassed
GSE43770_FAMILY_SOFT_SUBSET_CONTENT = """^SERIES = GSE43770
!Series_sample_id = GSM1070765
!Series_sample_id = GSM1070766
!Series_sample_id = GSM1070767
^SAMPLE = GSM1070765
!Sample_type = SRA
!Sample_organism_ch1 = Homo sapiens
!Sample_instrument_model = Illumina HiSeq 2000
!Sample_library_source = transcriptomic
!Sample_library_strategy = RNA-Seq
!Sample_supplementary_file_1 = ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX219/SRX219901
^SAMPLE = GSM1070766
!Sample_type = SRA
!Sample_organism_ch1 = Homo sapiens
!Sample_instrument_model = Illumina HiSeq 2000
!Sample_library_source = transcriptomic
!Sample_library_strategy = RNA-Seq
!Sample_supplementary_file_1 = ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX219/SRX219902
^SAMPLE = GSM1070767
!Sample_type = SRA
!Sample_organism_ch1 = Homo sapiens
!Sample_instrument_model = Illumina HiSeq 2000
!Sample_library_source = transcriptomic
!Sample_library_strategy = RNA-Seq
!Sample_supplementary_file_1 = """

INVALID_GSE43770_FAMILY_SOFT_SUBSET_CONTENT = """^SERIES = GSE00000
!Series_sample_id = GSM1070765
^SAMPLE = GSM1070765
!Sample_type = SRA
!Sample_organism_ch1 = Homo sapiens
!Sample_instrument_model = Illumina HiSeq 2000
!Sample_library_source = transcriptomic
!Sample_library_strategy = RNA-Seq
!Sample_supplementary_file_1 = ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX219/SRX219901"""
