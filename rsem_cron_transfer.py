import os
import sys
import re
import yaml
import paramiko

import logging
logging.basicConfig(level=logging.INFO, disable_existing_loggers=True,
                    format='%(levelname)s|%(asctime)s|%(name)s:%(message)s')


def sshexec(cmd, host, username, private_key_file='~/.ssh/id_rsa'):
    """
    :param private_key_file: could be ~/.ssh/id_dsa, as well
    """

    private_key_file = os.path.expanduser(private_key_file)
    rsa_key = paramiko.RSAKey.from_private_key_file(private_key_file)

    # This step will timeout after about 75 seconds if cannot proceed
    channel = paramiko.Transport((host, 22))
    channel.connect(username=username, pkey=rsa_key)
    session = channel.open_session()

    # if exec_command fails, None will be returned
    session.exec_command(cmd)

    # not sure what -1 does? learned from ssh.py
    output = session.makefile('rb', -1).readlines()
    channel.close()
    if output:
        return output


def get_remote_free_disk_space(df_cmd, remote, username):
    """
    find the free disk space on remote host.

    :param df_cmd: should be in the form of df -k -P target_dir
    """
    output = sshexec(df_cmd, remote, username)
    # e.g. output:
    # ['Filesystem         1024-blocks      Used Available Capacity Mounted on\n',
    #  '/dev/analysis        16106127360 12607690752 3498436608      79% /extscratch\n']
    return int(output[1].split()[3])


def get_current_remote_usage(find_cmd, remote, username,
                             r_rsem_output_dir, l_rsem_output_dir):
    """find the space that has already been consumed by rsem_output by walking
    through each GSM and computing the sum of their estimated usage, if
    rsem.COMPLETE exists for a GSM, then ignore that GSM
    
    mechanism: fetch the list of files in r_rsem_output_dir, and find the
    fastq.gz for each GSM, then find the corresponding fastq.gz in
    l_rsem_output_dir, and calculate sizes based on them

    :param find_cmd: should be in the form of find {remote_rsem_output_dir}
    :param r_rsem_output_dir: remote rsem output directory
    :param l_rsem_output_dir: local rsem output directory

    """
    output = sshexec(find_cmd, remote, username)
    output = [_.strip() for _ in output] # remote trailing '\n'

    usage = 0
    for dir_ in sorted(output):
        match = re.search('(GSM\d+$)', os.path.basename(dir_))
        if match:
            if not os.path.join(dir_, 'rsem.COMPLETE') in output:
                # only count the disk spaces used by those GSMs that are finished
                # or processed successfully
                gsm_dir = dir_.replace(r_rsem_output_dir, l_rsem_output_dir)
                usage += estimate_rsem_usage(find_fq_gzs(gsm_dir))
    logging.info('current remote usage by {0}: {1}'.format(
        r_rsem_output_dir, pretty_usage(usage)))
    return usage


def pretty_usage(val):
    """val should be in KB"""
    return '{0:.1f} GB'.format(val / 1e6)


def main():
    with open('rsem_pipeline_config.yaml') as inf:
        config = yaml.load(inf.read())

    r_host, username = config['REMOTE_HOST'], config['USERNAME']
    l_rsem_output_dir = config['LOCAL_RSEM_OUTPUT_DIR']
    r_rsem_output_dir = config['REMOTE_RSEM_OUTPUT_DIR']

    free_space = get_remote_free_disk_space(
        config['REMOTE_CMD_DF'], r_host, username)
    logging.info('free space available on remote host: {0}'.format(
        pretty_usage(free_space)))

    r_current_usage = get_current_remote_usage(
        'find {0}'.format(config['REMOTE_RSEM_OUTPUT_DIR']),
        r_host, username,
        r_rsem_output_dir, l_rsem_output_dir)
    
    r_max_usage = config['REMOTE_MAX_USAGE'] # in KB, 1e9 = 1TB
    r_min_free = config['REMOTE_MIN_FREE']

    r_free_to_use = max(0, r_max_usage - r_current_usage)
    logging.info('free to use: {0}'.format(pretty_usage(r_free_to_use)))


    if r_free_to_use < r_min_free:
        logging.info('free to use space ({0}) < min free ({1}) on remote host, '
                     'no transfer is happening'.format(
                         pretty_usage(r_free_to_use),
                         pretty_usage(r_min_free)))

    
    for root, dirs, files in os.walk(os.path.abspath(l_rsem_output_dir)):
        # trying to capture directory as such GSExxxxx/species/GSMxxxxx
        gse_path, gsm = os.path.split(root)
        gse = os.path.basename(os.path.dirname(gse_path))
        if not (re.search('GSM\d+$', gsm) and re.search('GSE\d+$', gse)):
            continue

        gsm_dir = root
        fq_gzs = find_fq_gzs(gsm_dir)
        if fq_gzs:
            size_fq_gzs = estimate_rsem_usage(fq_gzs)
            print pretty_usage(size_fq_gzs)
            
        # # check sra2fastq.COMPLETE

        # os.path.getsize(
        # print gse, gsm
        # print gsm_dir
        # print dirs
        # print files


RE_fq_gz = re.compile('(SRR\d+)_[12]\.fastq\.gz', re.IGNORECASE)
def find_fq_gzs(gsm_dir):

    """
    return a list of fastq.gz files for a GSM if sra2fastq.COMPLETE exists

    :param gsm_dir: the GSM directory, generated by os.walk
    """
    fq_gzs = []
    files = os.listdir(gsm_dir)
    for _ in files:
        match = RE_fq_gz.search(_)
        if match:
            srr = match.group(1)
            flag_sra2fastq = os.path.join(
                gsm_dir, '{0}.sra.sra2fastq.COMPLETE'.format(srr))
            if os.path.exists(flag_sra2fastq):
                fq_gzs.append(os.path.join(gsm_dir, match.group(0)))
    return fq_gzs


def estimate_rsem_usage(fq_gzs):
    """
    estimate the maximum disk space that is gonna be consumed by rsem analysis
    on a GSM based on a list of fq_gzs
    """
    # when estimating rsem usage, it has to search fastq.gz first and then
    # sra2fastq.COMPLETE because the later is created at last and there could
    # be multiple sra files to be converted

    # e.g. fq_gzs:
    # ['/path/rsem_output/GSExxxxx/species/GSMxxxxxxx/SRRxxxxxxx_x.fastq.gz',
    #  '/path/rsem_output/GSExxxxx/species/GSMxxxxxxx/SRRxxxxxxx_x.fastq.gz']

    # Based on observation of smaller fastq.gz file by gunzip -l
    # compressed        uncompressed  ratio uncompressed_name
    # 266348960          1384762028  80.8% rsem_output/GSE42735/homo_sapiens/GSM1048945/SRR628721_1.fastq
    # 241971266          1255233364  80.7% rsem_output/GSE42735/homo_sapiens/GSM1048946/SRR628722_1.fastq
    gzip_compression_ratio = 0.8
    # assume the maximum will be 9 times larger
    overestimate_ratio = 9

    raw_size = sum(map(os.path.getsize, fq_gzs))
    # Byte => KB, os.path.getsize return size in bytes
    res = raw_size / 1000.
    # estimate the size of uncompressed fastq
    res = res / (1 - gzip_compression_ratio)
    # overestimate
    res = res * overestimate_ratio
    return res


if __name__ == "__main__":
    main()
