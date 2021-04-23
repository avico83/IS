#!/usr/bin/env python
__author__ = 'avic'

import os
import argparse
import time
import multiprocessing
import pandas as pd
from urllib.parse import unquote

try:
    cpus = multiprocessing.cpu_count()
except NotImplementedError:
    cpus = 4  # arbitrary default

"""
Pre-requisites and assumptions:
1. Requires python 3.6+
2. Assuming multiprocessing is required by the origin of the question. otherwise could be implemented as multithreading
3. In case of directory, the code assume ONLY valid files exists.
"""


def process_file(file_path):
    """
    Reading a json file in chunks and convert into CSV
    :param file_path: file path or directory
    """
    df = pd.DataFrame()
    # read the json in chunks to make it memory efficient
    json_reader = pd.read_json(file_path, orient='columns', lines=True, chunksize=10000)
    for tmp_df in json_reader:
        df = df.append(tmp_df, ignore_index=True)

    # decode url
    df['RName'] = df.RName.apply(unquote)
    output_file = f"{file_path}.csv"
    df.to_csv(output_file, sep=',', encoding='utf-8')
    print(output_file)


def main(file_path):
    """
    Identify file or dir and execute the process
    :param file_path: file path or directory
    """
    if os.path.isdir(file_path):
        pool = multiprocessing.Pool(processes=cpus)
        files = [os.path.join(file_path, f) for f in os.listdir(file_path)]
        pool.map(process_file, files)
    else:
        process_file(file_path)


def option_parser():
    """
    The function show's the option parser menu
    :return: the options
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src",
        help="It is a file or a folder contains json files",
        action="store",
        required=True
    )
    return parser.parse_args()


if __name__ == "__main__":
    start_time = time.time()
    opt = option_parser()
    end_time = time.time()
    main(opt.src)
    print('"Converter runtime was {} seconds'.format(int(end_time - start_time)))
