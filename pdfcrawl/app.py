__author__ = "Smruti Mohanty"

import argparse
import logging
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing
from pdfcrawl.pdfcrawl.core import generate_logger, pdf_handler_wrapper
import re

version = """
            _  __                           _  
           | |/ _|                         | | 
  _ __   __| | |_    ___ _ __ __ ___      _| | 
 | '_ \ / _` |  _|  / __| '__/ _` \ \ /\ / / | 
 | |_) | (_| | |   | (__| | | (_| |\ V  V /| | 
 | .__/ \__,_|_|    \___|_|  \__,_| \_/\_/ |_| 
 | |                                           
 |_|                 0.0.1                                           

"""


def get_args():
    """
    get_args consolidates user inputs.
    :return:
    """
    parser = argparse.ArgumentParser(description="A commandline utility to crawl on pdf files.")
    parser.add_argument('-d', '--debug', required=False, help='Enable debug output', dest='debug', action='store_true')
    parser.add_argument("-f", "--files", required=True, dest="files", type=str, nargs='+',
                        help="The absolute path to all files separated by white space.")
    parser.add_argument("-l", "--log", nargs=1, dest="logfile", type=str, help="Log file name.")
    parser.add_argument("-n", "--num", required=False, dest="num", default=[4], type=int,
                        help="Number of files to operate on simultaneously. "
                             "Should not be a number greater than processor in your computer. Default is 4.")
    parser.add_argument("-g", "--grep", dest='search_list', type=str, nargs='+', required=True,
                        help="Strings to search. "
                             "The order of the string is preserved.")
    parser.add_argument("-s", "--state", dest="state", type=str, required=True,
                        help="State for which policy needs to be extracted.")
    args = parser.parse_args()
    return args


def main():
    print(version)
    opts = get_args()

    threads = opts.num[0]
    pdf_file_list = opts.files

    log_file = None
    if opts.logfile:
        log_file = opts.logfile[0]

    debug = opts.debug
    if debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logger = None
    if log_file:
        logger = generate_logger(log_level, log_file=log_file)
    else:
        log_time = datetime.now().strftime("%d%m%Y_%H%M%S")
        log_file = "pdf_crawler_{}.log".format(log_time)
        logger = generate_logger(log_level, log_file=log_file)

    header_string = ".*?Page\s*1\s*?of\s*?(\d+).*?"
    for token in opts.search_list:
        header_string += token + ".*?"

    search_state = opts.state

    policy_summary = r"YOUR\s+POLICY\s+SUMMARY.*?Policy number:(.*)?Amendmentvalid.*?{}.*".format(search_state)

    regex_item = r"{}{}".format(header_string, policy_summary)

    logger.debug("THREAD - main - Regex for search %s" % regex_item)

    policy_regex = re.compile(regex_item, re.DOTALL)

    pool = ThreadPool(threads)
    pool_specs = []
    for pdf_file in pdf_file_list:
        pool_specs.append((logger, pdf_file, policy_regex))

    pool.map(pdf_handler_wrapper, pool_specs)
    pool.close()
    pool.join()

    logger.info("THREAD - main - Completed All Tasks. Log file at %s " % log_file)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
