__author__ = "Smruti Mohanty"

import argparse
import logging
from datetime import datetime
# from multiprocessing.dummy import Pool as ThreadPool
# import multiprocessing
from pdfcrawl.pdfcrawl.core import generate_logger, pdf_handler_wrapper, search_handler_wrapper, simple_search
import re
import os
import concurrent.futures

version = """
            _  __                           _  
           | |/ _|                         | | 
  _ __   __| | |_    ___ _ __ __ ___      _| | 
 | '_ \ / _` |  _|  / __| '__/ _` \ \ /\ / / | 
 | |_) | (_| | |   | (__| | | (_| |\ V  V /| | 
 | .__/ \__,_|_|    \___|_|  \__,_| \_/\_/ |_| 
 | |                                           
 |_|                 0.0.3                                           

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
                        help='Formatt: "Home Building Plus:WA","Landlord Insurance Amendment:QLD"')

    """
    parser.add_argument("-s", "--state", dest="state", type=str, required=True,
                        help="State for which policy needs to be extracted.")
    """
    args = parser.parse_args()
    return args


def main():
    print(version)
    opts = get_args()

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

    ###################### 0.0.3 ###################################
    """
    header_string = ".*?Page\s*1\s*?of\s*?(\d+).*?"
    for token in opts.search_list:
        header_string += token + ".*?"

    search_state = opts.state

    policy_summary = r"YOUR\s+POLICY\s+SUMMARY.*?Policy number:.*?([A-Z]+.*?\d+)?\n*[a-zA-Z]+.*?{}.*".format(search_state)

    regex_item = r"{}{}".format(header_string, policy_summary)
    """
    #################################################################

    pdf_file_list = []
    input_paths = [f.rstrip("\\") for f in opts.files]  # Remove a \ from extreme rght of path
    for input_path in input_paths:
        if os.path.isfile(input_path) and input_path.endswith('.pdf'):
            pdf_file_list.append(input_path)
        elif os.path.isdir(input_path):
            for root, dirs, input_files in os.walk(input_path):
                for input_file in input_files:
                    if input_file.endswith('.pdf'):
                        pdf_file_list.append(input_path + "/" + input_file)
        else:
            logger.error("THREAD - main - The utility works on PDF files only.")
            raise NotImplementedError("pdfcrawl - Exception - The Utility only works with PDF files")
    logger.debug("THREAD - main - All file list {}".format(pdf_file_list))

    header_regex_dict = {}
    all_search_list = opts.search_list
    search_token = {}
    for token in all_search_list:
        token_list = token.split(":")
        search = token_list[0].strip()  # "Landlord Insurance Amendment"
        state = token_list[1].strip()  # WA
        grep_string = ".*?".join(search.split())
        header = r".*?{}\s*\b[a-zA-z0-9]{{1}}\b\s*\n*.*?Policy number:\n*([A-Z\s0-9]+?)\n+.*?{}.*?Page\s+1\s+of\s+(\d+).*".format(
            grep_string, state)
        result_file = "_".join(search.split()) + "_{}".format(state) + ".pdf"

        logger.debug("THREAD - main - Regular Expression String {}".format(header))
        regex_object = re.compile(header, re.DOTALL)
        header_regex_dict[result_file] = regex_object

    ##### 0.0.3 Create Search Based on Search String ############
    """
    pool = ThreadPool(threads)
    pool_specs = []
    for pdf_file in pdf_file_list:
        pool_specs.append((logger, pdf_file, policy_regex))
        for pdf_file in pdf_file_list:
        pool_specs.append((logger, pdf_file, header_regex_dict))
        # pool.map(pdf_handler_wrapper, pool_specs)
    pool.close()
    pool.join()
    """
    ##############################################################

    # threads = max(len(all_search_list), opts.num[0])

    search_result = {}

    for pdf_file in pdf_file_list:
        if len(list(header_regex_dict.keys())) > 0:
            logger.info(
                "THREAD - main - {} - Initiating search of all strings: {} ".format(pdf_file, header_regex_dict))
            fetched_file_dict = simple_search(logger, pdf_file, header_regex_dict)
            for fetched_file, page_number in fetched_file_dict.items():
                searched_token = fetched_file.replace(".pdf", "").split("_")
                logger.info("THERAD - main - {} - Search Completed. File @ {}".format(searched_token, fetched_file))
                header_regex_dict.pop(fetched_file, None)
                logger.debug("THREAD - main - Search Pending {}".format(header_regex_dict))
                search_result[fetched_file] = searched_token
        else:
            logger.info("THREAD - main - All strings searched. Exit Search.")
            break
    for search_file, search_item in search_result.items():
        logger.info("THREAD - main - Search Token: {}. Result File {}".format(search_item, search_file))
    logger.info("THREAD - main - Completed All Tasks. Log file at %s " % log_file)


if __name__ == '__main__':
    # multiprocessing.freeze_support()
    main()
