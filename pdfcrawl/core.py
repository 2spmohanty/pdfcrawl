__author__ = "Smruti Mohanty"

from PyPDF2 import PdfFileReader, PdfFileWriter
import time
from pdfminer.layout import LAParams
from pdfminer.high_level import extract_text


def generate_logger(log_level=None, log_file=None):
    """
    A Function to Generate Logger Object

    :param log_level:
    :param log_file:
    :return:
    """
    import logging
    #    PROJECT_DIR="/home/vmlib/spm/nsx"
    fh = None
    FORMAT = "%(asctime)s %(levelname)s %(message)s"
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    # Reset the logger.handlers if it already exists.
    if logger.handlers:
        logger.handlers = []
    formatter = logging.Formatter(FORMAT)
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def pdf_handler(logger, pdf_file_name, search_object):
    """
    pdf_handler Reads the pfd and applies transformation rules to extract matching texts and
    creates a new pdf out of it.
    :param logger:
    :param pdf_file_name:
    :param search_object:
    :return:
    """
    logger.info("THREAD - pdf_handler - {} - Search File initiated".format(pdf_file_name))
    # logger.info("THREAD - pdf_handler - {} - Search for {}".format(pdf_file_name, search_tokens))
    # logger.info("THREAD - pdf_handler - {} - Search for {}".format(pdf_file_name, state_name))

    logger.debug("THREAD - pdf_handler - {} - Reading File initiated".format(pdf_file_name))

    pdf_out_file = pdf_file_name.replace(".pdf", "_extracted.pdf")
    pdfOutput = open(pdf_out_file, 'wb')

    # pdfOutput = open('D:\\Smruti\\Python\\Practice\\pdfscrapper\\anish_selected_page.pdf', 'wb')

    try:

        with open(pdf_file_name, "rb") as f:
            pdf = PdfFileReader(f)
            pdfWriter = PdfFileWriter()

            total_pages = pdf.getNumPages()
            logger.debug("THREAD - pdf_handler - {} - "
                         "Total Number of pages in file: {}".format(pdf_file_name, total_pages))
            first_page = None
            for page_number in range(total_pages):
                logger.debug("THREAD - {} - Scanning Page {}".format(pdf_file_name, page_number))
                page_content = pdf.getPage(page_number)
                page_text = page_content.extractText()
                first_page_matched = search_object.match(page_text)
                if first_page_matched:
                    # logger.debug("THREAD - pdf_handler - {} - Page Content\n{}".format(pdf_file_name, page_text))
                    logger.info("THREAD - pdf_handler - {} - Match Obtained for user input.".format(pdf_file_name))
                    first_page = page_number
                    logger.info("THREAD - pdf_handler - {} - The first page is at {}".format(pdf_file_name, first_page))
                    total_policy_pages = int(first_page_matched.group(1).strip())
                    policy_number = first_page_matched.group(2)
                    logger.info(
                        "THREAD - pdf_handler - {} - Policy Number: {} . Number of pages to be extracted: {}".format(
                            pdf_file_name, policy_number, total_policy_pages))

                    break
            if first_page == 0 or first_page:
                for matched_page_number in range(first_page, first_page + total_policy_pages):
                    logger.debug(
                        "THREAD - pdf_handler - {} - Extracting Page {}".format(pdf_file_name, matched_page_number + 1))
                    page_content = pdf.getPage(matched_page_number)
                    pdfWriter.addPage(page_content)
                logger.info("THREAD - pdf_handler - {} - Extracted file name: {}".format(pdf_file_name, pdf_out_file))
                pdfWriter.write(pdfOutput)
            else:
                logger.error("THREAD - pdf_handler - {} - Failed Matching the desied text")


    except Exception as e:
        logger.error("THREAD - pdf_handler - {} - Error while processing {}".format(pdf_file_name, e))

    finally:
        pdfOutput.close()


def pdf_handler_wrapper(args):
    return pdf_handler(*args)


def search_handler(logger, pdf_file_name, search_object):
    for extract_file, regex_object in search_object.items():
        logger.info("{} - {} {}".format(pdf_file_name, extract_file, regex_object))
        time.sleep(2)
        return search_object.keys()


def search_handler_wrapper(args):
    return search_handler(*args)


def simple_search(logger, pdf_file, pdf_regex_dict):
    logger.info("THREAD - simple_search - Reading File {}".format(pdf_file))
    regex_dict = pdf_regex_dict
    extract_file_first_page = {}

    try:

        with open(pdf_file, 'rb') as fin:
            pdf = PdfFileReader(fin)
            total_pages = pdf.getNumPages()
            logger.info("THREAD - simple_search - {} - Will read {} pages.".format(pdf_file,total_pages))
            for page_number in range(total_pages):
                text = extract_text(fin, page_numbers=[page_number], laparams=LAParams())
                for extract_file, search_regex in regex_dict.items():
                    pdfWriter = None
                    first_page_matched = search_regex.match(text)
                    if first_page_matched:
                        pdfWriter = PdfFileWriter()
                        policy_number = (first_page_matched.group(1))
                        total_pages = int(first_page_matched.group(2))
                        logger.info("THREAD - simple_search - {} - Match obtained for {} at page {}. "
                                    "Policy Number: {}. "
                                    "Total Page to be extracted: {}.".format(pdf_file, extract_file,
                                                                             page_number, policy_number, total_pages))


                        for matched_page_number in range(page_number, page_number + total_pages):
                            logger.debug(
                                "THREAD - simple_search - {} - Collecting Page {}".format(pdf_file,
                                                                                          matched_page_number + 1))
                            page_content = pdf.getPage(matched_page_number)
                            pdfWriter.addPage(page_content)

                        pdfOutput = None
                        try:
                            logger.info(
                                "THREAD - simple_search - {} - Creating file : {}".format(pdf_file, extract_file))
                            pdfOutput = open(extract_file, 'wb')
                            pdfWriter.write(pdfOutput)
                            extract_file_first_page[extract_file] = page_number
                            logger.info(
                                "THREAD - simple_search - {} - Extracted file name: {}".format(pdf_file, extract_file))
                        except Exception as e:
                            logger.error("THREAD - simple_search - Error while extracting "
                                         "file for search {} is {}".format(extract_file, e))
                        finally:
                            pdfOutput.close()
    except Exception as e:
        logger.error("THREAD - simple_search - Error while reading file")

    return extract_file_first_page
