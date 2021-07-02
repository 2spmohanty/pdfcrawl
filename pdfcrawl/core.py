__author__ = "Smruti Mohanty"

import logging
from PyPDF2 import PdfFileReader, PdfFileWriter
import re


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

            for page_number in range(total_pages):
                page_content = pdf.getPage(page_number)
                page_text = page_content.extractText()
                first_page_matched = search_object.match(page_text)
                if first_page_matched:
                    logger.info("THREAD - pdf_handler - {} - Match Obtained for user input.".format(pdf_file_name))
                    first_page = int(page_number)
                    total_policy_pages = int(first_page_matched.group(1).strip())
                    policy_number = first_page_matched.group(2)
                    logger.info(
                        "THREAD - pdf_handler - {} - Policy Number: {} . Number of pages to be extracted: {}".format(
                            pdf_file_name, policy_number, total_policy_pages))

                    break

            for matched_page_number in range(first_page, first_page + total_policy_pages):
                logger.debug(
                    "THREAD - pdf_handler - {} - Extracting Page {}".format(pdf_file_name, matched_page_number + 1))
                page_content = pdf.getPage(matched_page_number)
                pdfWriter.addPage(page_content)
            logger.info("THREAD - pdf_handler - {} - Extracted file name: {}".format(pdf_file_name, pdf_out_file))
            pdfWriter.write(pdfOutput)


    except Exception as e:
        logger.error("THREAD - pdf_handler - {} - Error while processing {}".format(pdf_file_name, e))

    finally:
        pdfOutput.close()


def pdf_handler_wrapper(args):
    return pdf_handler(*args)
