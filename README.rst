pdfcrawl
===========================

**pdfcrawl** utility is meant for searching vast pdf files for certain policy and extract the pages where the match is found.


Getting Started
----------------------------

* Installation

The utility can be installed and run in multiple ways.

    #.  Get the latest executable from `Release <https://github.com/2spmohanty/pdfcrawl/releases/tag/0.0.1>`_ section of GitHub.
    #.  Get the **.whl** file from `Release <https://github.com/2spmohanty/pdfcrawl/releases/tag/0.0.1>`_ section of GitHub. Then install it using pip command.

        .. note::
                pip install pdfcrawl-0.0.1-py3-none-any.whl
    #. Clone the repo from `here <https://github.com/2spmohanty/pdfcrawl.git>`_


* Running

    #. **Help**
        .. note::
                usage: app.py [-h] [-d] -f FILES [FILES ...] [-l LOGFILE] [-n NUM] -g SEARCH_LIST [SEARCH_LIST ...] -s STATE

                A commandline utility to crawl on pdf files.

                optional arguments:
                  -h, --help            show this help message and exit
                  -d, --debug           Enable debug output
                  -f  FILES [FILES ...], --files FILES [FILES ...]
                                        The absolute path to all files separated by white space.

                  -l LOGFILE, --log LOGFILE
                                        Log file name.
                  -n NUM, --num NUM     Number of files to operate on simultaneously. Should not be a number greater than processor in your computer. Default is 4.

                  -g  SEARCH_LIST [SEARCH_LIST ...], --grep SEARCH_LIST [SEARCH_LIST ...]
                                        Strings to search. The order of the string is preserved.
                  -s STATE, --state STATE
                                        State for which policy needs to be extracted.



#. **Running the executable.**

        .. note::

           pdfcrawl -f *"pdf_file1","pdf_file2",pdf_file3* -g *Headline of Insurance Document* -s *state*



    #. **Running from source code.**

        .. note::

          C:\Workspace> python -m pdfcrawl.pdfcrawl.app -f *"pdf_file1","pdf_file2",pdf_file3* -g *Headline of Insurance Document* -s *state*



