__author__ = "Smruti Mohanty"

import setuptools
from sphinx.setup_command import BuildDoc


"""
Copyright [2021] [Smruti Mohanty]
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

name = "pdfcrawl"
version = '0.0.4'

with open("../projectpdf/README.rst", "r") as fh:
    long_description = fh.read()

install_requires = ["PyPDF2"]

setuptools.setup(
    name=name,
    version=version,
    author="smruti",
    author_email="2spmohanty@gmail.com",
    description="A PDF Search and Extract Tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/2spmohanty/pdfcrawl",
    packages=["pdfcrawl"],
    package_dir={
        name: 'pdfcrawl'
    },
    package_data={
        name: [

        ]
    },
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=install_requires,
    extras_require={
        'coverage': [
            'coverage',
        ],
        'docs': [

            'sphinx_rtd_theme',
            'sphinx-git',
            'sphinx-markdown-parser',
            'sphinx-markdown-tables',
            'sphinxcontrib-plantuml',
            'sphinxcontrib-confluencebuilder',
            'sphinxcontrib-programoutput',
        ],
        'docs-lint': [
            'rstcheck',
        ],
        'lint': [
            'pylint',
            'prospector',
            'pyflakes',
        ],
        'testing': [
            'pytest',
            'pytest-cov',
            'tox',
        ],
    },
    entry_points={
        'console_scripts': [
            'pdfcrawl = pdfcrawl.app:main',
        ]
    },
    cmdclass={
        'build_sphinx': BuildDoc
    },
    command_options={
        'build_sphinx': {
            'version': ('setup.py', version),
            'source_dir': ('setup.py', 'docs')}
    },
)
