from setuptools import setup, find_packages
from io import open
from os import path

import pathlib
# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# automatically captured required modules for install_requires in requirements.txt
with (HERE / 'requirements.txt').open("r", encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if ('git+' not in x) and (
    not x.startswith('#')) and (not x.startswith('-'))]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs \
                    if 'git+' not in x]
setup (
 name = 'deon',
 description = 'A lightweight utility for managing DEcentrailized ONline datasets',
 version = '1.0.0',
 packages = find_packages(), # list of all packages
 install_requires = install_requires,
 python_requires='>=3.4', # any python greater than 2.7
 entry_points='''
        [console_scripts]
        deoncli=deon.__main__:cli
    ''',
 author="Ashvin Nair",
 keyword="data",
 long_description=README,
 long_description_content_type="text/markdown",
 license='MIT',
 url='https://github.com/anair13/deon',
 download_url='https://github.com/CITGuru/cver/archive/1.0.0.tar.gz',
  dependency_links=dependency_links,
  author_email='nair.ashvin@gmail.com',
  classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ]
)
