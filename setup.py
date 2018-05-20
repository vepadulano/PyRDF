try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup
import sys

req_file = open('requirements.txt','r')
req_list = [l.strip() for l in req_file]

extra = {}
if sys.version_info >= (3,):
  extra['use_2to3'] = True

setup(
  name = 'PyTDF',

  packages = ['PyTDF'],

  version = '0.0.1',

  description = 'Python Library for doing TDataFrame Analysis',

  author = 'Shravan Murali',

  author_email = 'shravanmurali@gmail.com',

  install_requires= req_list,

  # entry_points= {
  #     'console_scripts':['competitive-dl = competitiveDl:'\
  #     'mains']
  # },

  url = 'https://github.com/shravan97/PyTDF',

  keywords = [],

  classifiers = [],

  license='MIT License'
)