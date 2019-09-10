# try:
from setuptools import setup, find_packages
# except ImportError:
#   from distutils.core import setup
import sys

req_file = open('requirements.txt', 'r')
req_list = [l.strip() for l in req_file]

extra = {}
if sys.version_info >= (3,):
    extra['use_2to3'] = True

setup(
    name='PyRDF',
    packages=find_packages(),
    version='0.2.0',
    description='Python Library for doing RDataFrame Analysis',
    author='Shravan Murali',
    author_email='shravanmurali@gmail.com',
    maintainer=(
        "Javier Cervantes, "
        "Enric Tejedor, "
        "Vincenzo Eduardo Padulano"
    ),
    maintainer_email=(
        "javier.cervantes@cern.ch, "
        "etejedor@cern.ch, "
        "vincenzo.eduardo.padulano@cern.ch"
    ),
    install_requires=req_list,
    url='https://github.com/JavierCVilla/PyRDF',
    keywords=[],
    classifiers=[],
    license='MIT License'
)
