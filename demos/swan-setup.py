# This is a script for setting up the environment in SWAN.
# Since PyRDF is not distributed through CVMFS yet, we need
# to explicitly send it to the remote workers. This script
# should be invoke like this for the outer directory after
# cloning the repository.
#
#   $ python PyRDF/demos/swan-setup.py
#
# This will just compress the PyRDF module into a zip file.
# Since the swan terminal may not have zip, we provide this
# script as an alternative.

import os
import shutil

cwd = cwd = os.getcwd()

destination = os.path.join(cwd, 'PyRDF')
source_directory = os.path.abspath(os.path.join(cwd, 'PyRDF'))

shutil.make_archive(destination, 'zip', source_directory)
