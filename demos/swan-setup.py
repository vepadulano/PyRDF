import os
import shutil
import subprocess

cwd = cwd = os.getcwd()

destination = os.path.join(cwd, 'PyRDF')
source_directory = os.path.abspath(os.path.join(cwd, 'PyRDF', 'PyRDF'))

shutil.make_archive(destination, 'zip', source_directory)

