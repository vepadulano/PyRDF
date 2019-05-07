#!/bin/bash

# Install PyRDF
python setup.py install --user

# Install sphinx and sphinx ReadTheDocs theme
pip install sphinx sphinx_rtd_theme

# Build the docs
sphinx-build -b html docs docs/build/html