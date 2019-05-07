#!/bin/bash

# This script is meant to be called by the "script" step defined in
# .travis.yml.

# Install PyRDF
python setup.py install --user

# Run flake8
flake8 --version
flake8 --config=flake8.cfg
