#!/bin/bash

# This script is meant to be called by the "install" step defined in
# .travis.yml.

check_error()
{
    local last_exit_code=$1
    local last_cmd=$2
    if [[ ${last_exit_code} -ne 0 ]]; then
        echo "${last_cmd} exited with code ${last_exit_code}"
        echo "TERMINATING TEST"
        exit 1
    else
        echo "${last_cmd} completed successfully"
    fi
}

# Run tests
# -x exit instantly on first error or failed test
# -v increase verbosity
pytest -x -v

# Run tutorials
echo " ======== Running single-threaded tutorials ======== "

# Run single-threaded tutorials locally
for filename in ./tutorials/local/sequential/df*.py
do
	echo " == Running $filename == "
	python "$filename" &> /dev/null
  check_error $? "$filename"
done


echo "======== Running multi-threaded tutorials ======== "

# Run multi-threaded tutorials locally
for filename in ./tutorials/local/MT/df*.py
do
	echo " == Running $filename == "
	python "$filename" &> /dev/null
  check_error $? "$filename"
done


echo "======== Running Spark tutorials ======== "

# Run Spark tutorials locally
for filename in ./tutorials/spark/df*.py
do
	echo " == Running $filename == "
	python "$filename" &> /dev/null
  check_error $? "$filename"
done
