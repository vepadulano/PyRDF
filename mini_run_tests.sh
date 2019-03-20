#!/bin/bash

echo -e "======== Running tests ========\n"

python_bin=python
nose_bin=nosetests

$nose_bin tests/unit/*.py || {
	echo "Please install nose and make sure that you're inside the PyRDF directory !"
	exit 1
}

$nose_bin tests/unit/backend/*.py || {
	echo "Please install nose and make sure that you're inside the PyRDF directory !"
	exit 1
}

$nose_bin tests/integration/local/*.py || {
	echo "Please install nose and make sure that you're inside the PyRDF directory !"
	exit 1
}

$nose_bin tests/integration/spark/*.py || {
	echo "Please install nose and make sure that you're inside the PyRDF directory !"
	exit 1
}

echo -e "\n \033[0;32m Ran tests successfully ! \033[0m \n"

echo -e "======== Running single-threaded tutorials ========\n"

{

# Run single-threaded tutorials locally
for filename in ./tutorials/local/sequential/df*.py
do
	echo -e "\n== Running $filename ==\n"
	$python_bin "$filename"
	echo -e "\n \033[0;32m Ran $filename successfully ! \033[0m"
done

} || {
	echo "Error in running tutorials ! Check if you're in the PyRDF root directory !"
	exit 1
}

echo -e "\n======== \033[0;32m Ran single-threaded tutorials successfully ! \033[0m ========\n"

echo -e "======== Running multi-threaded tutorials ========\n"

{

# Run multi-threaded tutorials locally
for filename in ./tutorials/local/MT/df*.py
do
	echo -e "\n== Running $filename ==\n"
	$python_bin "$filename"
	echo -e "\n \033[0;32m Ran $filename successfully ! \033[0m"
done

} || {
	echo -e "Error in running tutorials ! Check if you're in the PyRDF root directory !"
	exit 1
}

echo -e "\n======== \033[0;32m Ran multi-threaded tutorials successfully ! \033[0m ========\n"

echo -e "======== Running Spark tutorials ========\n"

{

# Run Spark tutorials locally
for filename in ./tutorials/spark/df*.py
do
	echo -e "\n== Running $filename ==\n"
	$python_bin "$filename"
	echo -e "\n \033[0;32m Ran $filename successfully ! \033[0m"
done

} || {
	echo -e "Error in running tutorials ! Check if you're in the PyRDF root directory !"
	exit 1
}

echo -e "\n======== \033[0;32m Ran Spark tutorials successfully ! \033[0m ========\n"
