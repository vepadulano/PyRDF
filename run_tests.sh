#!/bin/bash

echo "\nPlease enter the path or command that refers to your python binary ! \n"
echo "Some examples : \n"
echo "\"python3\""
echo "\"/usr/bin/python\""
echo "\"python\" (This will be the default)\n"
echo "Enter the path/command here : \n(Simply Press Enter to pick the default value)\n"

read python_bin

if [ -z "$python_bin" ] # Check empty input
then
	python_bin=python
fi

echo "======== Installing PyRDF from setup ========\n"

$python_bin setup.py install --user --prefix= || {
	echo "\033[0;31m Error ! \033[0m This error could occur due to one of the below reasons : "
	echo "1. 'setup.py' is not in the current directory"
	echo "2. You don't have write permissions to site-packages folder"
	exit 1
}

echo "\n \033[0;32m Installed successfully ! \033[0m \n" # Print in green

$python_bin -c "import enum" || pip install --user enum34 || easy_install enum34 || {
	echo "\"pip install --user enum34\" and \"easy_install enum34\" didn't work. Please find a way to install the package 'enum34'"
	exit 1
}

echo "======== Running tests ========\n"

echo "\nPlease enter the command for running python tests using nose ! \n"
echo "Some examples : \n"
echo "\"nosetests\" (This will be the default)"
echo "\"nosetests-2.7\""
echo "Enter the command here : \n(Simply Press Enter to pick the default value)\n"

read nose_bin

if [ -z "$nose_bin" ] # Check empty input
then
	nose_bin=nosetests
fi

$nose_bin tests/unit/*.py || {
	echo "Please install nose and make sure that you're inside the PyRDF directory !"
	exit 1
}

$nose_bin tests/integration/local/*.py || {
	echo "Please install nose and make sure that you're inside the PyRDF directory !"
	exit 1
}

echo "\n \033[0;32m Ran tests successfully ! \033[0m \n"

echo "======== Running single-threaded tutorials ========\n"

{

# Run single-threaded tutorials locally
for filename in ./tutorials/local/sequential/df*.py
do
	echo "\n== Running $filename ==\n"
	$python_bin "$filename"
	echo "\n \033[0;32m Ran $filename successfully ! \033[0m"
done

} || {
	echo "Error in running tutorials ! Check if you're in the PyRDF root directory !"
	exit 1
}

echo "\n======== \033[0;32m Ran single-threaded tutorials successfully ! \033[0m ========\n"

echo "======== Running multi-threaded tutorials ========\n"

{

# Run multi-threaded tutorials locally
for filename in ./tutorials/local/MT/df*.py
do
	echo "\n== Running $filename ==\n"
	$python_bin "$filename"
	echo "\n \033[0;32m Ran $filename successfully ! \033[0m"
done

} || {
	echo "Error in running tutorials ! Check if you're in the PyRDF root directory !"
	exit 1
}

echo "\n======== \033[0;32m Ran multi-threaded tutorials successfully ! \033[0m ========\n"
