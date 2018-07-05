sudo apt-get --yes update
sudo apt-get --yes install python-pip
sudo pip install enum34
sudo pip install nose
source /usr/local/bin/thisroot.sh
sudo chown -R $UID:$UID /app
cd app
python setup.py install --user

nosetests

echo " ======== Running single-threaded tutorials ======== "

# Run single-threaded tutorials locally
{

for filename in ./tutorials/local/sequential/df*.py
do
	echo " == Running $filename == "
	python "$filename"
	echo "  Ran $filename successfully ! "
done

} || {
	echo "Error in running tutorials ! Check if you're in the PyRDF root directory !"
	exit 1
}

echo " ======== Ran single-threaded tutorials successfully ! ======== "

echo "======== Running multi-threaded tutorials ======== "

{

# Run multi-threaded tutorials locally
for filename in ./tutorials/local/MT/df*.py
do
	echo " == Running $filename == "
	python "$filename"
	echo "  Ran $filename successfully ! "
done

} || {
	echo "Error in running tutorials ! Check if you're in the PyRDF root directory !"
	exit 1
}

echo " ======== Ran multi-threaded tutorials successfully ! ======== "
