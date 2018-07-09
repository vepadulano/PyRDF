# All apt-get installs
sudo apt-get --yes update
sudo apt-get --yes install python-pip
sudo apt-get --yes install wget
sudo apt-get --yes install default-jdk

# Install required packages
sudo pip install -r /app/requirements.txt

# Pyspark installation
mkdir /home/builder/spark
cd /home/builder/spark
wget http://www-eu.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
tar -xvzf spark-2.3.1-bin-hadoop2.7.tgz
cd spark-2.3.1-bin-hadoop2.7/python/
sudo python setup.py install

# Making ROOT available
source /usr/local/bin/thisroot.sh

# Change permissions of source code dir
sudo chown -R $UID:$UID /app

# Install PyRDF from source
cd /app
sudo python setup.py install

# Run tests
nosetests tests/unit/*.py || exit 1
nosetests tests/integration/local/*.py || exit 1

# Run tutorials
echo " ======== Running single-threaded tutorials ======== "

# Run single-threaded tutorials locally
for filename in ./tutorials/local/sequential/df*.py
do
	echo " == Running $filename == "
	python "$filename" || exit 1
	echo "  Ran $filename successfully ! "
done

echo " ======== Ran single-threaded tutorials successfully ! ======== "

echo "======== Running multi-threaded tutorials ======== "


# Run multi-threaded tutorials locally
for filename in ./tutorials/local/MT/df*.py
do
	echo " == Running $filename == "
	python "$filename" || exit 1
	echo "  Ran $filename successfully ! "
done

echo " ======== Ran multi-threaded tutorials successfully ! ======== "