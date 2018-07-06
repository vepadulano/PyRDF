sudo apt-get --yes update
sudo apt-get --yes install python-pip
sudo pip install enum34
sudo pip install nose
source /usr/local/bin/thisroot.sh
sudo chown -R $UID:$UID /app
cd app
nosetests