# Run tests
pytest

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

echo "======== Running Spark tutorials ======== "


# Run Spark tutorials locally
for filename in ./tutorials/spark/df*.py
do
	echo " == Running $filename == "
	python "$filename" || exit 1
	echo "  Ran $filename successfully ! "
done

echo " ======== Ran Spark tutorials successfully ! ======== "
