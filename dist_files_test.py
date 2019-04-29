import os
import pyspark
from pyspark import SparkFiles
import unittest
import PyRDF
import math


# path = "test.txt"
# with open(path, "w") as testFile:
# 	_ = testFile.write("100")
# sc = pyspark.SparkContext.getOrCreate()

# sc.addFile(path)
# print(SparkFiles.get(path))
# def func(iterator):
# 	with open(SparkFiles.get(path)) as testFile:
# 		print(SparkFiles.get(path))
# 		fileVal = int(testFile.readline())
# 	return [x * fileVal for x in iterator]
# result = sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()
# print(result)

header = "tests/integration/local/test_headers/header1.hxx"
PyRDF.use("spark")
PyRDF.include(header)
print("inside script", SparkFiles.get(header))

rdf = PyRDF.RDataFrame(100)
# This filters out all numbers less than 5
rdf_filtered = rdf.Filter("check_number_less_than_5(tdfentry_)")
histo = rdf_filtered.Histo1D("tdfentry_")

# The expected results after filtering
# The actual set of numbers required after filtering
required_numbers = range(5)
required_size = len(required_numbers)
required_mean = sum(required_numbers) / float(required_size)
required_stdDev = math.sqrt(sum((x - required_mean)**2
                            for x in required_numbers) / required_size)
histo.GetEntries()