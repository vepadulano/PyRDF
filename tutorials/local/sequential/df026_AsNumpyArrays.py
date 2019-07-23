## \file
## \ingroup tutorial_dataframe
## \notebook -nodraw
## This tutorial illustrates how to read data from an RDataFrame to Numpy
## arrays. This version of the tutorial uses a local backend.
##
## \macro_code
##
## \date July 2019
## \author Stefan Wunsch (KIT, CERN), Vincenzo Eduardo Padulano (UniMiB, CERN)

import PyRDF
from sys import exit
import sys

# Declare needed C++ code to the interpreter.
PyRDF.include_headers("tutorials/headers/df026_AsNumpyArrays.h")

# Let's create a simple dataframe with ten rows and two columns
df = PyRDF.RDataFrame(10).Define("x", "(int)rdfentry_")\
                         .Define("y", "1.f/(1.f+rdfentry_)")

# Next, we want to access the data from Python as Numpy arrays. To do so, the
# content of the dataframe is converted using the `AsNumpy` method. The
# returned object is a dictionary with the column names as keys and 1D numpy
# arrays with the content as values.
npy = df.AsNumpy()
print("Read-out of the full RDataFrame:\n{}\n".format(npy))

# Since reading out data to memory is expensive, always try to read-out only
# what is needed for your analysis. You can use all `RDataFrame` features to
# reduce your dataset, e.g., the `Filter` transformation. Furthermore, you can
# pass to the `AsNumpy` method a whitelist of column names with the option
# `columns` or a blacklist with column names with the option `exclude`.
df2 = df.Filter("x>5")
npy2 = df2.AsNumpy()
print("Read-out of the filtered RDataFrame:\n{}\n".format(npy2))

npy3 = df2.AsNumpy(columns=["x"])
print("Read-out of the filtered RDataFrame with the columns option:\n{}\n"
      .format(npy3))

npy4 = df2.AsNumpy(exclude=["x"])
print("Read-out of the filtered RDataFrame with the exclude option:\n{}\n"
      .format(npy4))

# You can read-out all objects from ROOT files since these are wrapped by
# PyROOT in the Python world. However, be aware that objects other than
# fundamental types, such as complex C++ objects and not `int` or `float`,
# are costly to read-out.
df3 = df.Define("custom_object", "fill_object()")
npy5 = df3.AsNumpy()
print("Read-out of C++ objects:\n{}\n".format(npy5["custom_object"]))

print("Access to all methods and data members of the C++ object:\nObject:"
      "{}\nAccess data member: custom_object.x = {}\n"
      .format(repr(npy5["custom_object"][0]), npy5["custom_object"][0].x))

# Note that you can pass the object returned by `AsNumpy` directly to
# `pandas.DataFrame` including any complex C++ object that may be read-out.
try:
    import pandas
except:
    print("Failed to import pandas.")
    exit()
df = pandas.DataFrame(npy5)
print("Content of the ROOT.RDataFrame as pandas.DataFrame:\n{}\n".format(df))