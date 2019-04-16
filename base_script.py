import PyRDF
import ROOT
import gc
import sys
import objgraph
import pprint
import math

d = PyRDF.RDataFrame(10)



f = d.Filter("tdfentry_ < 5")



f = d.Filter("tdfentry_ > 5").Define("z", "tdfentry_ * 2")



r = d.Define("p","tdfentry_ - 1")


r = d.Define("x", "tdfentry_ - 5")



h = f.Histo1D("z")

# by_type = objgraph.by_type("Node")
# def extra_info(obj=None):
#         try:
#             return(obj.operation.name)
#         except:
#             pass
# objgraph.show_backrefs(by_type, refcounts=True, extra_info=extra_info,
#                        filename='/home/PyRDF/graphs/base_script.png')

h.Draw()

# PyRDF.include("tests/integration/local/test_headers/header1.hxx")
# PyRDF.use("spark", {"npartitions":10})

# rdf = PyRDF.RDataFrame(100)
# print("RDF inside script", rdf)

# # This filters out all numbers less than 5
# rdf_filtered = rdf.Filter("check_number_less_than_5(tdfentry_)")
# histo = rdf_filtered.Histo1D("tdfentry_")

# # The expected results after filtering
# # The actual set of numbers required after filtering
# required_numbers = range(5)
# required_size = len(required_numbers)
# required_mean = sum(required_numbers) / float(required_size)
# required_stdDev = math.sqrt(sum((x - required_mean)**2
#                             for x in required_numbers) / required_size)

# entries = histo.GetEntries()
# print(entries)