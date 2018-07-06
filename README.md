<img width="754" alt="screen shot 2018-07-06 at 8 25 59 pm" src="https://user-images.githubusercontent.com/10980285/42385613-e373af56-815a-11e8-862a-83e1e2ffce93.png">

## PyRDF : The Python ROOT DataFrame Library
![https://travis-ci.org/shravan97/PyRDF.svg?branch=master](https://travis-ci.org/shravan97/PyRDF.svg?branch=master)

A pythonic wrapper around ROOT's [RDataFrame](https://root.cern/doc/master/classROOT_1_1RDataFrame.html) with support for distributed execution.

### Sample usage 
```python
import PyRDF
PyRDF.use('spark', {'npartitions':4})

rdf = PyRDF.RDataFrame("data", ['https://root.cern/files/teaching/CMS_Open_Dataset.root',])

chargeCutStr = "C1 != C2"
etaCutStr = "fabs(eta1) < 2.3 && fabs(eta2) < 2.3"
ptCutStr = "pt1 > 2 && pt2 > 2"
rdf_f = rdf.Filter(chargeCutStr, "Opposite Charge") \
           .Filter(etaCutStr, "Central Muons") \
           .Filter(ptCutStr, "Sane Pt")

num_rows = rdf_f.Count()
print(num_rows.GetValue())
```
### Docs
Coming up soon in [https://pyrdf.readthedocs.io](https://pyrdf.readthedocs.io) !

### Google Summer of Code 2018
This project is a part of [Google Summer of Code 2018](https://summerofcode.withgoogle.com/). Click this [link](https://hepsoftwarefoundation.org/gsoc/2018/proposal_ROOTspark.html) for description.