<img width="754" alt="screen shot 2018-07-06 at 8 25 59 pm" src="https://user-images.githubusercontent.com/10980285/42385613-e373af56-815a-11e8-862a-83e1e2ffce93.png">

## PyRDF : The Python ROOT DataFrame Library

[![Build Status](https://travis-ci.org/JavierCVilla/PyRDF.svg?branch=master)](https://travis-ci.org/JavierCVilla/PyRDF)
[![Documentation Status](https://readthedocs.org/projects/pyrdf/badge/?version=latest)](https://pyrdf.readthedocs.io/en/latest/?badge=latest)


A pythonic wrapper around ROOT's [RDataFrame](https://root.cern/doc/master/classROOT_1_1RDataFrame.html) with support for distributed execution.

### Sample usage
```python
import PyRDF, ROOT
PyRDF.use('spark', {'npartitions':4})

df = PyRDF.RDataFrame("data", ['https://root.cern/files/teaching/CMS_Open_Dataset.root',])

etaCutStr = "fabs(eta1) < 2.3"
df_f = df.Filter(etaCutStr)

df_histogram = df_f.Histo1D("eta1")

canvas = ROOT.TCanvas()
df_histogram.Draw()
canvas.Draw()

```

### Help and Support

- [Documentation](https://pyrdf.readthedocs.io/en/latest/)
- [Tutorials](https://github.com/JavierCVilla/PyRDF/tree/master/demos)

### Report
[http://shravanmurali.com/PyRDF/](http://shravanmurali.com/PyRDF/)
