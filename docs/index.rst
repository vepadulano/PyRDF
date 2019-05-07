.. PyRDF documentation master file, created by
   sphinx-quickstart on Wed May  8 14:18:48 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PyRDF : The Python ROOT DataFrame Library
=========================================

A pythonic wrapper around ROOT's `RDataFrame <https://root.cern/doc/master/classROOT_1_1RDataFrame.html>`_ with support for distributed execution.

Sample usage::

	import PyRDF, ROOT
	PyRDF.use('spark', {'npartitions':4})

	df = PyRDF.RDataFrame("data", ['https://root.cern/files/teaching/CMS_Open_Dataset.root',])

	etaCutStr = "fabs(eta1) < 2.3"
	df_f = df.Filter(etaCutStr)

	df_histogram = df_f.Histo1D("eta1")

	canvas = ROOT.TCanvas()
	df_histogram.Draw()
	canvas.Draw()

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api
   backend
   
Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
