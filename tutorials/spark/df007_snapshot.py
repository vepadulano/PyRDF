import PyRDF
import ROOT

PyRDF.use("spark")

# A simple helper function to fill a test tree: this makes the example stand-alone.
def fill_tree(treeName, fileName):
    rdf = PyRDF.RDataFrame(10000)
    return rdf.Define("b1", "(int) rdfentry_")\
              .Define("b2", "(float) rdfentry_ * rdfentry_")\
              .Snapshot(treeName, fileName)

# We prepare an input tree to run on
fileName = "df007_snapshot_py.root"
outFileName = "df007_snapshot_output_py.root"
outFileNameAllColumns = "df007_snapshot_output_allColumns_py.root"
treeName = "myTree"

# The tree is snapshotted and we retrieve a new PyRDF.RDataFrame from it
d = fill_tree(treeName, fileName)


# ## Select entries
# We now select some entries in the dataset
d_cut = d.Filter("b1 % 2 == 0")

# ## Enrich the dataset
# Build some temporary columns: we'll write them out
PyRDF.include_headers("tutorials/headers/df007_snapshot.h")
d2 = d_cut.Define("b1_square", "b1 * b1") \
          .Define("b2_vector", "getVector( b2 )")

# We can also get a fresh RDataFrame out of the snapshot and restart the
# analysis chain from it.
branchList = ROOT.vector('string')()
branchList.push_back("b1_square")

snapshot_rdf = d2.Snapshot(treeName, outFileName, branchList);
h = snapshot_rdf.Histo1D("b1_square")
c = ROOT.TCanvas()
h.Draw()
