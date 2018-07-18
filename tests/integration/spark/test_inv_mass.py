import unittest, PyRDF

class SparkHistogramsTest(unittest.TestCase):
    """
    Integration tests to check the working of PyRDF.

    """
    @classmethod
    def tearDown(cls):
        """
        Cleans up the `SparkContext` objects
        that were created.

        """
        from pyspark import SparkContext
        context = SparkContext.getOrCreate()
        context.stop()

    def build_pyrdf_graph(self):
        """
        Creates a PyRDF graph with a fixed
        set of operations and returns it.

        """
        rdf = PyRDF.RDataFrame("data", ['https://root.cern/files/teaching/CMS_Open_Dataset.root',])

        # Define the analysis cuts
        chargeCutStr = "C1 != C2"
        etaCutStr = "fabs(eta1) < 2.3 && fabs(eta2) < 2.3"
        ptCutStr = "pt1 > 2 && pt2 > 2"
        rdf_f = rdf.Filter(chargeCutStr, "Opposite Charge") \
                   .Filter(etaCutStr, "Central Muons") \
                   .Filter(ptCutStr, "Sane Pt")
                
        # Create the invariant mass column
        invMassFormulaStr = "sqrt(pow(E1+E2, 2) - (pow(px1+px2, 2) + pow(py1+py2, 2) + pow(pz1+pz2, 2)))"
        rdf_fd = rdf_f.Define("invMass", invMassFormulaStr)
        
        # Create the histograms
        pt1_h = rdf.Histo1D(("","",128,1,1200), "pt1")
        pt2_h = rdf.Histo1D(("","",128,1,1200), "pt2")
        invMass_h = rdf_fd.Histo1D(("invMass","CMS Opendata;#mu#mu mass [GeV];Events",512,5,110), "invMass")
        import ROOT
        pi = ROOT.TMath.Pi()
        phis_h = rdf_fd.Histo2D(("", "", 64, -pi, pi, 64, -pi, pi), "phi1", "phi2")

        return pt1_h, pt2_h, invMass_h, phis_h

    def test_spark_histograms(self):
        """
        Integration test to check that Spark
        backend works the same way as local.

        """
        # Spark execution
        PyRDF.use("spark", {'npartitions':5})
        pt1_h_spark, pt2_h_spark, invMass_h_spark, phis_h_spark = self.build_pyrdf_graph()
        pt1_h_spark.Draw("PL PLC PMC") # Trigger Event-loop, Spark

        # Local execution
        PyRDF.use("local")
        pt1_h_local, pt2_h_local, invMass_h_local, phis_h_local = self.build_pyrdf_graph()
        pt1_h_local.Draw("PL PLC PMC") # Trigger Event-loop, Local

        # Assert 'pt1_h' histogram
        self.assertEqual(pt1_h_spark.GetEntries(), pt1_h_local.GetEntries())
        # Assert 'pt2_h' histogram
        self.assertEqual(pt2_h_spark.GetEntries(), pt2_h_local.GetEntries())
        # Assert 'invMass_h' histogram
        self.assertEqual(invMass_h_spark.GetEntries(), invMass_h_local.GetEntries())
        # Assert 'phis_h' histogram
        self.assertEqual(phis_h_spark.GetEntries(), phis_h_local.GetEntries())
