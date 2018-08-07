import unittest, PyRDF, ROOT, math

class IncludesSparkTest(unittest.TestCase):
    """
    An integration test to check that the required
    header files are properly included in Spark environment.

    """
    def test_includes_function_with_filter_and_histo(self):
        """
        An integration test to check that the filter
        operation is able to use C++ functions that
        were included using header files.

        """
        PyRDF.include("tests/integration/local/test_headers/header1.hxx")
        PyRDF.use("spark")

        rdf = PyRDF.RDataFrame(10)

        # This filters out all numbers less than 5
        rdf_filtered = rdf.Filter("check_number_less_than_5(tdfentry_)")
        histo = rdf_filtered.Histo1D("tdfentry_")

        # The expected results after filtering
        required_numbers = range(5) # The actual set of numbers required after filtering
        required_size = len(required_numbers)
        required_mean = sum(required_numbers)/float(required_size)
        required_stdDev = math.sqrt(sum((x - required_mean)**2 for x in required_numbers)/required_size)

        # Compare the sizes of equivalent set of numbers
        self.assertEqual(histo.GetEntries(), float(required_size))

        # Compare the means of equivalent set of numbers
        self.assertEqual(histo.GetMean(), required_mean)

        # Compare the standard deviations of equivalent set of numbers
        self.assertEqual(histo.GetStdDev(), required_stdDev)