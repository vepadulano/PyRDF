import unittest
import PyRDF


class IncludesLocalTest(unittest.TestCase):
    """Check that the required header files are properly included."""

    def test_include_dir(self):
        """
        Check that the filter operation is able to use C++ functions included
        from a directory of headers
        """
        PyRDF.include("tests/integration/local/test_headers/")

        # creates and RDataFrame with 10 integers [0...9]
        rdf = PyRDF.RDataFrame(10)

        # This filters out all numbers less than 5
        filter1 = rdf.Filter("check_number_less_than_5(tdfentry_)")
        # This filters out all numbers greater than 5
        filter2 = rdf.Filter("check_number_greater_than_5(tdfentry_)")
        count1 = filter1.Count()
        count2 = filter2.Count()

        # The final answer should respectively 5 integers less than 5
        # and 4 integers greater than 5
        self.assertEqual(count1.GetValue(), 5)
        self.assertEqual(count2.GetValue(), 4)
