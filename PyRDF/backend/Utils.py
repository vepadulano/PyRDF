import ROOT

class Utils(object):
    """
    Class that houses general utility
    functions.

    """
    @classmethod
    def declare_headers(cls, includes):
        """
        Declares all required headers using
        PyROOT's "ROOT.gInterpreter.Declare".

        parameters
        ----------
        includes : list
            This list should consist of all necessary C++
            headers as strings.

        """
        for header in includes:
            ROOT.gInterpreter.Declare("#include \"{}\"\n".format(header))