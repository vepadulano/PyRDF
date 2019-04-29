import ROOT

class Utils(object):
    """Class that houses general utility functions."""

    @classmethod
    def declare_headers(cls, includes):
        """
        Declares all required headers using the ROOT's C++ Interpreter.

        parameters
        ----------
        includes : list
            This list should consist of all necessary C++ headers as strings.

        """
        print("\n\n")
        print("Includes list to Utils.declare_headers:", includes)
        print("\n\n")
        for header in includes:
            include_code = "#include \"{}\"\n".format(header)
            if not ROOT.gInterpreter.Declare(include_code):
                msg = "There was an error in including \"{}\" !".format(header)
                raise Exception(msg)
