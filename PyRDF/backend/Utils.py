import ROOT


class Utils(object):
    """Class that houses general utility functions."""
    @classmethod
    def declare_headers(cls, headers_to_include):
        """
        Declares all required headers using the ROOT's C++ Interpreter.

        Parameters
        ----------
        headers_to_include : list
            This list should consist of all necessary C++ headers as strings.
        """
        for header in headers_to_include:
            include_code = "#include \"{}\"\n".format(header)
            try:
                ROOT.gInterpreter.Declare(include_code)
            except Exception as e:
                msg = "There was an error in including \"{}\" !".format(header)
                raise e(msg)

    @classmethod
    def declare_shared_libraries(cls, libraries_to_include):
        """
        Declares all required shared libraries using the ROOT's C++
        Interpreter.

        Parameters
        ----------
        libraries_to_include : list
            This list should consist of all necessary C++ shared libraries as
            strings.
        """
        for shared_library in libraries_to_include:
            try:
                ROOT.gSystem.Load(shared_library)
            except Exception as e:
                msg = "There was an error in including \"{}\" !".\
                      format(shared_library)
                raise e(msg)
