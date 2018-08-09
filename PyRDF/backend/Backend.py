from abc import ABCMeta, abstractmethod

ABC = ABCMeta('ABC', (object,), {})

class Backend(ABC):
    """
    Base class for RDataFrame backends. Subclasses
    of this class need to implement the 'execute' method.

    Attributes
    ----------
    config
        The config object for the required
        backend.

    """
    supported_operations = [
        'Define',
        'Filter',
        'Histo1D',
        'Histo2D',
        'Histo3D',
        'Profile1D',
        'Profile2D',
        'Profile3D',
        'Count',
        'Min',
        'Max',
        'Mean',
        'Sum',
        'Fill',
        'Report',
        'Range',
        'Take',
        'Snapshot',
        'Foreach',
        'Reduce',
        'Aggregate',
        'Graph'
        ]

    def __init__(self, config={}):
        """
        Creates a new instance of the
        desired implementation of `Backend`.

        Parameters
        ----------
        config
            The config object for the required
            backend. The default value is an
            empty Python dictionary `{}`.

        """
        self.config = config

    def check_supported(self, operation_name):
        """
        Checks if a given operation is supported
        by the given backend.

        Parameters
        ----------
        operation_name
            Name of the operation to be checked.

        Raises
        ------
        Exception
            This happens when `operation_name` doesn't
            exist in `supported_operations` instance member.

        """
        if operation_name not in self.supported_operations:
            raise Exception("The current backend doesn't support \"{}\" !".format(operation_name))

    @abstractmethod
    def execute(self, generator):
        pass