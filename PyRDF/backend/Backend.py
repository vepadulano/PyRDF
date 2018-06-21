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

    @abstractmethod
    def execute(self, generator):
        pass