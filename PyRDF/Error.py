class RDataFrameException(Exception):
    """
    A special type of Exception that shows up for incorrect arguments to
    RDataFrame.
    """

    def __init__(self, exception, msg):
        """
        Creates a new `RDataFrameException`.

        Args:
            exception: An exception of type :obj:`Exception` or any child
                class of :obj:`Exception`.

            msg (str): Message to be printed while raising exception.
        """
        super(RDataFrameException, self).__init__(exception)
        print(msg)
