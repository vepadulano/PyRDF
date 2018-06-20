from __future__ import print_function
from enum import Enum

class Operation(object):
    """
    A Generic representation of an operation. The
    operation could be a transformation or an action.

    Attributes
    ----------
    Types
        A class member that is an Enum of the types
        of operations supported. This can be ACTION
        or TRANSFORMATION.

    name
        Name of the current operation.

    args
        Variable length argument list for the current
        operation.

    kwargs
        Arbitrary keyword arguments for the current
        operation.

    op_type
        The type or category of the current
        operation (ACTION OR TRANSFORMATION).


    Supported operations
    --------------------

    - Define
    - Filter
    - Histo1D
    - Histo2D
    - Histo3D
    - Profile1D
    - Profile2D
    - Profile3D
    - Count
    - Min
    - Max
    - Mean
    - Sum
    - Fill
    - Report


    Upcoming (future) operations
    ----------------------------

    - Range
    - Take
    - Snapshot
    - Foreach

    """

    Types = Enum("Types", "ACTION TRANSFORMATION")

    def __init__(self, name, *args, **kwargs):
        """
        Creates a new `Operation` for the given name
        and arguments.

        Parameters
        ----------
        name : str
            Name of the current operation

        *args
            Variable length argument list for the current
            operation.

        **kwargs
            Keyword arguments for the current
            operation.

        """
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.op_type = self._classify_operation(name)

    def _classify_operation(self, name):
        # Classifies the given operation as action or
        # transformation and returns the type.

        future_ops = [
        "Range",
        "Take",
        "Snapshot",
        "Foreach"
        ]

        if name in future_ops:
            raise NotImplementedError("This operation ({}) isn't supported yet ! ".format(name))

        ops = Operation.Types

        operations_dict = {
        'Define':ops.TRANSFORMATION,
        'Filter':ops.TRANSFORMATION,
        'Histo1D':ops.ACTION,
        'Histo2D':ops.ACTION,
        'Histo3D':ops.ACTION,
        'Profile1D':ops.ACTION,
        'Profile2D':ops.ACTION,
        'Profile3D':ops.ACTION,
        'Count':ops.ACTION,
        'Min':ops.ACTION,
        'Max':ops.ACTION,
        'Mean':ops.ACTION,
        'Sum':ops.ACTION,
        'Fill':ops.ACTION,
        'Report':ops.ACTION
        }
        
        op_type = operations_dict.get(name)

        if not op_type:
            raise Exception("Invalid operation \"{}\"".format(name))
        return op_type

    def is_action(self):
        """
        Checks if the current operation is an action.

        Returns
        -------
        True
            if the current operation is an action, False otherwise.

        """
        return self.op_type == Operation.Types.ACTION

    def is_transformation(self):
        """
        Checks if the current operation is a transformation.

        Returns
        -------
        True
            if the current operation is a transformation, False otherwise.

        """
        return self.op_type == Operation.Types.TRANSFORMATION
