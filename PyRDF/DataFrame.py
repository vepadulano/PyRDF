from __future__ import print_function

import logging

import ROOT

from PyRDF import Node
from PyRDF import Proxy

logger = logging.getLogger(__name__)


class DistDataFrame(object):
    """
    Interface to an RDataFrame that can run its computation graph distributedly.
    """

    def __init__(self, headnode, backend, **kwargs):
        """Initialization of """

        # Early check for correctness of arguments to the RDataFrame constructor
        ROOT.RDataFrame(*args)

        self._headnode = Node.HeadNode(*args)

        self._headnode.backend = backend

        self._headnode.npartitions = kwargs.get("npartitions", 2)

        self._headproxy = Proxy.TransformationProxy(self._headnode)

    def __dir__(self):
        opdir = self._headnode.backend.supported_operations + super().__dir__()
        opdir.sort()
        return opdir

    def __getattr__(self, attr):
        """getattr"""
        return getattr(self._headproxy, attr)
