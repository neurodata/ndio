from __future__ import absolute_import
import ndio
import requests
import numpy

from .Remote import Remote
from .errors import *
import ndio.ramon as ramon
from six.moves import range
import six

DEFAULT_HOSTNAME = ""
DEFAULT_PROTOCOL = "http"


class dvid(Remote):

    def __init__(self,
                 hostname=DEFAULT_HOSTNAME,
                 protocol=DEFAULT_PROTOCOL, **kwargs):

        super(dvid, self).__init__(hostname, protocol)

    def __repr__(self):
        """
        Returns a string representation that can be used to reproduce this
        instance. `eval(repr(this))` should return an identical copy.

        Arguments:
            None

        Returns:
            str: Representation of reproducible instance.
        """
        return "ndio.remote.dvid('{}', '{}')".format(
            self.hostname,
            self.protocol
        )

    def get_cutout(self):
        raise NotImplementedError

    def post_cutout(self):
        raise NotImplementedError
