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

    def url(self, suffix=""):
        """
        Returns a constructed URL, appending an optional suffix (uri path).

        Arguments:
            suffix (str : ""): The suffix to append to the end of the URL

        Returns:
            str: The complete URL
        """
        return super(dvid, self).url('{}/node/'.format(self._ext) + suffix)

    def get_data_info(self, uuid, dataname):
        """
        Gets DVID data for the specified dataname.

        Arguments:
            uuid (str): Hexidecimal to uniquely identify a version node
            dataname (str): Name of labelvoldata

        Returns:
            json
        """
        res = requests.get(self.url('{}/{}/info'.format(uuid, dataname)))
        return res.json()

    def get_cutout(self, uuid, dataname, label, options={}):
        """
        Gets a cutout and populates a dense numpy.ndarray. For a sparse voxel
        list, use ndio.remote.dvid#get_voxel_list().

        Arguments:
            uuid (str): Hexidecimal to uniquely identify a version node
            dataname (str): Name of labelvoldata
            label (str): The label to download
            options (dict): Options to pass to the GET url. See the docs at
                https://goo.gl/qatye6 (DVID repo on GitHub) for values.

        Returns:
            numpy.ndarray
        """
        url = self.url('{}/{}/sparsevol/{}'.format(uuid, dataname, label))
        
        # Add options after a '?'
        url += "?" + "&".join(
            ["{}={}".format(str(k), str(v)) for k, v in six.iteritems(options)]
        )

        res = requests.get(url)


    def post_cutout(self):
        raise NotImplementedError

    def get_label(self):
        raise NotImplementedError

    def post_label(self):
        raise NotImplementedError
