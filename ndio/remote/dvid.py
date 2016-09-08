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
    """
    Handles data transfer with a DVID server.
    """

    def __init__(self,
                 hostname=DEFAULT_HOSTNAME,
                 protocol=DEFAULT_PROTOCOL, **kwargs):
        """
        Initialize the new DVID remote, taking usual args.
        """
        self._ext = "api"
        if hostname == "":
            raise ValueError("You must specify a hostname.")
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
        return super(dvid, self).url('{}/'.format(self._ext) + suffix)

    def get_repositories(self, deep=False):
        """
        Gets a list of repositories on this server.

        Arguments:
            deep (bool : False): Pass True to get the full JSON instead of
                just the keys.

        Returns:
            list of UUIDs
        """
        res = requests.get(self.url("repos/info"))
        if res.status_code != 200:
            raise RemoteDataNotFoundError("Failed to connect to DVID.")

        if deep:
            return res.json()

        return [i for i in res.json().keys()]

    def get_repos(self, deep=False):
        """
        Alias for get_repositories
        """
        return self.get_repositories(deep)

    def get_repository_info(self, uuid):
        """
        Returns JSON information about a particular repository.

        Arguments:
            uuid (str): A unique identifier for a repo

        Returns:
            JSON
        """
        res = requests.get(self.url("repo/{}/info".format(uuid)))

        if res.status_code != 200:
            raise RemoteDataNotFoundError("Can't find UUID {}.".format(uuid))

        return res.json()

    def get_repository_nodes(self, uuid):
        """
        Returns a list of node UUIDs for a given repository.

        Arguments:
            uuid (str): The repo identifier

        Returns:
            list (str)
        """
        return list(self.get_repository_info(uuid)['DAG']['Nodes'].keys())

    def get_data_info(self, uuid, dataname):
        """
        Gets DVID data for the specified dataname.

        Arguments:
            uuid (str): Hexidecimal to uniquely identify a version node
            dataname (str): Name of labelvoldata

        Returns:
            json
        """
        res = requests.get(self.url('node/{}/{}/info'.format(uuid, dataname)))
        return res.json()

    def get_data_metadata(self, uuid, dataname):
        """
        Gets DVID metadata for the specified dataname.

        Arguments:
            uuid (str): Hexidecimal to uniquely identify a version node
            dataname (str): Name of labelvoldata

        Returns:
            json
        """
        res = requests.get(
            self.url('node/{}/{}/metadata'.format(uuid, dataname))
        )
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
        url = self.url('node/{}/{}/sparsevol/{}'.format(uuid, dataname, label))

        # Add options after a '?'
        url += "?" + "&".join(
            ["{}={}".format(str(k), str(v)) for k, v in six.iteritems(options)]
        )

        res = requests.get(url)
        import pdb
        pdb.set_trace()

    def post_cutout(self):
        """
        Not implemented.
        """
        raise NotImplementedError

    def get_label(self):
        """
        Not implemented.
        """
        raise NotImplementedError

    def post_label(self):
        """
        Not implemented.
        """
        raise NotImplementedError
