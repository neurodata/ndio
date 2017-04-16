from __future__ import absolute_import
import ndio
import requests
import os
import numpy
from io import BytesIO
import zlib
import tempfile
import blosc
import h5py
from .remote_utils import remote_utils

from .Remote import Remote
from .errors import *
import ndio.ramon as ramon
from six.moves import range
import six

from functools import wraps

try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

DEFAULT_HOSTNAME = "openconnecto.me"
DEFAULT_SUFFIX = "nd"
DEFAULT_PROTOCOL = "https"  # originally was https in master
DEFAULT_BLOCK_SIZE = (1024, 1024, 16)


class neuroRemote(Remote):
    """
    The neuroRemote remote, for interfacing with ndstore, ndlims, and friends.
    """

    # SECTION:
    # Enumerables
    IMAGE = IMG = 'image'
    ANNOTATION = ANNO = 'annotation'

    def __init__(self,
                 user_token='placeholder',
                 hostname=DEFAULT_HOSTNAME,
                 protocol=DEFAULT_PROTOCOL,
                 meta_root="http://lims.neurodata.io/",
                 meta_protocol=DEFAULT_PROTOCOL, **kwargs):
        """
        Initializer for the neuroRemote remote class.

        Arguments:
            hostname (str: "openconnecto.me"): The hostname to connect to
            protocol (str: "http"): The protocol (http or https) to use
            meta_root (str: "http://lims.neurodata.io/"): The metadata server
            meta_protocol (str: "http"): The protocol to use for the md server
            check_tokens (boolean: False): Whether functions that take `token`
                as an argument should check for the existance of that token and
                fail immediately if it is not found. This is a good idea for
                functions that take more time to complete, and might not fail
                until the very end otherwise.
            chunk_threshold (int: 1e9 / 4): The maximum size of a numpy array
                that will be uploaded in one HTTP request. If you find that
                your data requests are commonly timing out, try reducing this.
                Default is 1e9 / 4, or a 0.25GiB.
            suffix (str: "ocp"): The URL suffix to specify ndstore/microns. If
                you aren't sure what to do with this, don't specify one.
        """
        self._check_tokens = kwargs.get('check_tokens', False)
        self._chunk_threshold = kwargs.get('chunk_threshold', 1E9 / 4)
        self._ext = kwargs.get('suffix', DEFAULT_SUFFIX)
        self._known_tokens = []
        self._user_token = user_token

        # Prepare meta url
        self.meta_root = meta_root
        if not self.meta_root.endswith('/'):
            self.meta_root = self.meta_root + "/"
        if self.meta_root.startswith('https'):
            self.meta_root = self.meta_root[self.meta_root.index('://') + 3:]
        self.meta_protocol = meta_protocol

        self.remote_utils = remote_utils(self._user_token)
        super(neuroRemote, self).__init__(hostname, protocol)

    # SECTION:
    # Legacy code
    def getURL(self, url):
        """
        Old getURL method. Use remote_utils.get_url() instead.

        Arguments:
            url (str): The url to send get request to.

        Returns:
            str: result
        """
        return self.remote_utils.get_url(url)

    # SECTION:
    # Decorators
    def _check_token(f):
        @wraps(f)
        def wrapped(self, *args, **kwargs):
            if self._check_tokens:
                if 'token' in kwargs:
                    token = kwargs['token']
                else:
                    token = args[0]
                if token not in self._known_tokens:
                    if self.ping('{}/info/'.format(token)) != 200:
                        raise RemoteDataNotFoundError("Bad token {}".format(
                                                      token))
                    else:
                        self._known_tokens.append(token)
            return f(self, *args, **kwargs)
        return wrapped

    # SECTION:
    # neuroRemote Utilities
    def ping(self, suffix='public_tokens/'):
        """
        Return the status-code of the API (estimated using the public-tokens
        lookup page).

        Arguments:
            suffix (str : 'public_tokens/'): The url endpoint to check

        Returns:
            int: status code
        """
        return self.remote_utils.ping(super(neuroRemote, self).url(), suffix)

    def url(self, suffix=""):
        """
        Return a constructed URL, appending an optional suffix (uri path).

        Arguments:
            suffix (str : ""): The suffix to append to the end of the URL

        Returns:
            str: The complete URL
        """
        return super(neuroRemote,
                     self).url('{}/sd/'.format(self._ext) + suffix)

    def meta_url(self, suffix=""):
        """
        Return a constructed URL, appending an optional suffix (uri path),
        for the metadata server. (Should be temporary, until the LIMS shim
        is fixed.)

        Arguments:
            suffix (str : ""): The suffix to append to the end of the URL

        Returns:
            str: The complete URL
        """
        return self.meta_protocol + "://" + self.meta_root + suffix

    def __repr__(self):
        """
        Return a string representation that can be used to reproduce this
        instance. `eval(repr(this))` should return an identical copy.

        Arguments:
            None

        Returns:
            str: Representation of reproducible instance.
        """
        return "ndio.remote.neuroRemote('{}', '{}')".format(
            self.hostname,
            self.protocol,
            self.meta_url,
            self.meta_protocol
        )

    # SECTION:
    # ID Manipulation

    @_check_token
    def reserve_ids(self, token, channel, quantity):
        """
        Requests a list of next-available-IDs from the server.

        Arguments:
            quantity (int): The number of IDs to reserve

        Returns:
            int[quantity]: List of IDs you've been granted
        """
        quantity = str(quantity)
        url = self.url("{}/{}/reserve/{}/".format(token, channel, quantity))
        req = self.remote_utils.get_url(url)
        if req.status_code is not 200:
            raise RemoteDataNotFoundError('Invalid req: ' + req.status_code)
        out = req.json()
        return [out[0] + i for i in range(out[1])]

    @_check_token
    def merge_ids(self, token, channel, ids, delete=False):
        """
        Call the restful endpoint to merge two RAMON objects into one.

        Arguments:
            token (str): The token to inspect
            channel (str): The channel to inspect
            ids (int[]): the list of the IDs to merge
            delete (bool : False): Whether to delete after merging.

        Returns:
            json: The ID as returned by ndstore
        """
        url = self.url() + "/merge/{}/".format(','.join([str(i) for i in ids]))
        req = self.remote_utils.get_url(url)
        if req.status_code is not 200:
            raise RemoteDataUploadError('Could not merge ids {}'.format(
                                        ','.join([str(i) for i in ids])))
        if delete:
            self.delete_ramon(token, channel, ids[1:])
        return True

    @_check_token
    def create_channels(self, dataset, token, new_channels_data):
        """
        Creates channels given a dictionary in 'new_channels_data'
        , 'dataset' name, and 'token' (project) name.

        Arguments:
            token (str): Token to identify project
            dataset (str): Dataset name to identify dataset to download from
            new_channels_data (dict): New channel data to upload into new
                channels

        Returns:
            bool: Process completed succesfully or not
        """
        channels = {}
        for channel_new in new_channels_data:

            self._check_channel(channel_new.name)

            if channel_new.channel_type not in ['image', 'annotation']:
                raise ValueError('Channel type must be ' +
                                 'neuroRemote.IMAGE or ' +
                                 'neuroRemote.ANNOTATION.')

            if channel_new.readonly * 1 not in [0, 1]:
                raise ValueError("readonly must be 0 (False) or 1 (True).")

            channels[channel_new.name] = {
                "channel_name": channel_new.name,
                "channel_type": channel_new.channel_type,
                "datatype": channel_new.dtype,
                "readonly": channel_new.readonly * 1
            }
        req = requests.post(self.url("/{}/project/".format(dataset) +
                                     "{}".format(token)),
                            json={"channels": {channels}}, verify=False)

        if req.status_code is not 201:
            raise RemoteDataUploadError('Could not upload {}'.format(req.text))
        else:
            return True

    # Propagation

    # @_check_token
    def propagate(self, token, channel):
        """
        Kick off the propagate function on the remote server.

        Arguments:
            token (str): The token to propagate
            channel (str): The channel to propagate

        Returns:
            boolean: Success
        """
        if self.get_propagate_status(token, channel) != u'0':
            return
        url = self.url('{}/{}/setPropagate/1/'.format(token, channel))
        req = self.remote_utils.get_url(url)
        if req.status_code is not 200:
            raise RemoteDataUploadError('Propagate fail: {}'.format(req.text))
        return True

    # @_check_token
    def get_propagate_status(self, token, channel):
        """
        Get the propagate status for a token/channel pair.

        Arguments:
            token (str): The token to check
            channel (str): The channel to check

        Returns:
            str: The status code
        """
        url = self.url('{}/{}/getPropagate/'.format(token, channel))
        req = self.remote_utils.get_url(url)
        if req.status_code is not 200:
            raise ValueError('Bad pair: {}/{}'.format(token, channel))
        return req.text
