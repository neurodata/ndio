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

class metadata():

        def __init__(self,
                     user_token):
            self.remote_utils = remote_utils(user_token)

        # SECTION:
        # Metadata
        def get_public_tokens(self):
            """
            Get a list of public tokens available on this server.

            Arguments:
                None

            Returns:
                str[]: list of public tokens
            """
            r = self.remote_utils.get_url(self.url() + "public_tokens/")
            return r.json()

        def get_public_datasets(self):
            """
            NOTE: VERY SLOW!
            Get a list of public datasets. Different than public tokens!

            Arguments:
                None

            Returns:
                str[]: list of public datasets
            """
            return list(self.get_public_datasets_and_tokens().keys())

        def get_public_datasets_and_tokens(self):
            """
            NOTE: VERY SLOW!
            Get a dictionary relating key:dataset to value:[tokens] that rely
            on that dataset.

            Arguments:
                None

            Returns:
                dict: relating key:dataset to value:[tokens]
            """
            datasets = {}
            tokens = self.get_public_tokens()
            for t in tokens:
                dataset = self.get_token_dataset(t)
                if dataset in datasets:
                    datasets[dataset].append(t)
                else:
                    datasets[dataset] = [t]
            return datasets


        def get_token_dataset(self, token):
            """
            Get the dataset for a given token.

            Arguments:
                token (str): The token to inspect

            Returns:
                str: The name of the dataset
            """
            return self.get_proj_info(token)['dataset']['description']


        def get_proj_info(self, token):
            """
            Return the project info for a given token.

            Arguments:
                token (str): Token to return information for

            Returns:
                JSON: representation of proj_info
            """
            r = self.remote_utils.get_url(self.url() + "{}/info/".format(token))
            return r.json()


        def get_metadata(self, token):
            """
            An alias for get_proj_info.
            """
            return self.get_proj_info(token)


        def get_channels(self, token):
            """
            Wraps get_proj_info to return a dictionary of just the channels of
            a given project.

            Arguments:
                token (str): Token to return channels for

            Returns:
                JSON: dictionary of channels.
            """
            return self.get_proj_info(token)['channels']


        def get_image_size(self, token, resolution=0):
            """
            Return the size of the volume (3D). Convenient for when you want
            to download the entirety of a dataset.

            Arguments:
                token (str): The token for which to find the dataset image bounds
                resolution (int : 0): The resolution at which to get image bounds.
                    Defaults to 0, to get the largest area available.

            Returns:
                int[3]: The size of the bounds. Should == get_volume.shape

            Raises:
                RemoteDataNotFoundError: If the token is invalid, or if the
                    metadata at that resolution is unavailable in projinfo.
            """
            info = self.get_proj_info(token)
            res = str(resolution)
            if res not in info['dataset']['imagesize']:
                raise RemoteDataNotFoundError("Resolution " + res +
                                              " is not available.")
            return info['dataset']['imagesize'][str(resolution)]


        def set_metadata(self, token, data):
            """
            Insert new metadata into the OCP metadata database.

            Arguments:
                token (str): Token of the datum to set
                data (str): A dictionary to insert as metadata. Include `secret`.

            Returns:
                json: Info of the inserted ID (convenience) or an error message.

            Throws:
                RemoteDataUploadError: If the token is already populated, or if
                    there is an issue with your specified `secret` key.
            """
            req = requests.post(self.meta_url("metadata/ocp/set/" + token),
                                json=data, verify=False)

            if req.status_code != 200:
                raise RemoteDataUploadError(
                    "Could not upload metadata: " + req.json()['message']
                )
            return req.json()


        def get_subvolumes(self, token):
            """
            Return a list of subvolumes taken from LIMS, if available.

            Arguments:
                token (str): The token to read from in LIMS

            Returns:
                dict: or None if unavailable
            """
            md = self.get_metadata(token)['metadata']
            if 'subvolumes' in md:
                return md['subvolumes']
            else:
                return None


        def add_subvolume(self, token, channel, secret,
                          x_start, x_stop,
                          y_start, y_stop,
                          z_start, z_stop,
                          resolution, title, notes):
            """
            Adds a new subvolume to a token/channel.

            Arguments:
                token (str): The token to write to in LIMS
                channel (str): Channel to add in the subvolume. Can be `None`
                x_start (int): Start in x dimension
                x_stop (int): Stop in x dimension
                y_start (int): Start in y dimension
                y_stop (int): Stop in y dimension
                z_start (int): Start in z dimension
                z_stop (int): Stop in z dimension
                resolution (int): The resolution at which this subvolume is seen
                title (str): The title to set for the subvolume
                notes (str): Optional extra thoughts on the subvolume

            Returns:
                boolean: success
            """
            md = self.get_metadata(token)['metadata']
            if 'subvolumes' in md:
                subvols = md['subvolumes']
            else:
                subvols = []

            subvols.append({
                'token': token,
                'channel': channel,
                'x_start': x_start,
                'x_stop': x_stop,
                'y_start': y_start,
                'y_stop': y_stop,
                'z_start': z_start,
                'z_stop': z_stop,
                'resolution': resolution,
                'title': title,
                'notes': notes
            })

            return self.set_metadata(token, {
                'secret': secret,
                'subvolumes': subvols
            })
