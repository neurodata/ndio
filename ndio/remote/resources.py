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

from .neuroRemote import neuroRemote as nd
from .neuroRemote import DEFAULT_HOSTNAME
from .neuroRemote import DEFAULT_SUFFIX
from .neuroRemote import DEFAULT_PROTOCOL
from .neuroRemote import DEFAULT_BLOCK_SIZE


class resources(nd):
    """
    Resource api wrapper class.
    """

    def __init__(self,
                 user_token='placeholder',
                 hostname=DEFAULT_HOSTNAME,
                 protocol=DEFAULT_PROTOCOL,
                 meta_root="http://lims.neurodata.io/",
                 meta_protocol=DEFAULT_PROTOCOL, **kwargs):
        """
        Initializer for resources class.

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
        super(resources, self).__init__(user_token,
                                        hostname,
                                        protocol,
                                        meta_root,
                                        meta_protocol, **kwargs)

    def create_project(self,
                       project_name,
                       dataset_name,
                       hostname,
                       is_public,
                       s3backend=0,
                       kvserver='localhost',
                       kvengine='MySQL',
                       mdengine='MySQL',
                       description=''):
        """
        Creates a project with the given parameters.

        Arguments:
            project_name (str): Project name
            dataset_name (str): Dataset name project is based on
            hostname (str): Hostname
            s3backend (str): S3 region to save the data in
            is_public (int): 1 is public. 0 is not public.
            kvserver (str): Server to store key value pairs in
            kvengine (str): Database to store key value pairs in
            mdengine (str): ???
            description (str): Description for your project

        Returns:
            bool: True if project created, false if not created.
        """
        url = self.url()[:-4] + "/resource/dataset/{}".format(
            dataset_name) + "/project/{}/".format(project_name)

        json = {
            "project_name": project_name,
            "host": hostname,
            "s3backend": s3backend,
            "public": is_public,
            "kvserver": kvserver,
            "kvengine": kvengine,
            "mdengine": mdengine,
            "project_description": description
        }

        req = self.remote_utils.post_url(url, json=json)

        if req.status_code is not 201:
            raise RemoteDataUploadError('Could not upload {}'.format(req.text))
        if req.content == "" or req.content == b'':

            return True
        else:
            return False

    def get_project(self, project_name, dataset_name):
        """
        Get details regarding a project given its name and dataset its linked
        to.

        Arguments:
            project_name (str): Project name
            dataset_name (str): Dataset name project is based on

        Returns:
            dict: Project info
        """
        url = self.url()[:-4] + "/nd/resource/dataset/{}".format(dataset_name)\
            + "/project/{}/".format(project_name)
        req = self.remote_utils.get_url(url)

        if req.status_code is not 200:
            raise RemoteDataNotFoundError('Could not find {}'.format(req.text))
        else:
            return req.json()

    def delete_project(self, project_name, dataset_name):
        """
        Deletes a project once given its name and its related dataset.

        Arguments:
            project_name (str): Project name
            dataset_name (str): Dataset name project is based on

        Returns:
            bool: True if project deleted, False if not.
        """
        url = self.url()[:-4] + "/nd/resource/dataset/{}".format(dataset_name)\
            + "/project/{}/".format(project_name)
        req = self.remote_utils.delete_url(url)

        if req.status_code is not 204:
            raise RemoteDataUploadError('Could not delete {}'.format(req.text))
        if req.content == "" or req.content == b'':
            return True
        else:
            return False

    def list_projects(self, dataset_name):
        """
        Lists a set of projects related to a dataset.

        Arguments:
            dataset_name (str): Dataset name to search projects for

        Returns:
            dict: Projects found based on dataset query
        """
        url = self.url()[:-4] + "/nd/resource/dataset/{}".format(dataset_name)\
            + "/project/"

        req = self.remote_utils.get_url(url)

        if req.status_code is not 200:
            raise RemoteDataNotFoundError('Could not find {}'.format(req.text))
        else:
            return req.json()

    def create_token(self,
                     token_name,
                     project_name,
                     dataset_name,
                     is_public):
        """
        Creates a token with the given parameters.
        Arguments:
            project_name (str): Project name
            dataset_name (str): Dataset name project is based on
            token_name (str): Token name
            is_public (int): 1 is public. 0 is not public
        Returns:
            bool: True if project created, false if not created.
        """
        url = self.url()[:-4] + '/nd/resource/dataset/{}'.format(
            dataset_name) + '/project/{}'.format(project_name) + \
            '/token/{}/'.format(token_name)

        json = {
            "token_name": token_name,
            "public": is_public
        }

        req = self.remote_utils.post_url(url, json=json)

        if req.status_code is not 201:
            raise RemoteDataUploadError('Cout not upload {}:'.format(req.text))
        if req.content == "" or req.content == b'':
            return True
        else:
            return False

    def get_token(self,
                  token_name,
                  project_name,
                  dataset_name):
        """
        Get a token with the given parameters.
        Arguments:
            project_name (str): Project name
            dataset_name (str): Dataset name project is based on
            token_name (str): Token name
        Returns:
            dict: Token info
        """
        url = self.url()[:-4] + "/nd/resource/dataset/{}".format(dataset_name)\
            + "/project/{}".format(project_name)\
            + "/token/{}/".format(token_name)
        req = self.remote_utils.get_url(url)

        if req.status_code is not 200:
            raise RemoteDataUploadError('Could not find {}'.format(req.text))
        else:
            return req.json()

    def delete_token(self,
                     token_name,
                     project_name,
                     dataset_name):
        """
        Delete a token with the given parameters.
        Arguments:
            project_name (str): Project name
            dataset_name (str): Dataset name project is based on
            token_name (str): Token name
            channel_name (str): Channel name project is based on
        Returns:
            bool: True if project deleted, false if not deleted.
        """
        url = self.url()[:-4] + "/nd/resource/dataset/{}".format(dataset_name)\
            + "/project/{}".format(project_name)\
            + "/token/{}/".format(token_name)
        req = self.remote_utils.delete_url(url)

        if req.status_code is not 204:
            raise RemoteDataUploadError("Could not delete {}".format(req.text))
        if req.content == "" or req.content == b'':
            return True
        else:
            return False

    def list_tokens(self):
        """
        Lists a set of tokens that are public in Neurodata.
        Arguments:
        Returns:
            dict: Public tokens found in Neurodata
        """
        url = self.url()[:-4] + "/nd/resource/public/token/"
        req = self.remote_utils.get_url(url)

        if req.status_code is not 200:
            raise RemoteDataNotFoundError('Coud not find {}'.format(req.text))
        else:
            return req.json()

    def create_dataset(self,
                       name,
                       x_img_size,
                       y_img_size,
                       z_img_size,
                       x_vox_res,
                       y_vox_res,
                       z_vox_res,
                       x_offset=0,
                       y_offset=0,
                       z_offset=0,
                       scaling_levels=0,
                       scaling_option=0,
                       dataset_description="",
                       is_public=0):
        """
        Creates a dataset.

        Arguments:
            name (str): Name of dataset
            x_img_size (int): max x coordinate of image size
            y_img_size (int): max y coordinate of image size
            z_img_size (int): max z coordinate of image size
            x_vox_res (float): x voxel resolution
            y_vox_res (float): y voxel resolution
            z_vox_res (float): z voxel resolution
            x_offset (int): x offset amount
            y_offset (int): y offset amount
            z_offset (int): z offset amount
            scaling_levels (int): Level of resolution scaling
            scaling_option (int): Z slices is 0 or Isotropic is 1
            dataset_description (str): Your description of the dataset
            is_public (int): 1 'true' or 0 'false' for viewability of data set
                in public

        Returns:
            bool: True if dataset created, False if not
        """
        url = self.url()[:-4] + "/resource/dataset/{}".format(name)
        json = {
            "dataset_name": name,
            "ximagesize": x_img_size,
            "yimagesize": y_img_size,
            "zimagesize": z_img_size,
            "xvoxelres": x_vox_res,
            "yvoxelres": y_vox_res,
            "zvoxelres": z_vox_res,
            "xoffset": x_offset,
            "yoffset": y_offset,
            "zoffset": z_offset,
            "scalinglevels": scaling_levels,
            "scalingoption": scaling_option,
            "dataset_description": dataset_description,
            "public": is_public
        }
        req = self.remote_utils.post_url(url, json=json)

        if req.status_code is not 201:
            raise RemoteDataUploadError('Could not upload {}'.format(req.text))
        if req.content == "" or req.content == b'':
            return True
        else:
            return False

    def get_dataset(self, name):
        """
        Returns info regarding a particular dataset.

        Arugments:
            name (str): Dataset name

        Returns:
            dict: Dataset information
        """
        url = self.url()[:-4] + "/resource/dataset/{}".format(name)
        req = self.remote_utils.get_url(url)

        if req.status_code is not 200:
            raise RemoteDataNotFoundError('Could not find {}'.format(req.text))
        else:
            return req.json()

    def list_datasets(self, get_global_public):
        """
        Lists datasets in resources. Setting 'get_global_public' to 'True'
        will retrieve all public datasets in cloud. 'False' will get user's
        public datasets.

        Arguments:
            get_global_public (bool): True if user wants all public datasets in
                                      cloud. False if user wants only their
                                      public datasets.

        Returns:
            dict: Returns datasets in JSON format

        """
        appending = ""
        if get_global_public:
            appending = "public"
        url = self.url()[:-4] + "/resource/{}dataset/".format(appending)
        req = self.remote_utils.get_url(url)

        if req.status_code is not 200:
            raise RemoteDataNotFoundError('Could not find {}'.format(req.text))
        else:
            return req.json()

    def delete_dataset(self, name):
        """
        Arguments:
            name (str): Name of dataset to delete

        Returns:
            bool: True if dataset deleted, False if not
        """
        url = self.url()[:-4] + "/resource/dataset/{}".format(name)
        req = self.remote_utils.delete_url(url)

        if req.status_code is not 204:
            raise RemoteDataUploadError('Could not delete {}'.format(req.text))
        if req.content == "" or req.content == b'':
            return True
        else:
            return False

    def _check_channel(self, channel):
        for c in channel:
            if not c.isalnum():
                raise ValueError(
                    "Channel name cannot contain character {}.".format(c)
                )
        return True

    def create_channel(self,
                       channel_name,
                       project_name,
                       dataset_name,
                       channel_type,
                       dtype,
                       startwindow,
                       endwindow,
                       readonly=0,
                       start_time=0,
                       end_time=0,
                       propagate=0,
                       resolution=0,
                       channel_description=''):
        """
        Create a new channel on the Remote, using channel_data.

        Arguments:
            channel_name (str): Channel name
            project_name (str): Project name
            dataset_name (str): Dataset name
            channel_type (str): Type of the channel (e.g. `neurodata.IMAGE`)
            dtype (str): The datatype of the channel's data (e.g. `uint8`)
            startwindow (int): Window to start in
            endwindow (int): Window to end in
            readonly (int): Can others write to this channel?
            propagate (int): Allow propogation? 1 is True, 0 is False
            resolution (int): Resolution scaling
            channel_description (str): Your description of the channel

        Returns:
            bool: `True` if successful, `False` otherwise.

        Raises:
            ValueError: If your args were bad :(
            RemoteDataUploadError: If the channel data is valid but upload
                fails for some other reason.
        """
        self._check_channel(channel_name)

        if channel_type not in ['image', 'annotation', 'timeseries']:
            raise ValueError('Channel type must be ' +
                             'neurodata.IMAGE or neurodata.ANNOTATION.')

        if readonly * 1 not in [0, 1]:
            raise ValueError("readonly must be 0 (False) or 1 (True).")

        # Good job! You supplied very nice arguments.

        url = self.url()[:-4] + "/nd/resource/dataset/{}".format(dataset_name)\
            + "/project/{}".format(project_name) + \
            "/channel/{}/".format(channel_name)
        json = {
            "channel_name": channel_name,
            "channel_type": channel_type,
            "channel_datatype": dtype,
            "startwindow": startwindow,
            "endwindow": endwindow,
            'starttime': start_time,
            'endtime': end_time,
            'readonly': readonly,
            'propagate': propagate,
            'resolution': resolution,
            'channel_description': channel_description
        }
        req = self.remote_utils.post_url(url, json=json)

        if req.status_code is not 201:
            raise RemoteDataUploadError('Could not upload {}'.format(req.text))
        if req.content == "" or req.content == b'':
            return True
        else:
            return False

    def get_channel(self, channel_name, project_name, dataset_name):
        """
        Gets info about a channel given its name, name of its project
        , and name of its dataset.

        Arguments:
            channel_name (str): Channel name
            project_name (str): Project name
            dataset_name (str): Dataset name

        Returns:
            dict: Channel info
        """
        url = self.url()[:-4] + "/nd/resource/dataset/{}".format(dataset_name)\
            + "/project/{}".format(project_name) + \
            "/channel/{}/".format(channel_name)

        req = self.remote_utils.get_url(url)

        if req.status_code is not 200:
            raise RemoteDataNotFoundError('Could not find {}'.format(req.text))
        else:
            return req.json()

    def delete_channel(self, channel_name, project_name, dataset_name):
        """
        Deletes a channel given its name, name of its project
        , and name of its dataset.

        Arguments:
            channel_name (str): Channel name
            project_name (str): Project name
            dataset_name (str): Dataset name

        Returns:
            bool: True if channel deleted, False if not
        """
        url = self.url()[:-4] + "/nd/resource/dataset/{}".format(dataset_name)\
            + "/project/{}".format(project_name) + \
            "/channel/{}/".format(channel_name)

        req = self.remote_utils.delete_url(url)

        if req.status_code is not 204:
            raise RemoteDataUploadError('Could not delete {}'.format(req.text))
        if req.content == "" or req.content == b'':
            return True
        else:
            return False
