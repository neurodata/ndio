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
# import ndio.ramon as ramon
from six.moves import range
import six

from functools import wraps

try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

from .neuroRemote import neuroRemote
from .neuroRemote import DEFAULT_HOSTNAME
from .neuroRemote import DEFAULT_SUFFIX
from .neuroRemote import DEFAULT_PROTOCOL
from .neuroRemote import DEFAULT_BLOCK_SIZE

from .data import data
from ndio.remote.ramon import ramon
from .resources import resources


class neurodata:
    def __init__(self,
                 user_token='placeholder',
                 hostname=DEFAULT_HOSTNAME,
                 protocol=DEFAULT_PROTOCOL,
                 meta_root="http://lims.neurodata.io/",
                 meta_protocol=DEFAULT_PROTOCOL, **kwargs):
        self.data = data(user_token,
                         hostname,
                         protocol,
                         meta_root,
                         meta_protocol, **kwargs)
        # self.ramon = ramon(user_token,
        #                    hostname,
        #                    protocol,
        #                    meta_root,
        #                    meta_protocol, **kwargs)
        self.resources = resources(user_token,
                                   hostname,
                                   protocol,
                                   meta_root,
                                   meta_protocol, **kwargs)

    # SECTION:
    # Data Download

    def get_block_size(self, token, resolution=None):
        return self.data.get_block_size(token, resolution)


    def get_image_offset(self, token, resolution=0):
        return self.data.get_image_offset(token,resolution)


    def get_xy_slice(self, token, channel,
                     x_start, x_stop,
                     y_start, y_stop,
                     z_index,
                     resolution=0):
        return self.data.get_xy_slice(token, channel,
                         x_start, x_stop,
                         y_start, y_stop,
                         z_index,
                         resolution)

    def get_image(self, token, channel,
                  x_start, x_stop,
                  y_start, y_stop,
                  z_index,
                  resolution=0):
        return self.data.get_image(token, channel,
                      x_start, x_stop,
                      y_start, y_stop,
                      z_index,resolution)

    def get_volume(self, token, channel,
                   x_start, x_stop,
                   y_start, y_stop,
                   z_start, z_stop,
                   resolution=1,
                   block_size=DEFAULT_BLOCK_SIZE,
                   neariso=False):
        return self.data.get_volume(token, channel,
                       x_start, x_stop,
                       y_start, y_stop,
                       z_start, z_stop,
                       resolution, block_size, neariso)

    def get_cutout(self, token, channel,
                   x_start, x_stop,
                   y_start, y_stop,
                   z_start, z_stop,
                   t_start=0, t_stop=1,
                   resolution=1,
                   block_size=DEFAULT_BLOCK_SIZE,
                   neariso=False):
        return self.data.get_cutout(token, channel,
                       x_start, x_stop,
                       y_start, y_stop,
                       z_start, z_stop,
                       t_start, t_stop,
                       resolution,
                       block_size,
                       neariso)

    # SECTION:
    # Data Upload

    def post_cutout(self, token, channel,
                    x_start,
                    y_start,
                    z_start,
                    data,
                    resolution=0):
        return self.data.post_cutout(token, channel,
                        x_start,
                        y_start,
                        z_start,
                        data,
                        resolution)

    #SECTION:
    # Ramon

    # def get_ramon_bounding_box(self, token, channel, r_id, resolution=0):
    #     """
    #     Get the bounding box for a RAMON object (specified by ID).
    #
    #     Arguments:
    #         token (str): Project to use
    #         channel (str): Channel to use
    #         r_id (int): Which ID to get a bounding box
    #         resolution (int : 0): The resolution at which to download
    #
    #     Returns:
    #         (x_start, x_stop, y_start, y_stop, z_start, z_stop): ints
    #     """
    #     return self.ramon.get_ramon_bounding_box(token, channel, r_id,
    #                                              resolution)
    #
    #
    # def get_ramon_ids(self, token, channel, ramon_type=None):
    #     """
    #     Return a list of all IDs available for download from this token and
    #     channel.
    #
    #     Arguments:
    #         token (str): Project to use
    #         channel (str): Channel to use
    #         ramon_type (int : None): Optional. If set, filters IDs and only
    #             returns those of RAMON objects of the requested type.
    #
    #     Returns:
    #         int[]: A list of the ids of the returned RAMON objects
    #
    #     Raises:
    #         RemoteDataNotFoundError: If the channel or token is not found
    #     """
    #     return self.ramon.get_ramon_ids(token, channel, ramon_type)
    #
    # def get_ramon(self, token, channel, ids, resolution=0,
    #               include_cutout=False, sieve=None, batch_size=100):
    #     """
    #     Download a RAMON object by ID.
    #
    #     Arguments:
    #         token (str): Project to use
    #         channel (str): The channel to use
    #         ids (int, str, int[], str[]): The IDs of a RAMON object to gather.
    #             Can be int (3), string ("3"), int[] ([3, 4, 5]), or string
    #             (["3", "4", "5"]).
    #         resolution (int : None): Resolution. Defaults to the most granular
    #             resolution (0 for now)
    #         include_cutout (bool : False):  If True, r.cutout is populated
    #         sieve (function : None): A function that accepts a single ramon
    #             and returns True or False depending on whether you want that
    #             ramon object to be included in your response or not.
    #             For example,
    #             ```
    #             def is_even_id(ramon):
    #                 return ramon.id % 2 == 0
    #             ```
    #             You can then pass this to get_ramon like this:
    #             ```
    #             ndio.remote.neuroRemote.get_ramon( . . . , sieve=is_even_id)
    #             ```
    #         batch_size (int : 100): The amount of RAMON objects to download at
    #             a time. If this is greater than 100, we anticipate things going
    #             very poorly for you. So if you set it <100, ndio will use it.
    #             If >=100, set it to 100.
    #
    #     Returns:
    #         ndio.ramon.RAMON[]: A list of returned RAMON objects.
    #
    #     Raises:
    #         RemoteDataNotFoundError: If the requested ids cannot be found.
    #     """
    #     return self.ramon.get_ramon(token, channel, ids, resolution,
    #                   include_cutout, sieve, batch_size)
    #
    # def get_ramon_metadata(self, token, channel, anno_id):
    #     """
    #     Download a RAMON object by ID. `anno_id` can be a string `"123"`, an
    #     int `123`, an array of ints `[123, 234, 345]`, an array of strings
    #     `["123", "234", "345"]`, or a comma-separated string list
    #     `"123,234,345"`.
    #
    #     Arguments:
    #         token (str): Project to use
    #         channel (str): The channel to use
    #         anno_id: An int, a str, or a list of ids to gather
    #
    #     Returns:
    #         JSON. If you pass a single id in str or int, returns a single datum
    #         If you pass a list of int or str or a comma-separated string, will
    #         return a dict with keys from the list and the values are the JSON
    #         returned from the server.
    #
    #     Raises:
    #         RemoteDataNotFoundError: If the data cannot be found on the Remote
    #     """
    #     return self.ramon.get_ramon_metadata(token, channel, anno_id)
    #
    # def delete_ramon(self, token, channel, anno):
    #     """
    #     Deletes an annotation from the server. Probably you should be careful
    #     with this function, it seems dangerous.
    #
    #     Arguments:
    #         token (str): The token to inspect
    #         channel (str): The channel to inspect
    #         anno (int OR list(int) OR RAMON): The annotation to delete. If a
    #             RAMON object is supplied, the remote annotation will be deleted
    #             by an ID lookup. If an int is supplied, the annotation will be
    #             deleted for that ID. If a list of ints are provided, they will
    #             all be deleted.
    #
    #     Returns:
    #         bool: Success
    #     """
    #     return self.ramon.delete_ramon(token, channel, anno)
    #
    #
    # def post_ramon(self, token, channel, r, batch_size=100):
    #     """
    #     Posts a RAMON object to the Remote.
    #
    #     Arguments:
    #         token (str): Project to use
    #         channel (str): The channel to use
    #         r (RAMON or RAMON[]): The annotation(s) to upload
    #         batch_size (int : 100): The number of RAMONs to post simultaneously
    #             at maximum in one file. If len(r) > batch_size, the batch will
    #             be split and uploaded automatically. Must be less than 100.
    #
    #     Returns:
    #         bool: Success = True
    #
    #     Throws:
    #         RemoteDataUploadError: if something goes wrong
    #     """
    #     return self.ramon.post_ramon(token, channel, r, batch_size)

    #SECTION
    #Resources: Projects

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
        return self.resources.create_project(project_name,
                                             dataset_name,
                                             hostname,
                                             is_public,
                                             s3backend,
                                             kvserver,
                                             kvengine,
                                             mdengine,
                                             description)

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
        return self.resources.get_project(project_name, dataset_name)

    def delete_project(self, project_name, dataset_name):
        """
        Deletes a project once given its name and its related dataset.

        Arguments:
            project_name (str): Project name
            dataset_name (str): Dataset name project is based on

        Returns:
            bool: True if project deleted, False if not.
        """
        return self.resources.delete_project(project_name, dataset_name)

    def list_projects(self, dataset_name):
        """
        Lists a set of projects related to a dataset.

        Arguments:
            dataset_name (str): Dataset name to search projects for

        Returns:
            dict: Projects found based on dataset query
        """
        return self.resources.list_projects(dataset_name)

    def create_project_token(self, dataset_name, project_name,
                             token_name, is_public):
        """
        Creates and assigns a new token for a project.

        Arguments:
            dataset_name (str):
            project_name (str):
            token_name (str):
            is_public (int):

        Returns:
            bool: True if project deleted. False if not.
        """
        return self.resources.create_project_token(dataset_name, project_name,
                                 token_name, is_public)

    #SECTION
    #Resources: Datasets

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
        return self.resources.create_dataset(name,
                                             x_img_size,
                                             y_img_size,
                                             z_img_size,
                                             x_vox_res,
                                             y_vox_res,
                                             z_vox_res,
                                             x_offset,
                                             y_offset,
                                             z_offset,
                                             scaling_levels,
                                             scaling_option,
                                             dataset_description,
                                             is_public)


    def get_dataset(self, name):
        """
        Returns info regarding a particular dataset.

        Arugments:
            name (str): Dataset name

        Returns:
            dict: Dataset information
        """
        return self.resources.get_dataset(name)


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
        return self.resources.list_datasets(get_global_public)

    def delete_dataset(self, name):
        """
        Arguments:
            name (str): Name of dataset to delete

        Returns:
            bool: True if dataset deleted, False if not
        """
        return self.resources.delete_dataset(name)

    #SECTION
    #Resources: Channels

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
        return self.resources.create_channel(channel_name,
                                             project_name,
                                             dataset_name,
                                             channel_type,
                                             dtype,
                                             startwindow,
                                             endwindow,
                                             readonly,
                                             start_time,
                                             end_time,
                                             propagate,
                                             resolution,
                                             channel_description)

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
        return self.resources.get_channel(channel_name, project_name,
                                          dataset_name)

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
        return self.resources.delete_channel(channel_name, project_name,
                                             dataset_name)
