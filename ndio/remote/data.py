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

from .neuroRemote import neuroRemote
from .neuroRemote import DEFAULT_HOSTNAME
from .neuroRemote import DEFAULT_SUFFIX
from .neuroRemote import DEFAULT_PROTOCOL
from .neuroRemote import DEFAULT_BLOCK_SIZE

from .metadata import metadata


class data(neuroRemote, metadata):
    """
    Data class with data wrappers for ndio.
    """

    def __init__(self,
                 user_token='placeholder',
                 hostname=DEFAULT_HOSTNAME,
                 protocol=DEFAULT_PROTOCOL,
                 meta_root="http://lims.neurodata.io/",
                 meta_protocol=DEFAULT_PROTOCOL, **kwargs):
        """
        Initializer for the data class.

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
        super(data, self).__init__(user_token,
                                   hostname,
                                   protocol,
                                   meta_root,
                                   meta_protocol, **kwargs)

    # SECTION:
    # Data Download

    def get_block_size(self, token, resolution=None):
        """
        Gets the block-size for a given token at a given resolution.

        Arguments:
            token (str): The token to inspect
            resolution (int : None): The resolution at which to inspect data.
                If none is specified, uses the minimum available.

        Returns:
            int[3]: The xyz blocksize.
        """
        cdims = self.get_metadata(token)['dataset']['cube_dimension']
        if resolution is None:
            resolution = min(cdims.keys())
        return cdims[str(resolution)]

    def get_image_offset(self, token, resolution=0):
        """
        Gets the image offset for a given token at a given resolution. For
        instance, the `kasthuri11` dataset starts at (0, 0, 1), so its 1850th
        slice is slice 1850, not 1849. When downloading a full dataset, the
        result of this function should be your x/y/z starts.

        Arguments:
            token (str): The token to inspect
            resolution (int : 0): The resolution at which to gather the offset

        Returns:
            int[3]: The origin of the dataset, as a list
        """
        info = self.get_proj_info(token)
        res = str(resolution)
        if res not in info['dataset']['offset']:
            raise RemoteDataNotFoundError("Resolution " + res +
                                          " is not available.")
        return info['dataset']['offset'][str(resolution)]

    def get_xy_slice(self, token, channel,
                     x_start, x_stop,
                     y_start, y_stop,
                     z_index,
                     resolution=0):
        """
        Return a binary-encoded, decompressed 2d image. You should
        specify a 'token' and 'channel' pair.  For image data, users
        should use the channel 'image.'

        Arguments:
            token (str): Token to identify data to download
            channel (str): Channel
            resolution (int): Resolution level
            Q_start (int):` The lower bound of dimension 'Q'
            Q_stop (int): The upper bound of dimension 'Q'
            z_index (int): The z-slice to image

        Returns:
            str: binary image data
        """
        vol = self.get_cutout(token, channel, x_start, x_stop, y_start,
                              y_stop, z_index, z_index + 1, resolution)

        vol = numpy.squeeze(vol)  # 3D volume to 2D slice

        return vol

    def get_image(self, token, channel,
                  x_start, x_stop,
                  y_start, y_stop,
                  z_index,
                  resolution=0):
        """
        Alias for the `get_xy_slice` function for backwards compatibility.
        """
        return self.get_xy_slice(token, channel,
                                 x_start, x_stop,
                                 y_start, y_stop,
                                 z_index,
                                 resolution)

    def get_volume(self, token, channel,
                   x_start, x_stop,
                   y_start, y_stop,
                   z_start, z_stop,
                   resolution=1,
                   block_size=DEFAULT_BLOCK_SIZE,
                   neariso=False):
        """
        Get a RAMONVolume volumetric cutout from the neurodata server.

        Arguments:
            token (str): Token to identify data to download
            channel (str): Channel
            resolution (int): Resolution level
            Q_start (int): The lower bound of dimension 'Q'
            Q_stop (int): The upper bound of dimension 'Q'
            block_size (int[3]): Block size of this dataset
            neariso (bool : False): Passes the 'neariso' param to the cutout.
                If you don't know what this means, ignore it!

        Returns:
            ndio.ramon.RAMONVolume: Downloaded data.
        """
        size = (x_stop - x_start) * (y_stop - y_start) * (z_stop - z_start)
        volume = ramon.RAMONVolume()
        volume.xyz_offset = [x_start, y_start, z_start]
        volume.resolution = resolution

        volume.cutout = self.get_cutout(token, channel, x_start,
                                        x_stop, y_start, y_stop,
                                        z_start, z_stop,
                                        resolution=resolution,
                                        block_size=block_size,
                                        neariso=neariso)
        return volume

    def get_cutout(self, token, channel,
                   x_start, x_stop,
                   y_start, y_stop,
                   z_start, z_stop,
                   t_start=0, t_stop=1,
                   resolution=1,
                   block_size=DEFAULT_BLOCK_SIZE,
                   neariso=False):
        """
        Get volumetric cutout data from the neurodata server.

        Arguments:
            token (str): Token to identify data to download
            channel (str): Channel
            resolution (int): Resolution level
            Q_start (int): The lower bound of dimension 'Q'
            Q_stop (int): The upper bound of dimension 'Q'
            block_size (int[3]): Block size of this dataset. If not provided,
                ndio uses the metadata of this tokenchannel to set. If you find
                that your downloads are timing out or otherwise failing, it may
                be wise to start off by making this smaller.
            neariso (bool : False): Passes the 'neariso' param to the cutout.
                If you don't know what this means, ignore it!

        Returns:
            numpy.ndarray: Downloaded data.
        """
        if block_size is None:
            # look up block size from metadata
            block_size = self.get_block_size(token, resolution)

        origin = self.get_image_offset(token, resolution)

        # If z_stop - z_start is < 16, backend still pulls minimum 16 slices
        if (z_stop - z_start) < 16:
            z_slices = 16
        else:
            z_slices = z_stop - z_start

        # Calculate size of the data to be downloaded.
        size = (x_stop - x_start) * (y_stop - y_start) * z_slices * 4

        # Switch which download function to use based on which libraries are
        # available in this version of python.
        if six.PY2:
            dl_func = self._get_cutout_blosc_no_chunking
        elif six.PY3:
            dl_func = self._get_cutout_no_chunking
        else:
            raise ValueError("Invalid Python version.")

        if size < self._chunk_threshold:
            vol = dl_func(token, channel, resolution,
                          x_start, x_stop,
                          y_start, y_stop,
                          z_start, z_stop,
                          t_start, t_stop,
                          neariso=neariso)
            vol = numpy.rollaxis(vol, 1)
            vol = numpy.rollaxis(vol, 2)
            return vol
        else:
            from ndio.utils.parallel import block_compute
            blocks = block_compute(x_start, x_stop,
                                   y_start, y_stop,
                                   z_start, z_stop,
                                   origin, block_size)

            vol = numpy.zeros(((z_stop - z_start),
                               (y_stop - y_start),
                               (x_stop - x_start)))
            for b in blocks:

                data = dl_func(token, channel, resolution,
                               b[0][0], b[0][1],
                               b[1][0], b[1][1],
                               b[2][0], b[2][1],
                               0, 1,
                               neariso=neariso)

                if b == blocks[0]:  # first block
                    vol = numpy.zeros(((z_stop - z_start),
                                       (y_stop - y_start),
                                       (x_stop - x_start)), dtype=data.dtype)

                vol[b[2][0] - z_start: b[2][1] - z_start,
                    b[1][0] - y_start: b[1][1] - y_start,
                    b[0][0] - x_start: b[0][1] - x_start] = data

            vol = numpy.rollaxis(vol, 1)
            vol = numpy.rollaxis(vol, 2)
            return vol

    def _get_cutout_no_chunking(self, token, channel, resolution,
                                x_start, x_stop, y_start, y_stop,
                                z_start, z_stop, t_start, t_stop,
                                neariso=False):
        url = self.url() + "{}/{}/hdf5/{}/{},{}/{},{}/{},{}/{},{}".format(
            token, channel, resolution,
            x_start, x_stop,
            y_start, y_stop,
            z_start, z_stop,
            t_start, t_stop,
        )

        if neariso:
            url += "neariso/"

        req = self.remote_utils.get_url(url)
        if req.status_code is not 200:
            raise IOError("Bad server response for {}: {}: {}".format(
                          url,
                          req.status_code,
                          req.text))

        with tempfile.NamedTemporaryFile() as tmpfile:
            tmpfile.write(req.content)
            tmpfile.seek(0)
            h5file = h5py.File(tmpfile.name, "r")
            return h5file.get(channel).get('CUTOUT')[:]
        raise IOError("Failed to make tempfile.")

    def _get_cutout_blosc_no_chunking(self, token, channel, resolution,
                                      x_start, x_stop, y_start, y_stop,
                                      z_start, z_stop, t_start, t_stop,
                                      neariso=False):

        url = self.url() + "{}/{}/blosc/{}/{},{}/{},{}/{},{}/{},{}/".format(
            token, channel, resolution,
            x_start, x_stop,
            y_start, y_stop,
            z_start, z_stop,
            t_start, t_stop,
        )

        if neariso:
            url += "neariso/"

        req = self.remote_utils.get_url(url)
        if req.status_code is not 200:
            raise IOError("Bad server response for {}: {}: {}".format(
                          url,
                          req.status_code,
                          req.text))

        # This will need modification for >3D blocks
        return blosc.unpack_array(req.content)[0]

        raise IOError("Failed to retrieve blosc cutout.")

    # SECTION:
    # Data Upload

    def post_cutout(self, token, channel,
                    x_start,
                    y_start,
                    z_start,
                    data,
                    resolution=0):
        """
        Post a cutout to the server.

        Arguments:
            token (str)
            channel (str)
            x_start (int)
            y_start (int)
            z_start (int)
            data (numpy.ndarray): A numpy array of data. Pass in (x, y, z)
            resolution (int : 0): Resolution at which to insert the data

        Returns:
            bool: True on success

        Raises:
            RemoteDataUploadError: if there's an issue during upload.
        """
        datatype = self.get_proj_info(token)['channels'][channel]['datatype']
        if data.dtype.name != datatype:
            data = data.astype(datatype)

        data = numpy.rollaxis(data, 1)
        data = numpy.rollaxis(data, 2)

        if six.PY3 or data.nbytes > 1.5e9:
            ul_func = self._post_cutout_no_chunking_npz
        else:
            ul_func = self._post_cutout_no_chunking_blosc

        if data.size < self._chunk_threshold:
            return ul_func(token, channel, x_start,
                           y_start, z_start, data,
                           resolution)

        return self._post_cutout_with_chunking(token, channel,
                                               x_start, y_start, z_start, data,
                                               resolution, ul_func)

    def _post_cutout_with_chunking(self, token, channel, x_start,
                                   y_start, z_start, data,
                                   resolution, ul_func):
        # must chunk first
        from ndio.utils.parallel import block_compute
        blocks = block_compute(x_start, x_start + data.shape[2],
                               y_start, y_start + data.shape[1],
                               z_start, z_start + data.shape[0])
        for b in blocks:
            # data coordinate relative to the size of the arra
            subvol = data[b[2][0] - z_start: b[2][1] - z_start,
                          b[1][0] - y_start: b[1][1] - y_start,
                          b[0][0] - x_start: b[0][1] - x_start]
            # upload the chunk:
            # upload coordinate relative to x_start, y_start, z_start
            ul_func(token, channel, b[0][0],
                    b[1][0], b[2][0], subvol,
                    resolution)
        return True

    def _post_cutout_no_chunking_npz(self, token, channel,
                                     x_start, y_start, z_start,
                                     data, resolution):

        data = numpy.expand_dims(data, axis=0)
        tempfile = BytesIO()
        numpy.save(tempfile, data)
        compressed = zlib.compress(tempfile.getvalue())

        url = self.url("{}/{}/npz/{}/{},{}/{},{}/{},{}/".format(
            token, channel,
            resolution,
            x_start, x_start + data.shape[3],
            y_start, y_start + data.shape[2],
            z_start, z_start + data.shape[1]
        ))

        req = self.remote_utils.post_url(url, data=compressed, headers={
            'Content-Type': 'application/octet-stream'
        })

        if req.status_code is not 200:
            raise RemoteDataUploadError(req.text)
        else:
            return True

    def _post_cutout_no_chunking_blosc(self, token, channel,
                                       x_start, y_start, z_start,
                                       data, resolution):
        """
        Accepts data in zyx. !!!
        """
        data = numpy.expand_dims(data, axis=0)
        blosc_data = blosc.pack_array(data)
        url = self.url("{}/{}/blosc/{}/{},{}/{},{}/{},{}/0,0/".format(
            token, channel,
            resolution,
            x_start, x_start + data.shape[3],
            y_start, y_start + data.shape[2],
            z_start, z_start + data.shape[1]
        ))

        req = self.remote_utils.post_url(url, data=blosc_data, headers={
            'Content-Type': 'application/octet-stream'
        })

        if req.status_code is not 200:
            raise RemoteDataUploadError(req.text)
        else:
            return True
