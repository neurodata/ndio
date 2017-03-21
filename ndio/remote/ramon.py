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

from .neurodata import neurodata
from .neurodata import DEFAULT_HOSTNAME
from .neurodata import DEFAULT_SUFFIX
from .neurodata import DEFAULT_PROTOCOL
from .neurodata import DEFAULT_BLOCK_SIZE

class ramon(neurodata):
    def __init__(self,
                 user_token='placeholder',
                 hostname=DEFAULT_HOSTNAME,
                 protocol=DEFAULT_PROTOCOL,
                 meta_root="http://lims.neurodata.io/",
                 meta_protocol=DEFAULT_PROTOCOL, **kwargs):
        super(resources, self).__init__(user_token,
                                        hostname,
                                        protocol,
                                        meta_root,
                                        meta_protocol, **kwargs)


        @_check_token
        def get_ramon_bounding_box(self, token, channel, r_id, resolution=0):
            """
            Get the bounding box for a RAMON object (specified by ID).

            Arguments:
                token (str): Project to use
                channel (str): Channel to use
                r_id (int): Which ID to get a bounding box
                resolution (int : 0): The resolution at which to download

            Returns:
                (x_start, x_stop, y_start, y_stop, z_start, z_stop): ints
            """
            url = self.url('{}/{}/{}/boundingbox/{}/'.format(token, channel,
                                                             r_id, resolution))

            r_id = str(r_id)
            res = self.remote_utils.get_url(url)

            if res.status_code != 200:
                rt = self.get_ramon_metadata(token, channel, r_id)[r_id]['type']
                if rt in ['neuron']:
                    raise ValueError("ID {} is of type '{}'".format(r_id, rt))
                raise RemoteDataNotFoundError("No such ID {}".format(r_id))

            with tempfile.NamedTemporaryFile() as tmpfile:
                tmpfile.write(res.content)
                tmpfile.seek(0)
                h5file = h5py.File(tmpfile.name, "r")
                origin = h5file["{}/XYZOFFSET".format(r_id)][()]
                size = h5file["{}/XYZDIMENSION".format(r_id)][()]
                return (origin[0], origin[0] + size[0],
                        origin[1], origin[1] + size[1],
                        origin[2], origin[2] + size[2])

        @_check_token
        def get_ramon_ids(self, token, channel, ramon_type=None):
            """
            Return a list of all IDs available for download from this token and
            channel.

            Arguments:
                token (str): Project to use
                channel (str): Channel to use
                ramon_type (int : None): Optional. If set, filters IDs and only
                    returns those of RAMON objects of the requested type.

            Returns:
                int[]: A list of the ids of the returned RAMON objects

            Raises:
                RemoteDataNotFoundError: If the channel or token is not found
            """
            url = self.url("{}/{}/query/".format(token, channel))
            if ramon_type is not None:
                # User is requesting a specific ramon_type.
                if type(ramon_type) is not int:
                    ramon_type = ramon.AnnotationType.get_int(ramon_type)
                url += "type/{}/".format(str(ramon_type))

            req = self.remote_utils.get_url(url)

            if req.status_code is not 200:
                raise RemoteDataNotFoundError('No query results for token {}.'
                                              .format(token))
            else:
                with tempfile.NamedTemporaryFile() as tmpfile:
                    tmpfile.write(req.content)
                    tmpfile.seek(0)
                    h5file = h5py.File(tmpfile.name, "r")
                    if 'ANNOIDS' not in h5file:
                        return []
                    return [i for i in h5file['ANNOIDS']]
                raise IOError("Could not successfully mock HDF5 file for parsing.")

        @_check_token
        def get_ramon(self, token, channel, ids, resolution=0,
                      include_cutout=False, sieve=None, batch_size=100):
            """
            Download a RAMON object by ID.

            Arguments:
                token (str): Project to use
                channel (str): The channel to use
                ids (int, str, int[], str[]): The IDs of a RAMON object to gather.
                    Can be int (3), string ("3"), int[] ([3, 4, 5]), or string
                    (["3", "4", "5"]).
                resolution (int : None): Resolution. Defaults to the most granular
                    resolution (0 for now)
                include_cutout (bool : False):  If True, r.cutout is populated
                sieve (function : None): A function that accepts a single ramon
                    and returns True or False depending on whether you want that
                    ramon object to be included in your response or not.
                    For example,
                    ```
                    def is_even_id(ramon):
                        return ramon.id % 2 == 0
                    ```
                    You can then pass this to get_ramon like this:
                    ```
                    ndio.remote.neurodata.get_ramon( . . . , sieve=is_even_id)
                    ```
                batch_size (int : 100): The amount of RAMON objects to download at
                    a time. If this is greater than 100, we anticipate things going
                    very poorly for you. So if you set it <100, ndio will use it.
                    If >=100, set it to 100.

            Returns:
                ndio.ramon.RAMON[]: A list of returned RAMON objects.

            Raises:
                RemoteDataNotFoundError: If the requested ids cannot be found.
            """
            b_size = min(100, batch_size)

            _return_first_only = False
            if type(ids) is not list:
                _return_first_only = True
                ids = [ids]
            ids = [str(i) for i in ids]

            rs = []
            id_batches = [ids[i:i + b_size] for i in range(0, len(ids), b_size)]
            for batch in id_batches:
                rs.extend(self._get_ramon_batch(token, channel, batch, resolution))

            rs = self._filter_ramon(rs, sieve)

            if include_cutout:
                rs = [self._add_ramon_cutout(token, channel, r, resolution)
                      for r in rs]

            if _return_first_only:
                return rs[0]

            return sorted(rs, key=lambda x: ids.index(x.id))

        def _filter_ramon(self, rs, sieve):
            if sieve is not None:
                return [r for r in rs if sieve(r)]
            return rs

        def _add_ramon_cutout(self, token, channel, ramon, resolution):
            origin = ramon.xyz_offset
            # Get the bounding box (cube-aligned)
            bbox = self.get_ramon_bounding_box(token, channel,
                                               ramon.id, resolution=resolution)
            # Get the cutout (cube-aligned)
            cutout = self.get_cutout(token, channel,
                                     *bbox, resolution=resolution)
            cutout[cutout != int(ramon.id)] = 0

            # Compute upper offset and crop
            bounds = numpy.argwhere(cutout)
            mins = [min([i[dim] for i in bounds]) for dim in range(3)]
            maxs = [max([i[dim] for i in bounds]) for dim in range(3)]

            ramon.cutout = cutout[
                mins[0]:maxs[0],
                mins[1]:maxs[1],
                mins[2]:maxs[2]
            ]

            return ramon

        def _get_ramon_batch(self, token, channel, ids, resolution):
            ids = [str(i) for i in ids]
            url = self.url("{}/{}/{}/json/".format(token, channel, ",".join(ids)))
            req = self.remote_utils.get_url(url)

            if req.status_code is not 200:
                raise RemoteDataNotFoundError('No data for id {}.'.format(ids))
            else:
                return ramon.from_json(req.json())

        @_check_token
        def get_ramon_metadata(self, token, channel, anno_id):
            """
            Download a RAMON object by ID. `anno_id` can be a string `"123"`, an
            int `123`, an array of ints `[123, 234, 345]`, an array of strings
            `["123", "234", "345"]`, or a comma-separated string list
            `"123,234,345"`.

            Arguments:
                token (str): Project to use
                channel (str): The channel to use
                anno_id: An int, a str, or a list of ids to gather

            Returns:
                JSON. If you pass a single id in str or int, returns a single datum
                If you pass a list of int or str or a comma-separated string, will
                return a dict with keys from the list and the values are the JSON
                returned from the server.

            Raises:
                RemoteDataNotFoundError: If the data cannot be found on the Remote
            """
            if type(anno_id) in [int, numpy.uint32]:
                # there's just one ID to download
                return self._get_single_ramon_metadata(token, channel,
                                                       str(anno_id))
            elif type(anno_id) is str:
                # either "id" or "id,id,id":
                if (len(anno_id.split(',')) > 1):
                    results = {}
                    for i in anno_id.split(','):
                        results[i] = self._get_single_ramon_metadata(
                            token, channel, anno_id.strip()
                        )
                    return results
                else:
                    # "id"
                    return self._get_single_ramon_metadata(token, channel,
                                                           anno_id.strip())
            elif type(anno_id) is list:
                # [id, id] or ['id', 'id']
                results = []
                for i in anno_id:
                    results.append(self._get_single_ramon_metadata(token, channel,
                                                                   str(i)))
                return results

        def _get_single_ramon_metadata(self, token, channel, anno_id):
            req = self.remote_utils.get_url(self.url() +
                              "{}/{}/{}/json/".format(token, channel, anno_id))
            if req.status_code is not 200:
                raise RemoteDataNotFoundError('No data for id {}.'.format(anno_id))
            return req.json()

        @_check_token
        def delete_ramon(self, token, channel, anno):
            """
            Deletes an annotation from the server. Probably you should be careful
            with this function, it seems dangerous.

            Arguments:
                token (str): The token to inspect
                channel (str): The channel to inspect
                anno (int OR list(int) OR RAMON): The annotation to delete. If a
                    RAMON object is supplied, the remote annotation will be deleted
                    by an ID lookup. If an int is supplied, the annotation will be
                    deleted for that ID. If a list of ints are provided, they will
                    all be deleted.

            Returns:
                bool: Success
            """
            if type(anno) is int:
                a = anno
            if type(anno) is str:
                a = int(anno)
            if type(anno) is list:
                a = ",".join(anno)
            else:
                a = anno.id

            req = requests.delete(self.url("{}/{}/{}/".format(token, channel, a)), verify=False)
            if req.status_code is not 200:
                raise RemoteDataNotFoundError("Could not delete id {}: {}"
                                              .format(a, req.text))
            else:
                return True

        @_check_token
        def post_ramon(self, token, channel, r, batch_size=100):
            """
            Posts a RAMON object to the Remote.

            Arguments:
                token (str): Project to use
                channel (str): The channel to use
                r (RAMON or RAMON[]): The annotation(s) to upload
                batch_size (int : 100): The number of RAMONs to post simultaneously
                    at maximum in one file. If len(r) > batch_size, the batch will
                    be split and uploaded automatically. Must be less than 100.

            Returns:
                bool: Success = True

            Throws:
                RemoteDataUploadError: if something goes wrong
            """
            # Max out batch-size at 100.
            b_size = min(100, batch_size)

            # Coerce incoming IDs to a list.
            if type(r) is not list:
                r = [r]

            # If there are too many to fit in one batch, split here and call this
            # function recursively.
            if len(r) > batch_size:
                batches = [r[i:i + b_size] for i in range(0, len(r), b_size)]
                for batch in batches:
                    self.post_ramon(token, channel, batch, b_size)
                return

            with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                for i in r:
                    tmpfile = ramon.to_hdf5(i, tmpfile)

                url = self.url("{}/{}/overwrite/".format(token, channel))
                req = urllib2.Request(url, tmpfile.read())
                res = urllib2.urlopen(req)

                if res.code != 200:
                    raise RemoteDataUploadError('[{}] Could not upload {}'
                                                .format(res.code, str(r)))

                rets = res.read()
                if six.PY3:
                    rets = rets.decode()
                return_ids = [int(rid) for rid in rets.split(',')]

                # Now post the cutout separately:
                for ri in r:
                    if 'cutout' in dir(ri) and ri.cutout is not None:
                        orig = ri.xyz_offset
                        self.post_cutout(token, channel,
                                         orig[0], orig[1], orig[2],
                                         ri.cutout, resolution=r.resolution)
                return return_ids
            return True
