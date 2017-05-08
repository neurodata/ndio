import unittest
import ndio.remote.neurodata as nd
import ndio.remote.ndingest as NDIngest
import datetime
import requests
import json
import os
import numpy
import sys
import test_settings
from pprint import pprint
import ast

# SERVER_SITE = 'http://ec2-54-201-19-176.us-west-2.compute.amazonaws.com/'
# DATA_SITE = 'http://ec2-54-200-215-161.us-west-2.compute.amazonaws.com/'
# S3_SITE = 'http://ndios3test.s3.amazonaws.com/'

SERVER_SITE = test_settings.HOSTNAME
DATA_SITE = 'https://localhost/testserver'


class TestNDIngest(unittest.TestCase):

    def setUp(self):
        self.i = datetime.datetime.now()
        # self.oo = nd(SERVER_SITE[len('http://'):])
        self.oo = nd(SERVER_SITE)

    def test_bad_name(self):
        # Test a forbidden character
        data_name_10 = 'ndio@test@1'
        ai_10 = NDIngest.NDIngest()
        ai_10.add_channel(data_name_10, 'uint8', 'image', DATA_SITE,
                          'SLICE', 'tif')
        ai_10.add_project(data_name_10, data_name_10, 1)
        ai_10.add_dataset(data_name_10, (512, 512, 1), (1.0, 1.0, 10.0))
        ai_10.add_metadata('')
        with self.assertRaises(OSError):
            ai_10.output_json()

    def test_bad_url(self):
        # Test URL not HTTP available
        data_name_7 = 'ndio_test_1'
        ai_7 = NDIngest.NDIngest()
        ai_7.add_channel(data_name_7, 'uint8', 'image', "openconnectome",
                         'SLICE', 'tif')
        ai_7.add_project(data_name_7, data_name_7, 1)
        ai_7.add_dataset(data_name_7, (512, 512, 1), (1.0, 1.0, 10.0))
        ai_7.add_metadata('')
        self.assertRaises(ValueError, ai_7.output_json)

    def test_good_bw(self):
        # Test black and white channel
        data_name_10 = 'ndiotest01'
        filename = '/tmp/good_bw.json'
        ai_10 = NDIngest.NDIngest()
        ai_10.add_channel(data_name_10, 'uint8', 'image', DATA_SITE,
                          'SLICE', 'tiff')
        ai_10.add_project(data_name_10, data_name_10, 1)
        # import pdb; pdb.set_trace()
        ai_10.add_dataset(data_name_10, (512, 512, 1), (1.0, 1.0, 10.0))
        ai_10.add_metadata('')
        # import pdb; pdb.set_trace()
        self.assertEqual(ai_10.output_json(filename), None)
        dictcreated = dict(ast.literal_eval(open(filename).readline()))
        dictwanted = {u'channels': {u'ndiotest01':
                                    {u'channel_name': u'ndiotest01',
                                     u'channel_type': u'image',
                                     u'data_url':
                                     u'https://localhost/testserver',
                                     u'datatype': u'uint8',
                                     u'file_format': u'SLICE',
                                     u'file_type': u'tiff'}},
                      u'dataset': {u'dataset_name': u'ndiotest01',
                                   u'imagesize': [512, 512, 1],
                                   u'voxelres': [1.0, 1.0, 10.0]},
                      u'metadata': u'',
                      u'project': {u'project_name': u'ndiotest01',
                                   u'public': 1,
                                   u'token_name': u'ndiotest01'}}

        self.assertEqual(dictcreated, dictwanted)

        # pprint(dict(open(filename).readline()))

    # def test_good_rgb(self):
    #     # Test colored channel
    #     data_name_10 = 'ndiotest02'
    #     ai_10 = NDIngest.NDIngest()
    #     ai_10.add_channel('RGB32', 'uint32', 'image', DATA_SITE,
    #                       'SLICE', 'tiff')
    #     ai_10.add_project(data_name_10, data_name_10, 1)
    #     # import pdb; pdb.set_trace()
    #     ai_10.add_dataset('RBG32', (512,512,1), (1.0, 1.0, 10.0))
    #     ai_10.add_metadata('')
    #     # import pdb; pdb.set_trace()
    #     self.assertEqual(ai_10.output_json('/tmp/good_rgb.json'), None)

    def test_timeseries(self):
        data_name_10 = 'ndiotest03'
        filename = '/tmp/good_ts.json'
        ai_10 = NDIngest.NDIngest()
        ai_10.add_channel(data_name_10, 'uint8', 'timeseries', DATA_SITE,
                          'SLICE', 'tiff')
        ai_10.add_project(data_name_10, data_name_10, 1)
        # import pdb; pdb.set_trace()
        ai_10.add_dataset(data_name_10, (512, 512, 1),
                          (1.0, 1.0, 10.0), timerange=(0, 1))
        ai_10.add_metadata('')
        # import pdb; pdb.set_trace()
        self.assertEqual(ai_10.output_json(filename), None)
        dictcreated = dict(ast.literal_eval(open(filename).readline()))
        makeitfit = {u'channel_name':
                     u'ndiotest03',
                     u'channel_type':
                     u'timeseries',
                     u'data_url':
                     u'https://localhost/testserver',
                     u'datatype': u'uint8',
                     u'file_format': u'SLICE',
                     u'file_type': u'tiff'}
        dictwanted = {u'channels': {u'ndiotest03': makeitfit},
                      u'dataset': {u'dataset_name': u'ndiotest03',
                                   u'imagesize': [512, 512, 1],
                                   u'timerange': [0, 1],
                                   u'voxelres': [1.0, 1.0, 10.0]},
                      u'metadata': u'',
                      u'project': {u'project_name': u'ndiotest03',
                                   u'public': 1,
                                   u'token_name': u'ndiotest03'}}

        self.assertEqual(dictcreated, dictwanted)


if __name__ == '__main__':
    unittest.main()
