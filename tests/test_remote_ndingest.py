import unittest
import ndio.remote.neurodata as nd
import ndio.remote.ndingest as NDIngest
import datetime
import requests
import json
import os
import numpy
import sys

SERVER_SITE = 'http://ec2-54-201-19-176.us-west-2.compute.amazonaws.com/'
DATA_SITE = 'http://ec2-54-200-215-161.us-west-2.compute.amazonaws.com/'
S3_SITE = 'http://ndios3test.s3.amazonaws.com/'


class TestNDIngest(unittest.TestCase):

    def setUp(self):
        self.i = datetime.datetime.now()
        self.oo = nd(SERVER_SITE[len('http://'):])

    def test_bad_name(self):
        # Test a forbidden character
        data_name_10 = 'ndio@test@1'
        ai_10 = NDIngest.NDIngest()
        ai_10.add_channel(data_name_10, 'uint8', 'image', DATA_SITE,
                          'SLICE', 'tif')
        ai_10.add_project(data_name_10, data_name_10, 1)
        ai_10.add_dataset(data_name_10, (512, 512, 1), (1.0, 1.0, 10.0))
        ai_10.add_metadata('')
        with self.assertRaises(ValueError):
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

if __name__ == '__main__':
    unittest.main()
