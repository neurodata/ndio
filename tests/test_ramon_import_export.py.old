import unittest
import ndio.remote.neurodata as neurodata
import ndio.ramon as ramon
import test_settings
import numpy
import h5py
import os
import random
import json


class TestRAMON(unittest.TestCase):

    def setUp(self):
        self.user = test_settings.NEURODATA
        self.hostname = test_settings.HOSTNAME
        self.nd = neurodata(user_token=self.user, hostname=self.hostname)
        self.ramon_id = 1

    def test_json_export(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        self.nd.delete_channel('testc', 'testp', 'test')
        self.nd.create_channel(
            'testc', 'testp', 'test', 'image', 'uint8', 0, 500, 0)
        self.nd.create_token('testt', 'testp', 'test', 1)

        r = self.nd.get_ramon('testt', 'testc', self.ramon_id)
        
        self.assertEqual(
            str(self.ramon_id),
            json.loads(ramon.to_json(r))['1']['id']
        )

        self.nd.delete_token('testt', 'testp', 'test')
        self.nd.delete_channel('testc', 'testp', 'test')
        self.nd.delete_project('testp', 'test')
        self.nd.delete_dataset('test')


if __name__ == '__main__':
    unittest.main()
