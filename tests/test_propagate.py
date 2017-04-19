import unittest
import ndio.remote.neurodata as neurodata
import ndio.remote.errors
import numpy
import h5py
import os
import test_settings

HOSTNAME = test_settings.HOSTNAME
NEURODATA = test_settings.NEURODATA
TEST = test_settings.TEST

class TestPropagate(unittest.TestCase):

    def setUp(self):
        self.token_user = NEURODATA
        hostname = HOSTNAME
        self.nd = neurodata(self.token_user,
                            hostname=hostname,
                            check_tokens=True)
        self.ramon_id = 1
        dataset_name = 'demo1'
        project_name = 'ndio_demos'
        self.t = 'test_token'
        self.c = 'image1'
        self.nd.create_dataset(dataset_name, 100, 100, 100, 1.0,
                                      1.0,
                                      1.0)
        self.nd.create_project(project_name, dataset_name,
                                      'localhost',
                                      1, 1, 'localhost', 'Redis')
        self.nd.create_token(self.t, project_name, dataset_name, 1)
        self.nd.create_channel(self.c, project_name,
                               dataset_name, 'timeseries',
                               'uint8', 0, 500, propagate=1)

    def tearDown(self):
        dataset_name = 'demo1'
        project_name = 'ndio_demos'
        channel_name = 'image1'
        self.nd.delete_channel(channel_name, project_name,
                                      dataset_name)
        self.nd.delete_project(project_name, dataset_name)
        self.nd.delete_dataset(dataset_name)

    def test_propagate_status_fails_on_bad_token(self):
        token = 'this is not a token'
        with self.assertRaises(ValueError):
            self.nd.get_propagate_status(token, self.c)


    #Channel is by default not allowed to be propagated. But pass the test
    def test_propagated(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        self.nd.create_channel(
            'testc', 'testp', 'test', 'image', 'uint8', 0, 500, 0)
        self.nd.create_token('testt', 'testp', 'test', 1)

        self.assertEqual(self.nd.propagate('testt', 'testc'), True)
        #Is it supposed to be 1?
        self.assertEqual(self.nd.get_propagate_status('testt', 'testc'), '1')

        self.nd.delete_token('testt', 'testp', 'test')
        self.nd.delete_channel('testc', 'testp', 'test')
        self.nd.delete_project('testp', 'test')
        self.nd.delete_dataset('test')

    def test_kasthuri11_is_propagated(self):
        # token = 'kasthuri11'
        self.assertEqual(self.nd.get_propagate_status(self.t, self.c), '1')


if __name__ == '__main__':
    unittest.main()
