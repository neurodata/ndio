import unittest
import ndio.remote.neurodata as neurodata
import ndio.remote.errors
import test_settings
import numpy
import h5py
import os


class TestPropagate(unittest.TestCase):

    def setUp(self):
    	hostname = test_settings.HOSTNAME
    	self.token_user = test_settings.NEURODATA
        self.nd = neurodata(user_token=self.token_user, hostname=hostname)

    def test_propagate_status_fails_on_bad_token(self):
        token = 'this is not a token'
        with self.assertRaises(ValueError):
            self.nd.get_propagate_status(token, 'channel')


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



if __name__ == '__main__':
    unittest.main()
