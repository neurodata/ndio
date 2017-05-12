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

class TestData(unittest.TestCase):

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
        self.c = 'test_channel'
        self.nd.create_dataset(dataset_name, 100, 100, 100, 1.0,
                                      1.0,
                                      1.0)
        self.nd.create_project(project_name, dataset_name,
                                      'localhost',
                                      1, 1, 'localhost', 'Redis')
        self.nd.create_token(self.t, project_name, dataset_name, 1)
        try:
            self.nd.create_channel(self.c, project_name,
                               dataset_name, 'timeseries',
                               'uint8', 0, 500, propagate=1)
        except:
            pass

        #random 3-d array
        self.data = numpy.arange(30).reshape(2,3,5)

    def test_post_cutout(self):
        self.nd.post_cutout(self.t, self.c, 0, 0, 0, 0, self.data)




if __name__ == '__main__':
    unittest.main()