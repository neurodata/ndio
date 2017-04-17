import unittest
import ndio.remote.neurodata as neurodata
import ndio.ramon as ramon
import numpy
import h5py
import os
import random
import test_settings


class TestRAMON(unittest.TestCase):

    def setUp(self):
        self.token_user = '043813bf95d9d5be2bb19448d5a9337db086c559'
        hostname = test_settings.HOSTNAME
        self.nd = neurodata(self.token_user, hostname=hostname)
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
        self.nd.create_project_token(dataset_name, project_name,
                                            self.t, 1)
        self.nd.create_channel(self.c, project_name,
                                      dataset_name, 'timeseries',
                                      'uint8', 0, 500, 0, 0, 0)

        # self.nd.post_ramon(self.t,self.c,r)


    def tearDown(self):
        dataset_name = 'demo1'
        project_name = 'ndio_demos'
        channel_name = 'image1'
        self.nd.delete_channel(channel_name, project_name,
                                      dataset_name)
        self.nd.delete_project(project_name, dataset_name)
        self.nd.delete_dataset(dataset_name)

    # def test_download_single_ramon(self):
    #     r = self.nd.get_ramon(self.t, self.c, "3")
    #     self.assertEqual(r.id, "3")
    #
    # def test_download_multi_ramon(self):
    #     r = self.nd.get_ramon(self.t, self.c, ["3", "4"])
    #     self.assertEqual(True, '3' in [s.id for s in r])
    #
    # def test_download_ramon_of_type(self):
    #     ids = self.nd.get_ramon_ids(self.t, self.c,
    #                                 ramon.AnnotationType.SEGMENT)
    #     randindex = random.randint(0, len(ids))
    #     # randomly pick one to check
    #     r = self.nd.get_ramon(self.t, self.c, ids[randindex])
    #     self.assertEqual(type(r), ramon.RAMONSegment)
    #
    #     ids = self.nd.get_ramon_ids(self.t, self.c,
    #                                 ramon.AnnotationType.NEURON)
    #     randindex = random.randint(0, len(ids))
    #     # randomly pick one to check
    #     rn = self.nd.get_ramon(self.t, self.c, ids[randindex])
    #     self.assertEqual(type(rn), ramon.RAMONNeuron)
    #
    # def test_downup_ramon(self):
    #     rr = self.nd.get_ramon(self.t, self.c, 10016)
    #     ids = self.nd.reserve_ids('ndio_demos', 'ramontests', 1)
    #     rr.id = 0
    #     self.nd.post_ramon('ndio_demos', 'ramontests', rr)


if __name__ == '__main__':
    unittest.main()
