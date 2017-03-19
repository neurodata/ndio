import unittest
import ndio.remote.neurodata as neurodata
import ndio.remote.errors
import numpy
import random
import test_settings


class TestXYZZYX(unittest.TestCase):

    def setUp(self):
        self.token_user = '043813bf95d9d5be2bb19448d5a9337db086c559'
        hostname = test_settings.HOSTNAME
        self.nd = neurodata(self.token_user,hostname=hostname)
        self.nd_force_chunk = neurodata(self.token_user,hostname=hostname,
                                        chunk_threshold=0)
        dataset_name = 'demo1'
        project_name = 'ndio_demos'
        project_token = 'test_token'
        channel_name = 'image1'
        self.nd.create_dataset(dataset_name, 100, 100, 100, 1.0, 1.0, 1.0)
        self.nd.create_project(project_name, dataset_name, 'localhost',
                               1, 1,'localhost','Redis')
        self.nd.create_project_token(dataset_name, project_name,
                                     project_token, 1)
        self.nd.create_channel(channel_name,project_name,
                               dataset_name, 'timeseries',
                               'uint8', 0, 500, 0,0,0)

    def tearDown(self):
        dataset_name = 'demo1'
        project_name = 'ndio_demos'
        project_token = 'test_token'
        channel_name = 'image1'
        self.nd.delete_channel(channel_name,project_name, dataset_name)
        self.nd.delete_project(project_name, dataset_name)
        self.nd.delete_dataset(dataset_name)

    def test_post_get_no_chunk(self):
        token = 'test_token'
        channel = 'image1'
        cutout = numpy.zeros((10, 10, 10)).astype(int)
        zind = random.randint(0, 4)
        cutout[2, 3, zind] = random.randint(100, 200)
        self.nd.post_cutout(token, channel, 20, 20, 20, cutout, resolution=0)
        pulldown = self.nd.get_cutout(token, channel,
                                      20, 25, 20, 25, 20, 25, resolution=0)

        self.assertEqual(cutout[2, 3, zind], pulldown[3, zind, 0, 2])

    def test_post_get_chunk(self):
        token = 'test_token'
        channel = 'image1'
        cutout = numpy.zeros((10, 10, 10)).astype(int)
        zind = random.randint(0, 4)
        cutout[2, 3, zind] = random.randint(100, 200)
        self.nd_force_chunk.post_cutout(token, channel,
                                        20, 20, 20, cutout, resolution=0)
        pulldown = self.nd_force_chunk.get_cutout(token, channel,
                                                  20, 25, 20, 25, 20, 25,
                                                  resolution=0)
        self.assertEqual(cutout[2, 3, zind], pulldown[2, 3, zind])


if __name__ == '__main__':
    unittest.main()
