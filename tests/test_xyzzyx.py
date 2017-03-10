import unittest
import ndio.remote.neurodata as neurodata
import ndio.remote.errors
import numpy
import random
import test_settings


class TestXYZZYX(unittest.TestCase):

    def setUp(self):
        self.token_user = '043813bf95d9d5be2bb19448d5a9337db086c559'
        hostname = 'localhost'
        self.nd = neurodata(self.token_user,hostname=hostname)
        self.nd_force_chunk = neurodata(self.token_user,hostname=hostname, chunk_threshold=0)
        self.nd.create_dataset('demo1', 100, 100, 100, 1.0, 1.0, 1.0)
        self.nd.create_project('ndio_demos', 'demo1', 'localhost', 1, 1,'localhost','Redis')
        print self.nd.get_channel('image1','ndio_demos','demo1')
        #self.nd.create_channel('image1','ndio_demos', 'demo1', 'image','unit8', 0, 500, 0)

    def test_post_get_no_chunk(self):
        import pdb; pdb.set_trace()
        token = 'ndio_demos'
        channel = 'image1'
        cutout = numpy.zeros((10, 10, 10)).astype(int)
        zind = random.randint(0, 4)
        cutout[2, 3, zind] = random.randint(100, 200)
        self.nd.post_cutout(token, channel, 20, 20, 20, cutout, resolution=0)
        pulldown = self.nd.get_cutout(token, channel,
                                      20, 25, 20, 25, 20, 25, resolution=0)
        self.assertEqual(cutout[2, 3, zind], pulldown[2, 3, zind])

    def test_post_get_chunk(self):
        token = 'ndio_demos'
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
    #t = TestXYZZYX()
    #t.setUp()
    unittest.main()
