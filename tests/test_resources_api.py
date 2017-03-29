import unittest
from ndio.remote.neurodata import neurodata as nd
import numpy
import json
import test_settings


class TestResourcesApi(unittest.TestCase):

    def setUp(self):
        self.token_user = '043813bf95d9d5be2bb19448d5a9337db086c559'
        hostname = 'localhost'
        self.nd = nd(self.token_user, hostname=hostname)

    def test_create_dataset(self):
        result = self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0)
        self.assertEqual(result, True)
        self.nd.delete_dataset('test')

        result = self.nd.create_dataset(
            'test', 1, 1, 1, 1.0, 1.0, 1.0, is_public=1)
        self.assertEqual(result, True)
        self.nd.delete_dataset('test')

    def test_get_dataset(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        result = self.nd.get_dataset('test')
        compare_dict = {
            u'yimagesize': 1,
            u'ximagesize': 1,
            u'zoffset': 0,
            u'scalingoption': 0,
            u'zimagesize': 1,
            u'yoffset': 0,
            u'dataset_description': u'',
            u'scalinglevels': 0,
            u'user': 1,
            u'yvoxelres': 1.0,
            u'zvoxelres': 1.0,
            u'dataset_name': u'test',
            u'public': 0,
            u'xoffset': 0,
            u'xvoxelres': 1.0
        }
        for key in compare_dict:
            self.assertEqual(result[key], compare_dict[key])

        self.nd.delete_dataset('test')

    def test_list_datasets(self):
        test_dataset_names = [u'test1', u'test2', u'test3']
        self.nd.create_dataset('test1', 1, 1, 1, 1.0, 1.0, 1.0, is_public=1)
        self.nd.create_dataset('test2', 1, 1, 1, 1.0, 1.0, 1.0, is_public=1)
        self.nd.create_dataset('test3', 1, 1, 1, 1.0, 1.0, 1.0, is_public=1)
        test_dataset_list = self.nd.list_datasets(False)
        for name in test_dataset_names:
            self.assertEqual(name in test_dataset_list, True)
        self.nd.delete_dataset('test1')
        self.nd.delete_dataset('test2')
        self.nd.delete_dataset('test3')

    def test_delete_dataset(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        result = self.nd.delete_dataset('test')
        self.assertEqual(result, True)

    def test_create_project(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        result = self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        self.assertEqual(result, True)
        self.nd.delete_project('testp', 'test')
        self.nd.delete_dataset('test')

    def test_get_project(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        result = self.nd.get_project('testp', 'test')
        compare_dict = {u'kvengine': u'Redis',
                        u'mdengine': u'MySQL',
                        u'project_name': u'testp',
                        u'nd_version': u'1.0',
                        u'schema_version': u'0.7',
                        u'dataset': u'test',
                        u'host': u'localhost',
                        u'project_description': u'',
                        u'user': 1,
                        u'public': 1,
                        u'kvserver': u'localhost',
                        u's3backend': 1}
        for key in compare_dict:
            self.assertEqual(result[key], compare_dict[key])
        self.nd.delete_project('testp', 'test')
        self.nd.delete_dataset('test')

    def test_delete_project(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        result = self.nd.delete_project('testp', 'test')
        self.assertEqual(result, True)
        self.nd.delete_dataset('test')

    def test_list_projects(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        test_project_names = [u'test1p', u'test2p', u'test3p']
        self.nd.create_project(
            'test1p', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        self.nd.create_project(
            'test2p', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        self.nd.create_project(
            'test3p', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        test_project_list = self.nd.list_projects('test')
        for name in test_project_names:
            self.assertEqual(name in test_project_list, True)
        self.nd.delete_project('test1p', 'test')
        self.nd.delete_project('test2p', 'test')
        self.nd.delete_project('test3p', 'test')
        self.nd.delete_dataset('test')

    def test_create_channel(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        result = self.nd.create_channel(
            'testc', 'testp', 'test', 'image', 'uint8', 0, 500, 0)
        self.assertEqual(result, True)
        self.nd.delete_channel('testc', 'testp', 'test')
        self.nd.delete_project('testp', 'test')
        self.nd.delete_dataset('test')

    def test_get_channel(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        self.nd.create_channel('testc', 'testp', 'test',
                               'image', 'uint8', 0, 500, 0)
        result = self.nd.get_channel('testc', 'testp', 'test')
        compare_dict = {u'channel_description': u'',
                        u'endwindow': 500,
                        u'channel_name': u'testc',
                        u'default': False,
                        u'project': u'testp',
                        u'startwindow': 0,
                        u'channel_type': u'image',
                        u'header': u'',
                        u'readonly': 0,
                        u'propagate': 0,
                        u'starttime': 0,
                        u'exceptions': 0,
                        u'channel_datatype': u'uint8',
                        u'endtime': 0,
                        u'resolution': 0}
        for key in compare_dict:
            self.assertEqual(result[key], compare_dict[key])
        self.nd.delete_channel('testc', 'testp', 'test')
        self.nd.delete_project('testp', 'test')
        self.nd.delete_dataset('test')

    def test_delete_channel(self):
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        self.nd.create_channel('testc', 'testp', 'test',
                               'image', 'uint8', 0, 500, 0)
        result = self.nd.delete_channel('testc', 'testp', 'test')
        self.assertEqual(result, True)
        self.nd.delete_project('testp', 'test')
        self.nd.delete_dataset('test')

if __name__ == '__main__':
    unittest.main()
