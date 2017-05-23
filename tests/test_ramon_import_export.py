import unittest
import ndio.remote.neurodata as neurodata
import ndio.ramon as ramon
import test_settings
import numpy as np
import h5py
import os
import random
import json


class TestRAMON(unittest.TestCase):

    def setUp(self):
        self.user = test_settings.NEURODATA
        self.hostname = test_settings.HOSTNAME
        self.nd = neurodata(user_token=self.user, hostname=self.hostname)

    def test_json_export(self):
        ##################Dynamically create ramon object
        anno_type = 1
        r = ramon.AnnotationType.get_class(anno_type)()
        # print json.loads(ramon.to_json(r))
        self.nd.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        self.nd.create_project(
            'testp', 'test', 'localhost', 1, 1, 'localhost', 'MySQL')
        self.nd.delete_channel('testc', 'testp', 'test')
        self.nd.create_channel('testc', 'testp', 'test',
                               'annotation', 'uint32', 0, 500, 0)
        self.nd.create_token('testt', 'testp', 'test', 1)
        # import pdb; pdb.set_trace()
        post_id = self.nd.post_ramon('testt', 'testc', r)
        return_r = self.nd.get_ramon('testt', 'testc', post_id)
        # import pdb; pdb.set_trace()
        assertEqual(post_id, ramon.to_json(return_r)["0"]["id"])

        self.nd.delete_ramon('testt', 'testc', r)

        self.nd.delete_token('testt', 'testp', 'test')
        self.nd.delete_channel('testc', 'testp', 'test')
        self.nd.delete_project('testp', 'test')
        self.nd.delete_dataset('test')      

        # print self.nd.get_ramon_ids('public_neuro', 'public_channel')

     

     #    # #self.assertEqual(
     #    # #    str(self.ramon_id),
     #    # #    json.loads(ramon.to_json(r))['2']['id']
     #    # #)
        
        


if __name__ == '__main__':
    unittest.main()
