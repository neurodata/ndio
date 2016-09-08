import unittest
import ndio.remote.dvid as dvid
import numpy

FIB25_HASH = '5cc94d532799484cb01788fcdb7cd9f0'

class TestDVID(unittest.TestCase):

    def setUp(self):
        self.dvid = dvid('conduit:8000')

    def test_get_repositories(self):
        self.assertTrue(
            # We know the UUID of the repository sent with default DVID:
            FIB25_HASH in self.dvid.get_repositories()
        )

    def test_get_repo_info(self):
        self.assertEqual(
            type(self.dvid.get_repository_info(FIB25_HASH)),
            dict
        )

    def test_get_repo_nodes(self):
        self.assertEqual(
            type(self.dvid.get_repository_nodes(FIB25_HASH)),
            list
        )

if __name__ == '__main__':
    unittest.main()
