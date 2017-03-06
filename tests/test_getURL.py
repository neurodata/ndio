import unittest
import ndio.remote.neurodata as neurodata

neurodata_token = "1524d74ec9a915536061af8fd5b51f2cde7de9a5"
test_token = "b1876fab697e519e0cfe4acc9ca0496bf2889e17"
Default_token = ""


class TestgetURL(unittest.TestCase):

    def setUp(self):
        self.neurodata = neurodata(user_token=neurodata_token)
        self.public = neurodata(user_token="")
        self.test = neurodata(user_token=Default_token)
        self.private_URL = "https://localhost/nd/sd/private_neuro/info/"
        self.public_URL = "https://localhost/nd/sd/temp_neuro/info/"

    def test_masterToken(self):
        try:
            req = self.neurodata.getURL(self.private_URL)
            self.assertEqual(req.status_code, 200)
        except ValueError as e:
            print(e)
        try:
            req2 = self.neurodata.getURL(self.public_URL)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)

    def test_testToken(self):
        try:
            req = self.neurodata.getURL(self.private_URL)
            self.assertEqual(req.status_code, 403)
        except ValueError as e:
            print(e)
        try:
            req2 = self.neurodata.getURL(self.public_URL)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)

    def test_publicToken(self):
        try:
            req = self.neurodata.getURL(self.private_URL)
            self.assertEqual(req.status_code, 403)
        except ValueError as e:
            print(e)
        try:
            req2 = self.neurodata.getURL(self.public_URL)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)

if __name__ == '__main__':
    unittest.main()
