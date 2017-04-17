import ndio.remote.neurodata as neurodata
import test_settings
import unittest

HOSTNAME = test_settings.HOSTNAME
NEURODATA = test_settings.NEURODATA
TEST = test_settings.TEST


class TestgetURL(unittest.TestCase):

    def setUp(self):
        self.neurodata = neurodata(user_token=NEURODATA, hostname=HOSTNAME)
        self.public = neurodata(hostname=HOSTNAME)
        self.test = neurodata(user_token=TEST, hostname=HOSTNAME)
        self.private_project = "https://{}/nd/sd/pn/info/".format(
            HOSTNAME)
        self.public_project = "https://{}/nd/sd/pubn/info/".format(
            HOSTNAME)
        self.private_test_project = "https://{}/nd/sd/".format(HOSTNAME)\
                                    + "ptn/info/"

        # Create the projects/tokens that are required for this
        self.neurodata.create_dataset('pn', 10, 10, 10, 1, 1, 1)
        self.neurodata.create_project(
            'pn', 'pn', 'localhost', 0)
        self.neurodata.create_token(
            'pn', 'pn', 'pn', 0)

        self.neurodata.create_dataset('pubn', 10, 10, 10, 1, 1, 1)
        self.neurodata.create_project(
            'pubn', 'pubn', 'localhost', 1)
        self.neurodata.create_token(
            'pubn', 'pubn', 'pubn', 1)

        self.test.create_dataset('ptn', 10, 10, 10, 1, 1, 1)
        self.test.create_project('ptn',
                                 'ptn', 'localhost', 0)
        self.test.create_token('ptn',
                               'ptn',
                               'ptn', 0)

    def tearDown(self):
        self.neurodata.delete_token(
            'pn', 'pn', 'pn')
        self.neurodata.delete_project('pn', 'pn')
        self.neurodata.delete_dataset('pn')

        self.neurodata.delete_token(
            'pubn', 'pubn', 'pubn')
        self.neurodata.delete_project('pubn', 'pubn')
        self.neurodata.delete_dataset('pubn')

        self.neurodata.delete_token(
            'ptn', 'ptn',
            'ptn')
        self.neurodata.delete_project(
            'ptn', 'ptn')
        self.neurodata.delete_dataset('ptn')

    def test_masterToken(self):
        try:
            req = self.neurodata.remote_utils.get_url(self.private_project)
            print(req.status_code)
            self.assertEqual(req.status_code, 200)
        except ValueError as e:
            print(req.content)
        try:
            req2 = self.neurodata.remote_utils.get_url(self.public_project)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)

        try:
            req3 = self.neurodata.remote_utils.get_url(
                self.private_test_project)
            self.assertEqual(req3.status_code, 200)
        except ValueError as e:
            print(e)

    def test_testToken(self):
        try:
            req = self.test.remote_utils.get_url(self.private_project)
            self.assertEqual(req.status_code, 403)
        except ValueError as e:
            print(e)
        try:
            req2 = self.test.remote_utils.get_url(self.public_project)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)
        try:
            req3 = self.test.remote_utils.get_url(self.private_test_project)
            self.assertEqual(req3.status_code, 200)
        except ValueError as e:
            print(e)

    def test_publicToken(self):
        try:
            req = self.public.remote_utils.get_url(self.private_project)
            self.assertEqual(req.status_code, 401)
        except ValueError as e:
            print(req.content)
        try:
            req2 = self.public.remote_utils.get_url(self.public_project)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)

if __name__ == '__main__':
    unittest.main()
