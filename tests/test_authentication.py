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
        self.private_project = "https://{}/nd/sd/p_neuro/info/".format(HOSTNAME)
        self.public_project = "https://{}/nd/sd/public_neuro/info/".format(HOSTNAME)
        self.private_test_project = "https://{}/nd/sd/private_test_neuro/info/".format(HOSTNAME)

        #Create the projects/tokens that are required for this
        self.neurodata.create_dataset('private_neuro', 10, 10, 10, 1, 1, 1)
        # import pdb; pdb.set_trace()
        self.neurodata.create_project('p_neuro', 'private_neuro', 'localhost', 0)
        self.neurodata.create_token('p_neuro', 'p_neuro', 'private_neuro', 0)

        self.neurodata.create_dataset('public_neuro', 10, 10, 10, 1, 1, 1)
        self.neurodata.create_project('public_neuro', 'public_neuro', 'localhost', 1)
        self.neurodata.create_token('public_neuro', 'public_neuro', 'public_neuro', 1)

        self.test.create_dataset('private_test_neuro', 10, 10, 10, 1, 1, 1)
        self.test.create_project('private_test_neuro', 'private_test_neuro', 'localhost', 0)
        self.test.create_token('private_test_neuro', 'private_test_neuro', 'private_test_neuro', 0)

    def tearDown(self):
        self.neurodata.delete_token('p_neuro', 'p_neuro', 'private_neuro')
        self.neurodata.delete_project('p_neuro', 'private_neuro')
        self.neurodata.delete_dataset('private_neuro')

        self.neurodata.delete_token('public_neuro', 'public_neuro', 'public_neuro')
        self.neurodata.delete_project('public_neuro', 'public_neuro')
        self.neurodata.delete_dataset('public_neuro')

        self.neurodata.delete_token('private_test_neuro', 'private_test_neuro', 'private_test_neuro')
        self.neurodata.delete_project('private_test_neuro', 'private_test_neuro')
        self.neurodata.delete_dataset('private_test_neuro')

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
            req3 = self.neurodata.remote_utils.get_url(self.private_test_project)
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
