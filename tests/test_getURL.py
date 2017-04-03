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
        self.private_project = "https://{}/sd/private_neuro/info/".format(HOSTNAME)
        self.public_project = "https://{}/sd/public_neuro/info/".format(HOSTNAME)
        self.private_test_project = "https://{}/sd/private_test_neuro/info/".format(HOSTNAME)
    def test_masterToken(self):
        try:
            req = self.neurodata.getURL(self.private_project)
            print(req.status_code)
            self.assertEqual(req.status_code, 200)
        except ValueError as e:
            print(req.content)
        try:
            req2 = self.neurodata.getURL(self.public_project)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)
            
        try:
            req3 = self.neurodata.getURL(self.private_test_project)
            self.assertEqual(req3.status_code, 200)
        except ValueError as e:
            print(e)
    
    def test_testToken(self):
        try:
            req = self.test.getURL(self.private_project)
            self.assertEqual(req.status_code, 403)
        except ValueError as e:
            print(e)
        try:
            req2 = self.test.getURL(self.public_project)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)
        try:
            req3 = self.test.getURL(self.private_test_project)
            self.assertEqual(req3.status_code, 200)
        except ValueError as e:
            print(e)


    def test_publicToken(self):
        try:
            req = self.public.getURL(self.private_project)
            self.assertEqual(req.status_code, 401)
        except ValueError as e:
            print(req.content)
        try:
            req2 = self.public.getURL(self.public_project)
            self.assertEqual(req2.status_code, 200)
        except ValueError as e:
            print(e)

    def test_newToken(self):
        test = neurodata(user_token=TEST, hostname='localhost')
        test.create_dataset('test', 1, 1, 1, 1.0, 1.0, 1.0, 0)
        test.create_project('testp', 'test', 'localhost', 1, 1, 'localhost', 'Redis')
        test.create_token('testt', 'testp', 'test', 1)
        new_token = neurodata(user_token='testt', hostname=HOSTNAME)
        new_token_project = "https://{}/sd/testp/info/".format(HOSTNAME)
        # try:
        #     req = new_token.getURL(new_token_project)
        #     self.assertEqual(req.status_code, 200)
        # except ValueError as e:
        #     print(e)
        # try:
        #     req2 = new_token.getURL(self.private_project)
        #     self.assertEqual(req2.status_code, 401)
        # except ValueError as e:
        #     print(e)

        test.delete_token('testt', 'testp', 'test')
        test.delete_project('testp', 'test')
        test.delete_dataset('test')

if __name__ == '__main__':
    unittest.main()
