import unittest
import test_settings
import ndio.remote.neurodata as neurodata
import ndio.ramon
import ndio.convert.png as ndpng
import ndio.convert.tiff as ndtiff
import numpy as np


class TestDownload(unittest.TestCase):

    def setUp(self):
        self.hostname = test_settings.HOSTNAME
        self.token_user = test_settings.NEURODATA
        self.nd = neurodata(user_token=self.token_user, hostname=self.hostname)
        self.image_data = np.zeros((512,512), dtype=np.uint8)

        # self.image_data = (255.0 / data.max() * (data - data.min())).astype(np.uint8)

    def test_export_load_png(self):

        # if returns string, successful export
        self.assertEqual(
                ndpng.save("trash/download.png", self.image_data),
                "trash/download.png")

        # now confirm import works too
        self.assertEqual(ndpng.load("trash/download.png")[0][0],
                         self.image_data[0][0])
        self.assertEqual(ndpng.load("trash/download.png")[10][10],
                         self.image_data[10][10])


    def test_export_load_tiff(self):
        # if returns string, successful export
        self.assertEqual(
                         ndtiff.save("trash/download-1.tiff",
                                     self.image_data),
                         "trash/download-1.tiff")

        # now confirm import works too
        self.assertEqual(
                         ndtiff.load("trash/download-1.tiff")[0][0],
                         self.image_data[0][0])
        self.assertEqual(
                         ndtiff.load("trash/download-1.tiff")[10][10],
                         self.image_data[10][10])

if __name__ == '__main__':
    unittest.main()
