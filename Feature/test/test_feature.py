import os
import unittest

from feature.executor import Executor


class Feature(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Paths for the test directory
        cls.location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))  # test folder path
        cls.assets = os.path.join(cls.location, "assets")  # asset folder path

        cls.feature = Executor()

    def test_output(self):
        expected_res = ['Psychopharmacology', 'Journal of emergency nursing', 'The journal of maternal-fetal & neonatal medicine']
        res = self.feature.execute(os.path.join(self.assets, "conf"), self.assets)
        self.assertListEqual(res, expected_res)


if __name__ == '__main__':
    unittest.main()
