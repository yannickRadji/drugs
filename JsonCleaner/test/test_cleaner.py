import os
import unittest
from shutil import rmtree

from json_cleaner.executor import Executor


class Cleaner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Paths for the test directory
        cls.location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))  # test folder path
        cls.assets = os.path.join(cls.location, "assets")  # asset folder path
        cls.results_dir = os.path.join(cls.assets, "result")

        if os.path.exists(cls.results_dir):
            rmtree(cls.results_dir)
        os.makedirs(cls.results_dir)

        cls.cleaner = Executor()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.results_dir):
            rmtree(cls.results_dir)

    def test_output(self):
        expected_res = [[{'id': '', 'title': 'Bla, bla.', 'date': '01/03/2020', 'journal': 'The journal'}], [{'id': 9, 'title': 'Gold'}, {'id': '9', 'title': 'Blue'}]]
        res = self.cleaner.execute(os.path.join(self.assets, "conf"), self.assets, self.results_dir, False)
        self.assertListEqual(res, expected_res)


if __name__ == '__main__':
    unittest.main()
