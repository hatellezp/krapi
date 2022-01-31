import unittest

from kra_api.api import Kapi


class TestKraApi(unittest.TestCase):

    def test_test_connection(self):
        self.assertEqual(2, Kapi.test_connection())


if __name__ == "__main__":
    unittest.main()
