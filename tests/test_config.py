import unittest
import cgrcompute.components.config as cfg

class ConfigTest(unittest.TestCase):

    def test_parse_config(self):
        config = cfg.parse_config()

if __name__ == '__main__':
    unittest.main()
