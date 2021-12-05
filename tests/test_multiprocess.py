import unittest
from multiprocessing import Manager, Process
from cgrcompute.components.multiprocess import SharableCache

class SharableCacheTest(unittest.TestCase):

    def test_can_cache(self):
        with Manager() as m:
            c = SharableCache(m)
            self.assertRaises(KeyError, lambda: c.get('k'))
            Process
            self.assertEqual('val', c.get_or_create('k', lambda : 'val'))
            c.update('k', 'val2')
            self.assertEqual('val2', c.get('k'))

if __name__ == '__main__':
    unittest.main()
