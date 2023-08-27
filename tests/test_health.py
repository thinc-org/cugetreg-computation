import unittest
from cgrcompute.components.health import HealthServicer
from concurrent.futures import ProcessPoolExecutor, BrokenExecutor
import os
import signal

class HealthServicerTestCase(unittest.TestCase):
    
    def test_ping(self):
        self.assertEqual(HealthServicer.ping(), "pong")


    @staticmethod
    def _poison():
        # Simulate OOM
        os.kill(os.getpid(), signal.SIGKILL)

    def test_check_pool(self):
        pool = ProcessPoolExecutor(max_workers=2)
        srv = HealthServicer(pool=pool)
        # Pool is ok
        self.assertTrue(srv._check_pool())

        # Pool Broken
        self.assertRaises(BrokenExecutor, lambda: pool.submit(HealthServicerTestCase._poison).result(timeout=5))

        self.assertFalse(srv._check_pool())

