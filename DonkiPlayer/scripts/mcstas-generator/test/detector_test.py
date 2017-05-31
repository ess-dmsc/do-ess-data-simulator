import unittest
import numpy as np
import sys
sys.path.append('../src')
from detector import *


class Test1dDetector(unittest.TestCase):
    t = ToF('tof_test.txt')
        
    def test_extract_info(self) :
        self.assertTrue(self.t.xmin == 0)
        self.assertTrue(self.t.xmax == 1)
        self.assertTrue(self.t.Ncount == 1000)

    def test_count(self) :
        self.assertEqual(self.t.count(),750.0)

    def test_randomize(self) :
        self.t.randomize(100)
        self.assertEqual(np.count_nonzero(np.where( 0 > self.t.n )),0)
        self.assertEqual(np.count_nonzero(np.where( 1 < self.t.n )),0)


class Test2dDetector(unittest.TestCase):
    t = D2('d2_test.txt')
        
    def test_extract_info(self) :
        self.assertTrue(self.t.n.all() > 0)
        self.assertTrue(np.amax(self.t.n) == 98765)
        self.assertTrue(np.amin(self.t.n) == 12345)
        self.assertTrue(self.t.Ncount == 1e+03 )
    
    def test_count(self) :
        self.assertTrue(self.t.count() == (12345+23456+34567+98765+87654+76543))

                
if __name__ == '__main__':
    unittest.main()
