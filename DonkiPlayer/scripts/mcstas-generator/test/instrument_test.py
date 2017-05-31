import unittest
import numpy as np
import sys
import json

sys.path.append('../src')

from detector import D2
from instrument import Rita2


class TestRita2(unittest.TestCase):
    instrument = Rita2(D2('d2_test.txt'))
        
    def test_setup(self) :
        self.assertTrue(self.instrument.area.count() > 0 )
        self.assertTrue(self.instrument.d.size > 0 )
        self.assertFalse(np.any(self.instrument.d['ts'] > 0) )
        self.assertFalse(np.any(self.instrument.d['data'] > 0) )
        
    def test_dataHeader(self) :
        try:
            return json.loads(json.dumps(self.instrument.header))
        except ValueError:
            self.fail("Wrong header format")
        
    def test_stream(self) :
        expected_size = self.instrument.d.size
        data = self.instrument.mcstas2stream([0,0,0,0,0])
        self.assertTrue(expected_size,data.size)

        
if __name__ == '__main__':
    unittest.main()
