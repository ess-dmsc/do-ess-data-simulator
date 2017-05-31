import unittest
import numpy as np
import sys
import json

sys.path.append('../src')

import kafkaGenerator

class TestKafkaGenerator(unittest.TestCase):
    generator = kafkaGenerator.generatorSource('192.168.10.11','mcstas-test',1)
    def test_setup(self) :
        self.assertTrue(self.generator.broker == '192.168.10.11')
        self.assertTrue(self.generator.topic == 'mcstas-test')
        self.assertTrue(self.generator.multiplier == 1)

#    def test_connect(self) :
#        try:
#            return json.loads(json.dumps(self.instrument.header))
#        except ValueError:
#            self.fail("Wrong header format")
        
#    def test_run(self) :
#        expected_size = self.instrument.d.size
#        data = self.instrument.mcstas2stream([0,0,0,0,0])
#        self.assertTrue(expected_size,data.size)

        
if __name__ == '__main__':
    unittest.main()
