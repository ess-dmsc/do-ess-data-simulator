import unittest
import numpy as np
import sys
import json
import fileinput
from pprint import pprint
import time

sys.path.append('../src')

from control import FileControl
from control import CommandLineControl


class TestFileControl(unittest.TestCase):
    ctl = FileControl('control_test.in')
    original_value = ctl.dump()
    
    def test_setup(self) :
        self.assertEqual(self.ctl.get('run'),'run' )
        self.assertNotEqual(self.ctl.get('run'),'stop' )
        self.assertNotEqual(self.ctl.get('run'),'pause' )

    def test_changes(self) :
        return
        

class TestCommandLineControl(unittest.TestCase):
    ctl = CommandLineControl('control_test.in')
    original_value = ctl.dump()
    
    def test_setup(self) :
        self.assertEqual(self.ctl.get('run'),'run' )
        self.assertNotEqual(self.ctl.get('run'),'stop' )
        self.assertNotEqual(self.ctl.get('run'),'pause' )

    def test_changes(self) :
        return

if __name__ == '__main__':
    unittest.main()
    
