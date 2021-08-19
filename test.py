#!/usr/bin/env python3

import unittest

class TestMapReduce(unittest.TestCase):

    def test_output(self):
        lines = open("output.txt", "r").readlines()
        passed = any("PASSED ALL TESTS" in line for line in lines)

        self.assertEqual(passed, True)

if __name__ == '__main__':
    unittest.main()