#!/usr/bin/env python3

import unittest

class TestMapReduce(unittest.TestCase):
    def __init__(self, *args, **kargs):
        super(TestMapReduce, self).__init__(*args, **kargs)
        
        with open("./output.txt", "r") as file:
            self.input = file.readlines()
        
    def __assert_mr__(self, keyword):
        passed = any(keyword in line for line in self.input)

        self.assertEqual(passed, True)
    
    def test_wc(self):
        self.__assert_mr__("wc test: PASS")
    
    def test_indexer(self):
        self.__assert_mr__("indexer test: PASS")
    
    def test_grep(self):
        self.__assert_mr__("grep test: PASS")

    def test_parallel_map(self):
        self.__assert_mr__("map parallelism test: PASS")

    def test_parallel_reduce(self):
        self.__assert_mr__("reduce parallelism test: PASS")

    def test_job_count(self):
        self.__assert_mr__("job count test: PASS")

    def test_early_exit(self):
        self.__assert_mr__("early exit test: PASS")

    def test_crash(self):
        self.__assert_mr__("crash test: PASS")

if __name__ == '__main__':
    unittest.main()