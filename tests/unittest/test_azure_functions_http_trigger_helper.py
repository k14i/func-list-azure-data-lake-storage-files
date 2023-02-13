#!/usr/bin/env python

import unittest
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / 'listAzureDataLakeStorageFilesByHttpTrigger'))
from listAzureDataLakeStorageFilesByHttpTrigger.azure_functions_http_trigger_helper import AzureFunctionsHttpTriggerHelper


class TestAzureFunctionsHttpTriggerHelper(unittest.TestCase):

    def test_decode_request_params_with_zero_length_string(self):
        self.assertEqual(AzureFunctionsHttpTriggerHelper.decode_request_params(''), '')

    def test_decode_request_params_with_a_normal_string(self):
        self.assertEqual(AzureFunctionsHttpTriggerHelper.decode_request_params('foo'), 'foo')

    def test_decode_request_params_with_a_string_with_spaces(self):
        self.assertEqual(AzureFunctionsHttpTriggerHelper.decode_request_params('foo bar'), 'foo bar')

    def test_decode_request_params_with_a_string_with_spaces_and_plus_signs_and_percent_signs(self):
        self.assertEqual(AzureFunctionsHttpTriggerHelper.decode_request_params('foo%20bar'), 'foo bar')

    def test_decode_request_params_with_a_url_string(self):
        self.assertEqual(AzureFunctionsHttpTriggerHelper.decode_request_params('http://www.example.com/'), 'http://www.example.com/')

    def test_decode_request_params_with_a_base64_encoded_string(self):
        self.assertEqual(AzureFunctionsHttpTriggerHelper.decode_request_params('aHR0cDovL3d3dy5leGFtcGxlLmNvbS8='), 'http://www.example.com/')
    
    def test_decode_request_params_with_a_url_encoded_string(self):
        self.assertEqual(AzureFunctionsHttpTriggerHelper.decode_request_params('http%3A%2F%2Fwww.example.com%2F'), 'http://www.example.com/')

    def test_decode_request_params_with_a_base64_encoded_string_of_url_encoded_string(self):
        self.assertEqual(AzureFunctionsHttpTriggerHelper.decode_request_params('aHR0cCUzQSUyRiUyRnd3dy5leGFtcGxlLmNvbSUyRg=='), 'http://www.example.com/')


if __name__ == '__main__':
    unittest.main()
