#!/usr/bin/env python

import unittest
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / 'listAzureDataLakeStorageFilesByHttpTrigger'))
from listAzureDataLakeStorageFilesByHttpTrigger.azure_data_factory_azure_functions_activity_helper import AzureDataFactoryAzureFunctionsActivityHelper


class TestAzureDataFactoryAzureFunctionsActivityHelper(unittest.TestCase):

    def test_remove_quotes_with_a_single_set_of_quotes(self):
        self.assertEqual(AzureDataFactoryAzureFunctionsActivityHelper.remove_quotes('"foo"'), 'foo')

    def test_remove_quotes_without_quotes(self):
        self.assertEqual(AzureDataFactoryAzureFunctionsActivityHelper.remove_quotes('foo'), 'foo')

    def test_remove_quotes_with_quotes_in_the_middle(self):
        self.assertEqual(AzureDataFactoryAzureFunctionsActivityHelper.remove_quotes('"f"o"o"'), 'f"o"o')

    def test_remove_quotes_only_quotes(self):
        self.assertEqual(AzureDataFactoryAzureFunctionsActivityHelper.remove_quotes('""'), '')

    def test_remove_quotes_with_multiple_quotes(self):
        self.assertEqual(AzureDataFactoryAzureFunctionsActivityHelper.remove_quotes('""""'), '')


if __name__ == '__main__':
    unittest.main()
