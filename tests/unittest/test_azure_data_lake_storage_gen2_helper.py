#!/usr/bin/env python

import unittest
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path
from datetime import datetime
from copy import deepcopy


sys.path.append(str(Path(__file__).parent.parent.parent / 'listAzureDataLakeStorageFilesByHttpTrigger'))
from listAzureDataLakeStorageFilesByHttpTrigger.azure_data_lake_storage_gen2_helper import AzureDataLakeStorageGen2Helper


class TestAzureDataLakeStorageGen2Helper(unittest.TestCase):

    data_lake_service_client = 'listAzureDataLakeStorageFilesByHttpTrigger.azure_data_lake_storage_gen2_helper.DataLakeServiceClient'
    strptime_format = '%Y-%m-%dT%H:%M:%SZ'

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.helper = AzureDataLakeStorageGen2Helper(account_name='my_account_name', account_key='my_account_key', container_name='my_container_name')
        self.helper.data_lake_service_client = MagicMock()
        self.helper.file_system_client = MagicMock()
        self.helper.file_system_client.get_paths = MagicMock()
        self.helper.folder_name = 'my_folder_name'

    def tearDown(self):
        pass


    @patch(data_lake_service_client)
    def test_list_files(self, _):
        self.helper.file_system_client.get_paths.return_value = [
            MagicMock(is_directory=False),
            MagicMock(is_directory=True),
            MagicMock(is_directory=False),
            MagicMock(is_directory=True),
            MagicMock(is_directory=False),
        ]

        self.assertEqual(3, len(self.helper.list_files()))


    @patch(data_lake_service_client)
    def test_filter_files_list_by_extension(self, _):
        self.helper.file_system_client.get_paths.return_value = [
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
        ]

        names = [
            'file1.txt',
            'file2.csv',
            'file3.txt',
            'file4.json',
            'file5.txt',
        ]

        for i in range(len(names)):
            self.helper.file_system_client.get_paths.return_value[i].name = names[i]
        # NOTE: The name attribute cannot be mocked during creation of the MagicMock object, since it has special meaning:
        #   > name: If the mock has a name then it will be used in the repr of the mock. This can be useful for debugging. The name is propagated to child mocks.
        #   https://python.readthedocs.io/en/latest/library/unittest.mock.html#unittest.mock.Mock
        #   Therefore, in order to mock the name, it shall be set after creating the Mock or MagicMock object and before passing it forward.

        files = self.helper.filter_files_list_by_extension(
            files_list=self.helper.file_system_client.get_paths(),
            extension='.txt')

        self.assertEqual(3, len(files))
        self.assertEqual('file1.txt', files[0].name)
        self.assertEqual('file3.txt', files[1].name)
        self.assertEqual('file5.txt', files[2].name)


    @patch(data_lake_service_client)
    def test_filter_files_list_by_last_modified(self, _):
        self.helper.file_system_client.get_paths.return_value = [
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-02T23:59:59Z', self.strptime_format)),
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-03T00:00:00Z', self.strptime_format)),
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-03T11:59:59Z', self.strptime_format)),
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-03T23:59:59Z', self.strptime_format)),
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-04T00:00:00Z', self.strptime_format)),
        ]

        files = self.helper.filter_files_list_by_last_modified(
            files_list=self.helper.file_system_client.get_paths(),
            modified_since=datetime.strptime('2020-01-03T00:00:00Z', self.strptime_format))
        
        self.assertEqual(4, len(files))
        self.assertEqual('2020-01-03T00:00:00Z', files[0].last_modified.strftime(self.strptime_format))
        self.assertEqual('2020-01-03T11:59:59Z', files[1].last_modified.strftime(self.strptime_format))
        self.assertEqual('2020-01-03T23:59:59Z', files[2].last_modified.strftime(self.strptime_format))
        self.assertEqual('2020-01-04T00:00:00Z', files[3].last_modified.strftime(self.strptime_format))


    @patch(data_lake_service_client)
    def test_filter_files_list_by_last_modified_and_extension(self, _):
        self.helper.file_system_client.get_paths.return_value = [
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-02T23:59:59Z', self.strptime_format)),
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-03T00:00:00Z', self.strptime_format)),
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-03T11:59:59Z', self.strptime_format)),
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-03T23:59:59Z', self.strptime_format)),
            MagicMock(is_directory=False, last_modified=datetime.strptime('2020-01-04T00:00:00Z', self.strptime_format)),
        ]

        names = [
            'file1.txt',
            'file2.csv',
            'file3.txt',
            'file4.json',
            'file5.txt',
        ]

        for i in range(len(names)):
            self.helper.file_system_client.get_paths.return_value[i].name = names[i]

        files = self.helper.filter_files_list_by_last_modified_and_extension(
            files_list=self.helper.file_system_client.get_paths(),
            modified_since=datetime.strptime('2020-01-03T00:00:00Z', self.strptime_format),
            extension='.txt')
        
        self.assertEqual(2, len(files))
        self.assertEqual('2020-01-03T11:59:59Z', files[0].last_modified.strftime(self.strptime_format))
        self.assertEqual('2020-01-04T00:00:00Z', files[1].last_modified.strftime(self.strptime_format))
        self.assertEqual('file3.txt', files[0].name)
        self.assertEqual('file5.txt', files[1].name)


    @patch(data_lake_service_client)
    def test_add_container_name(self, _):
        self.helper.file_system_client.get_paths.return_value = [
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
        ]

        self.helper.file_system_client.get_file_system_properties = MagicMock()
        self.helper.file_system_client.get_file_system_properties.return_value = MagicMock()

        for i in range(len(self.helper.file_system_client.get_paths.return_value)):
            self.helper.file_system_client.get_paths.return_value[i].container_name = 'my_container_name'

        files = self.helper.add_container_name(files_list=self.helper.file_system_client.get_paths())

        self.assertEqual(5, len(files))
        self.assertEqual('my_container_name', files[0].container_name)
        self.assertEqual('my_container_name', files[1].container_name)
        self.assertEqual('my_container_name', files[2].container_name)
        self.assertEqual('my_container_name', files[3].container_name)
        self.assertEqual('my_container_name', files[4].container_name)


    @patch(data_lake_service_client)
    def test_add_file_name_to_each_list_item(self, _):
        self.helper.file_system_client.get_paths.return_value = [
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
        ]

        names = [
            'dir/file1.txt',
            'dir/file2.csv',
            'dir/file3.txt',
            'dir/file4.json',
            'dir/file5.txt',
        ]

        for i in range(len(names)):
            self.helper.file_system_client.get_paths.return_value[i].name = names[i]

        self.helper._add_item_from_name = MagicMock()
        self.helper._add_item_from_name.return_value = [
            MagicMock(file_name='file1.txt'),
            MagicMock(file_name='file2.csv'),
            MagicMock(file_name='file3.txt'),
            MagicMock(file_name='file4.json'),
            MagicMock(file_name='file5.txt'),
        ]

        files = self.helper.add_file_name_to_each_list_item(files_list=self.helper.file_system_client.get_paths())

        self.assertEqual(5, len(files))
        self.assertEqual('file1.txt', files[0].file_name)
        self.assertEqual('file2.csv', files[1].file_name)
        self.assertEqual('file3.txt', files[2].file_name)
        self.assertEqual('file4.json', files[3].file_name)
        self.assertEqual('file5.txt', files[4].file_name)


    @patch(data_lake_service_client)
    def test_add_directory_path_to_each_list_item(self, _):
        self.helper.file_system_client.get_paths.return_value = [
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
        ]

        names = [
            'dir/file1.txt',
            'dir/file2.csv',
            'dir/file3.txt',
            'dir/file4.json',
            'dir/file5.txt',
        ]

        for i in range(len(names)):
            self.helper.file_system_client.get_paths.return_value[i].name = names[i]

        self.helper._add_item_from_name = MagicMock()
        self.helper._add_item_from_name.return_value = [
            MagicMock(directory_path='dir'),
            MagicMock(directory_path='dir'),
            MagicMock(directory_path='dir'),
            MagicMock(directory_path='dir'),
            MagicMock(directory_path='dir'),
        ]

        files = self.helper.add_directory_path_to_each_list_item(files_list=self.helper.file_system_client.get_paths())

        self.assertEqual(5, len(files))
        self.assertEqual('dir', files[0].directory_path)
        self.assertEqual('dir', files[1].directory_path)
        self.assertEqual('dir', files[2].directory_path)
        self.assertEqual('dir', files[3].directory_path)
        self.assertEqual('dir', files[4].directory_path)


    @patch(data_lake_service_client)
    def test_add_file_extension_to_each_list_item(self, _):
        self.helper.file_system_client.get_paths.return_value = [
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
            MagicMock(is_directory=False),
        ]

        names = [
            'dir/file1.txt',
            'dir/file2.csv',
            'dir/file3.txt',
            'dir/file4.json',
            'dir/file5.txt',
        ]

        for i in range(len(names)):
            self.helper.file_system_client.get_paths.return_value[i].name = names[i]

        self.helper._add_item_from_name = MagicMock()
        self.helper._add_item_from_name.return_value = [
            MagicMock(file_extension='.txt'),
            MagicMock(file_extension='.csv'),
            MagicMock(file_extension='.txt'),
            MagicMock(file_extension='.json'),
            MagicMock(file_extension='.txt'),
        ]

        files = self.helper.add_file_extension_to_each_list_item(files_list=self.helper.file_system_client.get_paths())

        self.assertEqual(5, len(files))
        self.assertEqual('.txt', files[0].file_extension)
        self.assertEqual('.csv', files[1].file_extension)
        self.assertEqual('.txt', files[2].file_extension)
        self.assertEqual('.json', files[3].file_extension)
        self.assertEqual('.txt', files[4].file_extension)


    @patch(data_lake_service_client)
    def test__add_item_from_name(self, _):
        # TODO: Implement this test
        pass


    @patch(data_lake_service_client)
    def test_list_files_in_json(self, _):
        # TODO: Implement this test
        pass



if __name__ == '__main__':
    unittest.main()
