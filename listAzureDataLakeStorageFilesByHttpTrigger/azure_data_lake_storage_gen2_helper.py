#!/usr/bin/env python

import os
import json
from datetime import datetime

from azure.storage.filedatalake import DataLakeServiceClient


class AzureDataLakeStorageGen2Helper(object):
    def __init__(self, connection_string=None, account_name=None, account_key=None, file_system_name=None, folder_name=None):
        if not connection_string:
            connection_string = os.environ.get("AzureStorageConnectionString")
        
        try:
            if connection_string:
                self.data_lake_service_client = DataLakeServiceClient.from_connection_string(conn_str=connection_string)
            else:
                if not account_name:
                    account_name = os.environ["AzureStorageAccountName"]
                if not account_key:
                    account_key = os.environ["AzureStorageAccountKey"]
                self.data_lake_service_client = DataLakeServiceClient(
                    account_url="{}://{}.dfs.core.windows.net".format("https", account_name),
                    credential=account_key
                )
            if not file_system_name:
                file_system_name = os.environ["AzureStorageAccountContainerName"]
            self.file_system_client = self.data_lake_service_client.get_file_system_client(file_system=file_system_name)

            if (not folder_name) and (os.environ.get("AzureStorageAccountFolderName")):
                self.folder_name = os.environ["AzureStorageAccountFolderName"]
            elif not folder_name:
                self.folder_name = ""
            else:
                self.folder_name = folder_name
        except Exception as e:
            raise Exception(e)
    
    def _get_file_paths(self, path: str) -> list:
        return [x for x in self.file_system_client.get_paths(path=path) if not x.is_directory]

    def list_files(self, *args, **kwargs) -> list:
        return self._get_file_paths(path=self.folder_name)

    def list_files_by_extension(self, extension: str, *args, **kwargs) -> list:
        return self.filter_files_list_by_extension(self.list_files(), extension=extension)

    def list_files_by_last_modified(self, modified_since: datetime, *args, **kwargs) -> list:
        return self.filter_files_list_by_last_modified(self.list_files(), modified_since=modified_since)

    def list_files_by_last_modified_and_extension(self, modified_since: datetime, extension: str, *args, **kwargs) -> list:
        return self.filter_files_list_by_last_modified_and_extension(self.list_files(), modified_since=modified_since, extension=extension)

    def filter_files_list_by_extension(self, files_list: list, extension: str, *args, **kwargs) -> list:
        return [x for x in files_list if x.name.endswith(extension)]
    
    def filter_files_list_by_last_modified(self, files_list: list, modified_since: datetime, *args, **kwargs) -> list:
        return [x for x in files_list if x.last_modified >= modified_since]

    def filter_files_list_by_last_modified_and_extension(self, files_list: list, modified_since: datetime, extension: str, *args, **kwargs) -> list:
        return [x for x in files_list if x.last_modified >= modified_since and x.name.endswith(extension)]

    def list_files_in_json(self, list_func=None, filter_funcs=[], *args, **kwargs) -> str:
        if not list_func:
            list_func = self.list_files
        files_list = list_func(*args, **kwargs)
        for func in filter_funcs:
            files_list = func(files_list, *args, **kwargs)
        return json.dumps(
            files_list,
            default=lambda x: x.isoformat() if isinstance(x, datetime) else x.__dict__,
            ensure_ascii=False
        )


if __name__ == "__main__":
    pass
    # adls2_helper = AzureDataLakeStorageGen2Helper()
    # retval = adls2_helper.list_files_in_json(filter_funcs=[adls2_helper.filter_files_list_by_extension, adls2_helper.filter_files_list_by_last_modified], extension=".json", modified_since=datetime(2023, 1, 1, 0, 0, 0))
    # print(retval)
