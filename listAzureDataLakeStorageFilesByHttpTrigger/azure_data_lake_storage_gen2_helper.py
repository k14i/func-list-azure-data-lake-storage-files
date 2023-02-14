#!/usr/bin/env python

import os
import json
import re
from datetime import datetime

from azure.storage.filedatalake import DataLakeServiceClient

from azure.core.paging import ItemPaged
from typing import Union, Optional, List, Any, Dict, Tuple, Callable, TypeVar, Generic, cast, Type, TYPE_CHECKING, overload


class AzureDataLakeStorageGen2Helper(object):
    def __init__(self, connection_string=None, account_name=None, account_key=None, container_name=None, folder_name=None):
        if not connection_string:
            connection_string = os.environ.get("AzureStorageConnectionString")
        
        try:
            if connection_string:
                self.data_lake_service_client = DataLakeServiceClient.from_connection_string(conn_str=connection_string)
            else:
                if not account_name and not os.environ.get("AzureStorageAccountName"):
                    raise Exception("Account name is not specified both in the query parameters and in the environment variables.")
                elif not account_name:
                    account_name = os.environ["AzureStorageAccountName"]

                if not account_key and not os.environ.get("AzureStorageAccountKey"):
                    raise Exception("Account key is not specified both in the query parameters and in the environment variables.")
                elif not account_key:
                    account_key = os.environ["AzureStorageAccountKey"]

                self.data_lake_service_client = DataLakeServiceClient(
                    account_url="{}://{}.dfs.core.windows.net".format("https", account_name),
                    credential=account_key
                )

            if not container_name and not os.environ.get("AzureStorageAccountContainerName"):
                raise Exception("Container name is not specified both in the query parameters and in the environment variables.")
            elif not container_name:
                container_name = os.environ["AzureStorageAccountContainerName"]

            self.file_system_client = self.data_lake_service_client.get_file_system_client(file_system=container_name)

            if not folder_name and not os.environ.get("AzureStorageAccountFolderName"):
                self.folder_name = ""
            elif not folder_name and os.environ.get("AzureStorageAccountFolderName"):
                self.folder_name = os.environ["AzureStorageAccountFolderName"]
            elif folder_name:
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

    def filter_files_list_by_extension(self, files_list: Union[list, ItemPaged], extension: str, *args, **kwargs) -> list:
        return [x for x in files_list if x.name.endswith(extension)]
    
    def filter_files_list_by_last_modified(self, files_list: Union[list, ItemPaged], modified_since: datetime, *args, **kwargs) -> list:
        return [x for x in files_list if x.last_modified >= modified_since]

    def filter_files_list_by_last_modified_and_extension(self, files_list: Union[list, ItemPaged], modified_since: datetime, extension: str, *args, **kwargs) -> list:
        return [x for x in files_list if x.last_modified >= modified_since and x.name.endswith(extension)]

    def add_container_name(self, files_list: Any, *args, **kwargs) -> list:
        for i in range(len(files_list)):
            try:
                files_list[i]["container_name"] = self.file_system_client.get_file_system_properties().name
            except Exception as e:
                print(e)
                raise Exception(f"{e}: Container name is not set in self.file_system_client.file_system_name.")
        return files_list

    def _add_item_from_name(self, files_list: Any, new_item_name: str, source_item_name: str, regex: str) -> list:
        for i in range(len(files_list)):
            try:
                files_list[i][new_item_name] = re.findall(regex, files_list[i][source_item_name])[0]
            except Exception:
                files_list[i][new_item_name] = None
        return files_list

    def add_file_name_to_each_list_item(self, files_list: Union[list, ItemPaged], *args, **kwargs) -> list:
        return self._add_item_from_name(files_list, "file_name", "name", r'([^/]+)$')

    def add_directory_path_to_each_list_item(self, files_list: Union[list, ItemPaged], *args, **kwargs) -> list:
        return self._add_item_from_name(files_list, "directory_path", "name", r'^(.+)\/[^/]+')

    def add_file_extension_to_each_list_item(self, files_list: Union[list, ItemPaged], *args, **kwargs) -> list:
        return self._add_item_from_name(files_list, "file_extension", "name", r'.+(\..+)$')

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
