import logging
from base64 import b64decode, b64encode
from urllib.parse import unquote, quote
import re
from datetime import datetime

import azure.functions as func
from .azure_data_lake_storage_gen2_helper import AzureDataLakeStorageGen2Helper


RE_BASE64_STRING = re.compile(r'^[A-Za-z0-9+/]+={0,2}$')
RE_URLENCODED_STRING = re.compile(r'(%[0-9a-fA-F]{2}|[^<>\'" %])+')


def _decode_request_params(param) -> str:
    if re.match(RE_BASE64_STRING, param) and b64encode(b64decode(param)) == param.encode('utf-8'):
        logging.debug(f"base64 decode {param}")
        param = b64decode(param).decode('utf-8')
    if re.match(RE_URLENCODED_STRING, param) and quote(unquote(param)) == param:
        logging.debug(f"url decode {param}")
        param = unquote(param)
    logging.debug(f"decoded_param = {param}")
    return param


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f'Started listAzureDataLakeStorageFilesByHttpTrigger function at {datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%SZ")}')

    try:
        if connection_string := req.params.get('connection_string'):
            connection_string = _decode_request_params(connection_string)
        if account_name := req.params.get('account_name'):
            account_name = _decode_request_params(account_name)
        if account_key := req.params.get('account_key'):
            account_key = _decode_request_params(account_key)
        if container_name := req.params.get('container_name'):
            container_name = _decode_request_params(container_name)
        if folder_name := req.params.get('folder_name'):
            folder_name = _decode_request_params(folder_name)
        if extension := req.params.get('extension'):
            extension = _decode_request_params(extension)
        if modified_since := req.params.get('modified_since'):
            modified_since = datetime.strptime(_decode_request_params(modified_since), "%Y-%m-%dT%H:%M:%SZ")

        if connection_string:
            adls2_helper = AzureDataLakeStorageGen2Helper(
                connection_string=connection_string,
                container_name=container_name,
                folder_name=folder_name
            )
        else:
            adls2_helper = AzureDataLakeStorageGen2Helper(
                account_name=account_name,
                account_key=account_key,
                container_name=container_name,
                folder_name=folder_name
            )

        filter_functions = [
            adls2_helper.add_container_name,
            adls2_helper.add_file_name_to_each_list_item,
            adls2_helper.add_directory_path_to_each_list_item,
            adls2_helper.add_file_extension_to_each_list_item
        ]

        if extension and not modified_since:
            files = adls2_helper.list_files_in_json(
                list_func=adls2_helper.list_files_by_extension,
                filter_funcs=filter_functions,
                extension=extension
            )
        elif not extension and modified_since:
            files = adls2_helper.list_files_in_json(
                list_func=adls2_helper.list_files_by_last_modified,
                filter_funcs=filter_functions,
                modified_since=modified_since
            )
        elif extension and modified_since:
            files = adls2_helper.list_files_in_json(
                list_func=adls2_helper.list_files_by_last_modified_and_extension,
                filter_funcs=filter_functions,
                modified_since=modified_since,
                extension=extension
            )
        else:
            files = adls2_helper.list_files_in_json(filter_funcs=filter_functions)

        logging.info(f"req.url = {req.url}")
        logging.info(f"req.headers = {[x for x in req.headers]}")
        logging.info(f"req.params = {req.params}")
        logging.info(f"req.get_body() = {req.get_body()}")
        logging.info(f"files = {files}")
        return func.HttpResponse(files, status_code=200, mimetype="application/json", charset="utf-8")
    except Exception as e:
        logging.error(e)
        return func.HttpResponse(str(e), status_code=500, mimetype="text/plain", charset="utf-8")
    finally:
        logging.info(f'Finished listAzureDataLakeStorageFilesByHttpTrigger function at {datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%SZ")}')
        logging.shutdown()
