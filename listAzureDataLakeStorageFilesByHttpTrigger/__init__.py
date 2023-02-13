import logging
import re
from datetime import datetime

import azure.functions as func

from .azure_data_factory_azure_functions_activity_helper import AzureDataFactoryAzureFunctionsActivityHelper
from .azure_data_lake_storage_gen2_helper import AzureDataLakeStorageGen2Helper
from .azure_functions_http_trigger_helper import AzureFunctionsHttpTriggerHelper


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f'Started listAzureDataLakeStorageFilesByHttpTrigger function at {datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%SZ")}')

    remove_quotes = AzureDataFactoryAzureFunctionsActivityHelper.remove_quotes
    decode_request_params = AzureFunctionsHttpTriggerHelper.decode_request_params

    try:
        if connection_string := req.params.get('connection_string'):
            connection_string = decode_request_params(remove_quotes(connection_string))
        if account_name := req.params.get('account_name'):
            account_name = decode_request_params(remove_quotes(account_name))
        if account_key := req.params.get('account_key'):
            account_key = decode_request_params(remove_quotes(account_key))
        if container_name := req.params.get('container_name'):
            container_name = decode_request_params(remove_quotes(container_name))
        if folder_name := req.params.get('folder_name'):
            folder_name = decode_request_params(remove_quotes(folder_name))
        if extension := req.params.get('extension'):
            extension = decode_request_params(remove_quotes(extension))
            # TODO: support extensions and will be comma separated string
        if modified_since := req.params.get('modified_since'):
            modified_since = decode_request_params(remove_quotes(modified_since))
            if re.match(re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'), modified_since):
                format = "%Y-%m-%dT%H:%M:%S.%fZ"
            elif re.match(re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'), modified_since):
                format = "%Y-%m-%dT%H:%M:%SZ"
            elif re.match(re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'), modified_since):
                format = "%Y-%m-%d"
            elif re.match(re.compile(r'^[0-9]{4}-[0-9]{2}$'), modified_since):
                format = "%Y-%m"
            elif re.match(re.compile(r'^[0-9]{4}$'), modified_since):
                format = "%Y"
            else:
                raise ValueError(f"modified_since parameter is invalid. modified_since = {modified_since}")
            modified_since = datetime.strptime(modified_since, format)

        if connection_string:
            adls2_helper = AzureDataLakeStorageGen2Helper(
                connection_string=connection_string,
                container_name=container_name,
                folder_name=folder_name
            )
        elif account_name and account_key:
            adls2_helper = AzureDataLakeStorageGen2Helper(
                account_name=account_name,
                account_key=account_key,
                container_name=container_name,
                folder_name=folder_name
            )
        else:
            raise ValueError("connection_string or account_name and account_key parameters are required.")

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
