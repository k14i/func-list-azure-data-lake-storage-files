import logging
from base64 import b64decode
from datetime import datetime

import azure.functions as func
from .azure_data_lake_storage_gen2_helper import AzureDataLakeStorageGen2Helper


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f'Started listAzureDataLakeStorageFilesByHttpTrigger function at {datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%SZ")}')

    try:
        if connection_string := req.params.get('connection_string'):
            connection_string = b64decode(connection_string).decode('utf-8')
        if account_name := req.params.get('account_name'):
            account_name = b64decode(account_name).decode('utf-8')
        if account_key := req.params.get('account_key'):
            account_key = b64decode(account_key).decode('utf-8')
        if container_name := req.params.get('container_name'):
            container_name = b64decode(container_name).decode('utf-8')
        if folder_name := req.params.get('folder_name'):
            folder_name = b64decode(folder_name).decode('utf-8')
        if extension := req.params.get('extension'):
            extension = b64decode(extension).decode('utf-8')
        if modified_since := req.params.get('modified_since'):
            modified_since = datetime.strptime(b64decode(modified_since).decode('utf-8'), "%Y-%m-%dT%H:%M:%SZ")

        if connection_string:
            adls2_helper = AzureDataLakeStorageGen2Helper(connection_string=connection_string, container_name=container_name, folder_name=folder_name)
        else:
            adls2_helper = AzureDataLakeStorageGen2Helper(account_name=account_name, account_key=account_key, container_name=container_name, folder_name=folder_name)

        if extension and not modified_since:
            files = adls2_helper.list_files_in_json(
                list_func=adls2_helper.list_files_by_extension,
                extension=extension
            )
        elif not extension and modified_since:
            files = adls2_helper.list_files_in_json(
                list_func=adls2_helper.list_files_by_last_modified,
                modified_since=modified_since
            )
        elif extension and modified_since:
            files = adls2_helper.list_files_in_json(
                list_func=adls2_helper.list_files_by_last_modified_and_extension,
                modified_since=modified_since,
                extension=extension
            )
        else:
            files = adls2_helper.list_files_in_json()

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
