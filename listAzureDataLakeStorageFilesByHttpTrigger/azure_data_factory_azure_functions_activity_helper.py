import re

class AzureDataFactoryAzureFunctionsActivityHelper(object):

    # NOTE: Azure Functions activity of Azure Data Factory Pipeline automatically add quotes to string parameters.
    #       For example, parameter foo becomes "foo" (not foo).
    #       So, we need to remove them.
    @staticmethod
    def remove_quotes(param: str) -> str:
        if re.match(re.compile(r'^[\'\"].*[\'\"]$'), param):
            param = param[1:-1]
        return param
