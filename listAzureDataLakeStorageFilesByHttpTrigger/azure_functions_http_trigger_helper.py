import re
from base64 import urlsafe_b64encode, urlsafe_b64decode
from urllib.parse import unquote


class AzureFunctionsHttpTriggerHelper(object):

    @staticmethod
    def decode_request_params(param) -> str:
        try:
            if re.match(re.compile(r'^[A-Za-z0-9+/]+={0,2}$'), param) and urlsafe_b64encode(urlsafe_b64decode(param)) == param.encode('utf-8'):
                param = urlsafe_b64decode(param).decode('utf-8')
        except Exception as e:
            pass

        try:
            if re.match(re.compile(r'(%[0-9a-fA-F]{2}|[^<>\'" %])+'), param):
                param = unquote(param)
        except Exception as e:
            print(f"{e} in unquote, param = {param}")
            pass

        return param
