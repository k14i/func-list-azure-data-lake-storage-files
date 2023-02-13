import re
from base64 import b64encode, b64decode
from urllib.parse import quote, unquote


class AzureFunctionsHttpTriggerHelper(object):

    @staticmethod
    def decode_request_params(param) -> str:
        try:
            if re.match(re.compile(r'^[A-Za-z0-9+/]+={0,2}$'), param) and b64encode(b64decode(param)) == param.encode('utf-8'):
                param = b64decode(param).decode('utf-8')
        except Exception:
            pass

        try:
            if re.match(re.compile(r'(%[0-9a-fA-F]{2}|[^<>\'" %])+'), param) and quote(unquote(param)) == param:
                param = unquote(param)
        except Exception:
            pass

        return param
