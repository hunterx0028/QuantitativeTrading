"""日內行情專用 HTTP transport；不修改 SDK 或 requests 的全域行為。"""
from types import MethodType
from urllib.parse import urlencode

import requests
from urllib3.util import Timeout


def install_intraday_timeout(client, timeout_seconds):
    """只覆寫此 Intraday 實例的 request，沿用 SDK 的 URL 與認證格式。

    total 會將建立連線耗時扣除後用於讀取等待；並非整個 HTTP 回應
    （例如持續慢速傳輸或 DNS 解析）的硬性 wall-clock deadline。
    """
    if timeout_seconds <= 0:
        raise ValueError('行情 HTTP timeout 必須大於 0')

    def request(instance, path, **params):
        config = instance.config
        headers = {}
        for key, header in (('api_key', 'X-API-KEY'), ('bearer_token', 'Authorization'),
                            ('sdk_token', 'X-SDK-TOKEN')):
            if config.get(key):
                headers[header] = f"Bearer {config[key]}" if key == 'bearer_token' else config[key]
        endpoint = path if path.startswith('/') else '/' + path
        query = '?' + urlencode(params) if params else ''
        with requests.get(
            config['base_url'] + endpoint + query,
            headers=headers,
            timeout=Timeout(total=timeout_seconds, connect=timeout_seconds, read=timeout_seconds),
        ) as response:
            response.raise_for_status()
            return response.json()

    client.request = MethodType(request, client)
    return client
