import requests


class ApiRequest(object):
    def __init__(self):
        requests.DEFAULT_RETRIES = 5
        self.res = requests.session()
        self.res.keep_alive = False

    def request(self, url, data):
        for i in range(0, 3):
            try:
                result = self.res.post(url, data=data).json()
                return result
            except Exception as e:
                if i == 2:
                    return f"request_failed:{str(e)}"
                else:
                    pass
