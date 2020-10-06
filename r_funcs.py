from typing import List
import csv
import requests
from fake_useragent import UserAgent
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


headers = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko)'
                  ' Chrome/85.0.4183.102 Safari/537.36',
    'Origin': 'https://www.unissu.com',
    'Referer': 'https://www.unissu.com/',
    'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
}

def get_page(page_link: str, parameters: dict, proxies: dict = None, session=None):
    '''Sends requests and returns response'''
    session = session or requests.Session()
    retries = Retry(total=10, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries, pool_maxsize=100))
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_maxsize=100))
    user_agent = UserAgent()
    headers.update({'User-Agent': str(user_agent.random)})
    session.headers.update(headers)
    if proxies is None:
        r = session.get(page_link, params=parameters)
    else:
        r = session.get(page_link, params=parameters, proxies=proxies)

    if r.status_code != 200:
        print(r.status_code)
        raise requests.ConnectionError("Connection error is occured!")

    return r


def get_proxy(index):
    '''Fetch the proxy on the requested line '''
    with open("Proxies.csv", "r", encoding="utf-8") as readFile:
        proxies = csv.reader(readFile)
        for idx, item in enumerate(proxies):
            if index == idx:
                proxy_name: List = item[0].split(':')
                proxy = ''.join([proxy_name[0], ":", proxy_name[1]])
                return proxy
