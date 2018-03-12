from stem import Signal
from stem.control import Controller
import requests


def set_new_ip( ):
    """Change IP using TOR"""
    with Controller.from_port( port = 9051 ) as controller:
        controller.authenticate( )
        controller.signal( Signal.NEWNYM )

def get_tor_session():
    session = requests.session()
    # Tor uses the 9050 port as the default socks port
    session.proxies = {'http':  'socks5://127.0.0.1:9150',
                       'https': 'socks5://127.0.0.1:9150'}

    session.verify = False

    return session

# Make a request through the Tor connection
# IP visible through Tor
session = get_tor_session()
print(session.get("http://httpbin.org/ip").text)
# Above should print an IP different than your public IP


local_proxy = 'socks5://localhost:9150'
socks_proxy = { 'http': local_proxy, 'https': local_proxy }

current_ip = requests.get( url = 'http://icanhazip.com/', proxies = socks_proxy, verify = False )
print( current_ip.text )

set_new_ip( )

current_ip = requests.get( url = 'http://icanhazip.com/', proxies = socks_proxy, verify = False )
print( current_ip.text )

import requests

url = "http://www.twse.com.tw/exchangeReport/TWT93U"

querystring = {"response":"json","date":"20180309","_":"1520856196380"}

headers = {
    'accept': "application/json, text/javascript, */*; q=0.01",
    'x-requested-with': "XMLHttpRequest",
    'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
    'referer': "http://www.twse.com.tw/zh/page/trading/exchange/TWT93U.html",
    'accept-encoding': "gzip, deflate",
    'accept-language': "zh-TW,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
    'cookie': "_ga=GA1.3.1838061013.1520518370; _gid=GA1.3.1972994029.1520761897; JSESSIONID=627854663D0CFAB13435F0FA8680B206",
    'cache-control': "no-cache",
    'postman-token': "6d02623d-84a1-365b-f2c5-7a72e37dec2e"
    }

response = session.request("GET", url, headers=headers, params=querystring)

print(response.text)
