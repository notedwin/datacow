import time
import requests
from fake_useragent import UserAgent
from stem import Signal
from stem.control import Controller
from stem.util.log import get_logger

logger = get_logger()
logger.propagate = False

proxies = {
    'http': 'socks5://127.0.0.1:9050',
    'https': 'socks5://127.0.0.1:9050'
}
print("Changing IP Address in every 10 seconds....\n\n")
while True:
    headers = { 'User-Agent': UserAgent().random }
    with Controller.from_port(port = 9051) as c:
        c.authenticate(password="swagmoney")
        c.signal(Signal.NEWNYM)
        print(f"Your IP is : {requests.get('https://ident.me', proxies=proxies, headers=headers).text}  ||  User Agent is : {headers['User-Agent']}")
    time.sleep(10)