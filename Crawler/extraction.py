from bs4 import BeautifulSoup
import requests
import time
import re
from urllib.parse import urljoin, urldefrag, urlsplit, urlparse

class extractors():
    def __init__(self,body,Project, url,**selectors):
        self.url = url
        self.project = Project
        soup = BeautifulSoup(body)
        print(selectors.values())
        for i in selectors:
            print(i)



