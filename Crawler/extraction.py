from bs4 import BeautifulSoup
import requests
import time
import re
from urllib.parse import urljoin, urldefrag, urlsplit, urlparse

class extractors():
    def __init__(self,body,Project, url,selectors):
        self.url = url
        self.project = Project
        soup = BeautifulSoup(body)
        string = []
        for i in selectors:
            for key, val in i.items():
                #print(len(key))
                attrib = val[0]
                id = val[1]
                #print(attrib)
                result = soup.find(key, attrs={attrib: id})
                result = result.get_text()
                try:
                    result = result.strip('\n')
                    result = result.replace('\n', '')
                    result = result.replace('|', ' ')
                except:
                    pass
                string.append(str(result))
        with open('extraction-working-new-sitemap.csv','a',encoding='utf-8') as file:
            string = '|'.join(string)
            file.write('{}|{}\n'.format(url, string))
            #print('complete')




