from Data import *
from Searchterms import *
from extraction import *
from robotsgenerator import *
import aiohttp
import asyncio
from aiohttp import ClientSession
from urllib.parse import urljoin, urldefrag, urlsplit, urlparse
from bs4 import BeautifulSoup
import sqlite3
import time

data = []

async def get_body(url):
    async with ClientSession() as session:
        async with session.get(url, timeout=60) as response:
            response = await response.read()
            # print(response)
            return response

class spider():

    data = []

    @staticmethod
    async def handle_task(self,task_id, work_queue, Project):
        while not work_queue.empty():
            queue_url = await work_queue.get()
            print('Now crawling ' + queue_url + " | there are " + str(len(queue)) + " links in queue")
            try:
                body = await get_body(queue_url)
                self.send_to_json(body)
                print(len(body))
            except Exception as e:
                print(e)
                pass
            return self.data

    @staticmethod
    def send_to_json(body):
        data.append(body)

    def __init__ (self, queue):
        q = asyncio.Queue()
        [q.put_nowait(url) for url in queue]
        loop = asyncio.get_event_loop()
        tasks = [self.handle_task(self,task_id, q, 'list') for task_id in range(3)]
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()



queue= ['http://www.theluxetravel.com','http://www.adaptworldwide.com','http://www.theluxetravel.com']

results = spider(queue)
print(data)