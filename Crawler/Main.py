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

# Settings so that it's possible to pause & resume crawls. 3 Crawl modes, list, crawl, web

Project = 'Travelblogs'
Threads = 100
Limit = 100000000
crawl_type = 'web'
domains = set()
queue = []
root_url = "http://nomadicsamuel.com/top100travelblogs"
root_base = "{0.scheme}://{0.netloc}/".format(urlsplit(root_url))
robots = generate_robots(root_base)
print(robots)
list_file = 'list.txt'
queue.append(root_url)
crawled_urls, url_hub = [], [root_url]
now = time.strftime('%Y-%m-%d %H:%M')
db = sqlite3.connect(Project)
cursor = db.cursor()
create_tables(db, cursor)
into_campaigns(Project,now,cursor,db)

search_terms = searchterms()

selectors = [{'div': ['class','market-stats-text']}, {'span':['class','market-panel-stat-element-value js-market-stats-value-change market-panel-stat-value-change-up']},
             {'span':['class','market-panel-stat-element-value js-market-stats-average-price']}, {'span':['class','market-panel-stat-element-value js-market-stats-average-value']}
             ,{'span':['class','market-panel-stat-element-value js-market-stats-num-sales']}]


def resume():
    c = cursor.execute('SELECT DOMAIN FROM DOMAINS WHERE ID = "{c}"'.format(c=Project))
    list = c.fetchall()
    list = [x[0] for x in list]
    [domains.add(i) for i in list]

    d = cursor.execute('SELECT URL FROM QUEUE WHERE id = "{c}"'.format(c=Project))
    list1 = d.fetchall()
    list1 = [x[0] for x in list1]
    finallist1 = []
    for i in list1:
        domain = "{0.scheme}://{0.netloc}/".format(urlsplit(i))
        if domain not in domains and domain.find('http') != -1:
            finallist1.append(i)
    [queue.append(i) for i in finallist1]

    d = cursor.execute('SELECT URL FROM CRAWLED WHERE id = "{c}"'.format(c=Project))
    list1 = d.fetchall()
    list1 = [x[0] for x in list1]
    [crawled_urls.append(i) for i in list1]

if crawl_type == 'list':
    qq = [line.rstrip('\n') for line in open(r'list.txt')]
    for i in qq:
        queue.append(i)

resume()

print(len(queue))

if len(queue) == 1 and crawl_type != 'list':
    finder = load_queue(root_url,crawl_type,root_base)
    [queue.append(i) for i in finder.page_links()]

def get_urls(html):
    new_urls = [url.split('"')[0] for url in str(html).replace("'",'"').split('href="')[1:]]
    filtered = []
    for i in new_urls:
        link_base = "{0.scheme}://{0.netloc}/".format(urlsplit(i))
        link_path = "{0.path}".format(urlsplit(i))
        if link_base.find(root_url) != -1 and crawl_type == 'crawl':
            if i.find(link_path) != -1:
                filtered.append(i)
        elif link_base == ':///' and crawl_type == 'crawl':
            url = i
            url = re.sub(r"^/", "", url)
            url = root_base + url
            if url.find(root_url) != -1:
                filtered.append(url)
        elif crawl_type == 'web':
            filtered.append(i)
        else:
            continue
    return [urljoin(root_base, remove_fragment(new_url)) for new_url in filtered]

async def get_body(url):
    async with ClientSession() as session:
        async with session.get(url, timeout=60) as response:
            response = await response.read()
            #print(response)
            return response

async def handle_task(task_id, work_queue,Project):
    while not work_queue.empty():
        queue_url = await work_queue.get()
        print('Now crawling ' + queue_url + " | there are "+ str(len(queue)) + " links in queue")
        crawled_urls.append(queue_url)
        if len(crawled_urls) < Limit:
            try:
                body = await get_body(queue_url)
                data = bloggers(queue_url, body, Project, cursor, db)
                try:
                    extractors(body, Project, queue_url, selectors)
                except:
                    pass
                if len(search_terms) > 1:
                    (queue_url, body, search_terms, root_url)
                for new_url in get_urls(body):
                    domain = "{0.scheme}://{0.netloc}/".format(urlsplit(new_url))
                    if new_url in crawled_urls:
                        pass
                    elif new_url in queue:
                        pass
                    # elif domain == root_url and crawl_type == 'crawl':
                    elif crawl_type == 'crawl':
                        if robots.is_allowed('*', new_url):
                            queue.append(new_url)
                            q.put_nowait(new_url)
                            now = time.strftime('%Y-%m-%d %H:%M')
                            into_queue(Project, new_url, now, cursor, db)
                    elif domain not in domains and crawl_type == 'web':
                        now = time.strftime('%Y-%m-%d %H:%M')
                        into_queue(Project, new_url, now, cursor, db)
                        queue.append(new_url)
                        q.put_nowait(new_url)
                now = time.strftime('%Y-%m-%d %H:%M')
                domain = "{0.scheme}://{0.netloc}/".format(urlsplit(queue_url))
                if domain not in domains:
                    into_domains(Project, domain, now, cursor, db)
                domains.add(domain)
                remove_queue(Project, queue_url, cursor, db)
                into_crawled(Project, queue_url, cursor, db, now)
            except Exception as e:
                pass
                print(e)
        else:
            print('Crawl has reached the limit fool!')

def remove_fragment(url):
    pure_url, frag = urldefrag(url)
    return pure_url

if __name__ == "__main__":
    q = asyncio.Queue()
    #print('working')
    [q.put_nowait(url) for url in queue]
    loop = asyncio.get_event_loop()
    tasks = [handle_task(task_id, q,Project) for task_id in range(Threads)]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()