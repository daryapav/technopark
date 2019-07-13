import asyncio
import aiohttp
import html5lib
from bs4 import BeautifulSoup
import async_timeout
from urllib.parse import urlparse
from aioelasticsearch import Elasticsearch


class Parser:
    def __init__(self):
        self.domain = 'docs.python.org/'
        self.host = 'https://' + self.domain
        self.forbidden_prefix = ['_', '#']


    async def get_links(self, q, client, _es):
        while True:
            url = await q.get()
            async with client.get(url) as resp:
                links = set()
                html = await resp.read()
                soup = BeautifulSoup(html, 'html.parser')

                # for_index = {'link':url, 'text': soup.get_text()}
                # await _es.index(index="index", doc_type='tweet', body=for_index)

                all_link = soup.find_all('link') + soup.find_all('a')
                for link in all_link:
                    link = link.get('href')
                    if all(not link.startswith(prefix) for prefix in self.forbidden_prefix):

                        if not link.startswith('http'):
                            link = self.host + link
                            links.add(link)
                            await q.put(link)

                        elif link.startswith('https://docs.python.org/') and link not in links:
                            links.add(link)
                            await q.put(link)
                        else:
                            continue


    async def main(self, number):
        tasks = []
        q = asyncio.Queue()
        q.put_nowait(self.host)
        async with aiohttp.ClientSession() as client:
            for i in range(number):
                _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
                task = asyncio.create_task(self.get_links(q, client, _es))
                tasks.append(task)
            await asyncio.gather(*tasks)




p = Parser()

if __name__=='__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(p.main(5))
    # asyncio.run(p.main(5))

