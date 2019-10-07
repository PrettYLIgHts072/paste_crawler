import asyncio
import logging
import random
from typing import Optional

import arrow
import pymongo.collation
import pymongo.errors
import requests
import yaml
from lxml import html
from pymongo import MongoClient
from requests.exceptions import HTTPError

settings = {
    'seeds': [
        {
            'seed': 'https://www.pastebin.com',
            'next_page': 'archive',
            'interval': 50,
        },
    ],
    'pages': {
        'archive': {
            'seed': 'https://www.pastebin.com',
            'url': '/archive',
            'features': {
                'pastes_urls': '//tr/td[not(@class)]/a/@href',
            },
            'next_page': 'paste_url',
        },
        'paste_url': {
            'seed': 'https://www.pastebin.com',
            'url': '/*',
            'features': {
                'time': '//div[@class="paste_box_frame"]/descendant::'
                              'div[@class="paste_box_line2"]/span/@title',
                'title': '//div[@class="paste_box_line1"]/@title',
                'author': '//div[@class="paste_box_line2"]/a/text()',
                'content': '//textarea[@class="paste_code"]/text()',
            },
            'next_page': None,
        },
    },
    'mongo_url': 'mongodb',
    # 'mongo_url': 'localhost',
    'mongo_port': 27017,

    'max_url_queue_size': 1000,
    'browser_headers': [
        {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1)'
                       ' AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/39.0.2171.95 Safari/537.36'},
        {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/70.0.3538.77 Safari/537.36'},
        {'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:64.0) '
                       'Gecko/20100101 Firefox/64.0'},
        {'User-Agent': 'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) '
                       'Presto/2.12.388 Version/12.16'},
        {'User-Agent': 'Opera/12.02 (Android 4.1; Linux; '
                       'Opera Mobi/ADR-1111101157; U; en-US) '
                       'Presto/2.9.201 Version/12.02'}
    ],
    'time_parse_str':  'dddd Do [of] MMMM YYYY hh:mm:ss a [CDT]',
    'res_time_format': 'UTC',
    'src_time_zone': 'US/Central',
    'logger_format': '%(asctime)s: %(message)s',
    'logging_datefmt': "%H:%M:%S"
}


def format_time(in_time) -> str:
    in_time = str(in_time)
    try:
        rv = arrow.get(str(in_time), settings['time_parse_str'])
        rv = rv.replace(tzinfo=settings['src_time_zone'])
        rv = rv.to(settings['res_time_format']).ctime()
    except Exception as e:
        logging.error(e)
    return rv


def clean_trailing_spaces(in_str: str):
    return '\n'.join(map(lambda line: line.strip(), in_str.split('\n')))


def clean_results(res: dict) -> dict:
    res['time'] = format_time(res['time'][0])
    try:
        res['title'] = res['title'][0]
        if res['title'] == 'Untitled' or res['title'] == 'Guest' \
                or res['title'] == 'Unknown'\
                or res['title'] == 'Anonymous':
            res['title'] = ""
    except IndexError:
        res['title'] = ""
    try:
        res['author'] = res['author'][0]
        if res['author'] == 'Untitled' or res['author'] == 'Guest'\
                or res['author'] == 'Unknown' \
                or res['author'] == 'Anonymous':
            res['author'] = ""
    except IndexError:
        res['author'] = ""
    res['content'] = clean_trailing_spaces(res['content'][0])
    return res


def fetch_html_content_from_url(url: str) -> Optional[bytes]:
    html_page = requests.get(url)  # TODO: add headers
    html_page.raise_for_status()
    return html_page.content


def parse_page_with_rule(page: bytes, xpath_rule: str) -> list:
    tree = html.fromstring(page)
    results_list = tree.xpath(xpath_rule)
    return results_list


async def random_wait(a: int = 1, b: int = 5, caller=None):
    i = random.randint(a, b)
    if caller:
        logging.info(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)


async def produce(seed: dict, url_queue: asyncio.Queue) -> None:
    while True:
        i = random.randint(132322, 1123123)
        await url_queue.put(settings['pages'][seed['next_page']])
        logging.info(f"Producer {settings['seeds'][0]['seed']}"
                     f" added <{i}> to queue.")
        logging.info("queue length:", url_queue.qsize())
        await asyncio.sleep(seed['interval'])


async def consume(name: int, q: asyncio.Queue,
                  paste_collection: pymongo.collection.Collection) -> None:
    while True:
        try:
            await random_wait(caller=f"Consumer {name}")
            job = await q.get()
            logging.info(f"Consumer {name} got element <{job}>"
                         f" and {job['features']} {job['next_page']}")
            html_page_content = fetch_html_content_from_url(job['seed']
                                                            + job['url'])

            if job['next_page']:
                res = []
                for feature_name, feature_rule in job['features'].items():
                    res = parse_page_with_rule(html_page_content,
                                               feature_rule)
                next_job = settings['pages'][job['next_page']]
                for r in res:
                    next_job['url'] = str(r)
                    await q.put(next_job.copy())
            else:
                # store to mongo db
                res = {}
                for feature_name, feature_rule in job['features'].items():
                    res[feature_name] = \
                        parse_page_with_rule(html_page_content, feature_rule)
                res = clean_results(res)
                # res['url'] = job['seed'] + job['url']
                logging.info("storing job results to db \t\t {}"
                             .format(list(res.items())[:2]))
                try:
                    # post_id = paste_collection.insert_one(res).inserted_id
                    post_id = paste_collection\
                        .update({'url': job['seed'] + job['url']},
                                res,
                                upsert=True)
                    logging.info(f"post successful, post id {post_id}")
                except pymongo.errors.ServerSelectionTimeoutError as e:
                    logging.error(e)
                except Exception as e:
                    logging.error("failed to save post to mongo", e)
            q.task_done()
        except HTTPError as http_err:
            print(f'http error occurred: {http_err}')
        except Exception as e:
            logging.info(f"consumer got problem {e}")


async def main(nprod: int, ncon: int):
    logging.basicConfig(format=settings['logger_format'], level=logging.INFO,
                        datefmt=settings['logging_datefmt'])
    q = asyncio.Queue()

    loop = asyncio.get_event_loop()

    config = yaml.safe_load(open("crawler_config.yml"))
    # config = settings
    mongo_client = MongoClient(config['mongo_url'], config['mongo_port'],
                               # username='user',
                               # password='pass',
                               )
    paste_db = mongo_client['paste_bin']
    paste_collection = paste_db['paste_collection']
    consumers = \
        [loop.create_task(consume(n, q, paste_collection))
         for n in range(ncon)]
    producers = [loop.create_task(produce(seed, q))
                 for seed in settings['seeds']]
    await asyncio.gather(*producers)
    await q.join()  # Implicitly awaits consumers, too
    for c in consumers:
        c.cancel()


if __name__ == "__main__":
    import argparse
    random.seed(444)
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--nprod", type=int, default=1)
    parser.add_argument("-c", "--ncon", type=int, default=4)
    ns = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(**ns.__dict__))
