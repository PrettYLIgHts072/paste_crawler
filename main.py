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


def format_time(in_time, parse_str, src_time, dst_time) -> str:
    in_time = str(in_time)
    try:
        rv = arrow.get(str(in_time), parse_str)
        rv = rv.replace(tzinfo=src_time)
        rv = rv.to(dst_time).ctime()
        return rv
    except Exception as e:
        logging.error(e)


def clean_trailing_spaces(in_str: str):
    return '\n'.join(map(lambda line: line.strip(), in_str.split('\n')))


def clean_results(res: dict, settings) -> dict:
    res['time'] = format_time(res['time'][0], settings['time_parse_str'],
                              settings['src_time_zone'], settings['res_time_format'])
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


async def fetch_html_content_from_url(url: str, retry: int = 2) -> Optional[bytes]:
    try:
        html_page = requests.get(url)  # TODO: add headers
        html_page.raise_for_status()
        return html_page.content
    except HTTPError as http_err:
        print(f'http error occurred: {http_err} while getting: {url}')
        if retry > 0:
            await asyncio.sleep(120)
            await fetch_html_content_from_url(url, retry - 1)


def extract_features(page: bytes, xpath_rule: str) -> list:
    tree = html.fromstring(page)
    results_list = tree.xpath(xpath_rule)
    return results_list


async def random_wait(a: int = 1, b: int = 5, caller=None):
    i = random.randint(a, b)
    if caller:
        logging.info(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)


class Job:
    def __init__(self, url: str, next_url, seed):
        self.url = url
        self.next_url = next_url
        self.seed = seed


class Crawler:
    def __init__(self, settings_path: str = "crawler_config.yml"):
        try:
            self.config = yaml.safe_load(open(settings_path))
        except FileNotFoundError:
            logging.error(f"settings file cant be fount at: {settings_path}")
            print(f"settings file cant be fount at: {settings_path}")

        self.to_run = True

        logging.basicConfig(format=self.config['logger_format'],
                            level=logging.DEBUG,
                            # level=logging.INFO,
                            datefmt=self.config['logging_datefmt'])
        self.loop = asyncio.get_event_loop()
        self.url_queue = asyncio.Queue()

        self.db_collection = self.get_db_collection(self.config['db'])
        self.producers = \
            self.create_loop_event_tasks(self.produce, self.config['seeds'])
        self.producers = \
            self.create_loop_event_tasks(self.consume,
                                         range(self.config['num_connections']))

    def create_loop_event_tasks(self, func, tasks):
        rv = [self.loop.create_task(func(t, self.url_queue)) for t in tasks]
        return rv

    @staticmethod
    def get_db_collection(conf):
        mongo_client = MongoClient(conf['mongo_url'], conf['mongo_port'])
        db = mongo_client[conf['db_name']]
        return db['paste_collection']

    async def consume(self, name: int, q: asyncio.Queue) -> None:
        while True:
            try:
                await random_wait(caller=f"Consumer {name}")
                job = await q.get()
                logging.info(f"Consumer {name} got element <{job}>"
                             f" and {job['features']} {job['next_page']}")
                content = await fetch_html_content_from_url(job['seed'] + job['url'])

                if job['next_page']:
                    res = []
                    for feature, rule in job['features'].items():
                        res = extract_features(content, rule)
                    next_job = self.config['pages'][job['next_page']]
                    for r in res:
                        next_job['url'] = str(r)
                        await q.put(next_job.copy())
                else:
                    res = {}
                    for feature, rule in job['features'].items():
                        res[feature] = extract_features(content, rule)
                    res = clean_results(res, self.config)
                    logging.debug("storing job results to db \t\t {}"
                                  .format(list(res.items())[:2]))
                    self.save_result(job, res)
                q.task_done()
            except HTTPError as http_err:
                print(f'http error occurred: {http_err}')
            except Exception as e:
                logging.info(f"consumer got problem {e}")

    def save_result(self, job, result):
        try:
            post_id = self.db_collection.update(
                {'url': job['seed'] + job['url']},
                result,
                upsert=True)
            logging.debug(f"post successful, post id {post_id}")
        except pymongo.errors.ServerSelectionTimeoutError as e:
            logging.error(e)
        except Exception as e:
            logging.error("failed to save post to mongo", e)

    def save_result_to_db(self, result):
        pass

    async def produce(self, seed: dict, url_queue: asyncio.Queue) -> None:
        while self.to_run:
            i = random.randint(132322, 1123123)
            await url_queue.put(self.config['pages'][seed['next_page']])
            logging.info(f"Producer {self.config['seeds'][0]['seed']}"
                         f" added <{i}> to queue.")
            await asyncio.sleep(seed['interval'])

    async def crawl(self):
        await asyncio.gather(*self.producers)
        await self.url_queue.join()  # Implicitly awaits consumers, too
        for c in self.consumers:
            c.cancel()

    def start(self):
        try:
            self.loop.run_until_complete(self.crawl())
        except KeyboardInterrupt:
            logging.info("exiting on keyboard interrupt")
            self.stop()

    def stop(self):
        self.to_run = False


if __name__ == "__main__":
    paste_bin_crawler = Crawler()
    paste_bin_crawler.start()
