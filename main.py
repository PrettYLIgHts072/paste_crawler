import asyncio
import logging
import random
from typing import Optional, List

import arrow
import pymongo.collation
import pymongo.errors
import requests
import yaml
from lxml import html
from pymongo import MongoClient
from requests.exceptions import HTTPError


def clean_trailing_spaces(in_str: str):
    return '\n'.join(map(lambda line: line.strip(), in_str.split('\n')))


def extract_features(page: bytes, xpath_rule: str) -> list:
    tree = html.fromstring(page)
    results_list = tree.xpath(xpath_rule)
    return results_list


def clean_untitled_unknown(text: [str, List[str]]) -> str:
    try:
        text = text[0]
    except IndexError:
        return ""
    if text == 'Untitled' or text == 'Guest' or \
            text == 'Unknown' or text == 'Anonymous':
        return ""
    return text


async def random_wait(a: int = 0, b: int = 3, caller=None):
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

        logging.basicConfig(format=self.config['logger_format'],
                            level=logging.DEBUG,
                            # level=logging.INFO,
                            datefmt=self.config['logging_datefmt'])
        self.time_reformatter = TimeReformat(self.config['time_parse_str'],
                                             self.config['src_time_zone'],
                                             self.config['res_time_format'])
        self.to_run = True
        self.loop = asyncio.get_event_loop()
        self.url_queue = asyncio.Queue()
        self.db_collection = self.get_db_collection(self.config['db'])
        self.headers = self.config['browser_headers']
        self.schedulers = \
            self.create_loop_event_tasks(self.produce, self.config['seeds'])
        self.downloaders = \
            self.create_loop_event_tasks(self.consume,
                                         range(self.config['num_connections']))

    def clean_results(self, res: dict) -> dict:
        return {'time': self.time_reformatter.reformat(res['time'][0]),
                'title': clean_untitled_unknown(res['title']),
                'author': clean_untitled_unknown(res['author']),
                'content': clean_trailing_spaces(res['content'][0])}

    def create_loop_event_tasks(self, func, tasks):
        rv = [self.loop.create_task(func(t)) for t in tasks]
        return rv

    async def fetch_page(self, url: str, retry: int = 2) -> Optional[bytes]:
        try:
            response = requests.get(url, headers=random.choice(self.headers))
            response.raise_for_status()
            return response.content
        except HTTPError as http_err:
            print(f'http error occurred: {http_err} while getting: {url}')
            if retry > 0:
                await asyncio.sleep(120)
                await self.fetch_page(url, retry - 1)

    @staticmethod
    def get_db_collection(conf):
        mongo_client = MongoClient(conf['mongo_url'], conf['mongo_port'])
        db = mongo_client[conf['db_name']]
        return db['paste_collection']

    def get_next_job(self, job):
        return self.config['pages'][job['next_page']]

    async def consume(self, name: int) -> None:
        while True:
            try:
                await random_wait(caller=f"Consumer {name}")
                job = await self.url_queue.get()
                logging.debug(f"Consumer {name} got element <{job}>"
                              f" and {job['features']} {job['next_page']}")
                job_url = job['seed'] + job['url']
                content = await self.fetch_page(job_url)

                if job['next_page']:
                    res = []
                    for feature, rule in job['features'].items():
                        res = extract_features(content, rule)
                    logging.debug(f"push links to queue {res[:5]}")
                    next_job = self.get_next_job(job)
                    for r in res:
                        next_job['url'] = str(r)
                        await self.url_queue.put(next_job.copy())
                else:
                    res = {}
                    for feature, rule in job['features'].items():
                        res[feature] = extract_features(content, rule)
                    res = self.clean_results(res)
                    logging.debug("storing job results to db {}"
                                  .format([res['author'],
                                           res['title'],
                                           res['content'][:30]]))
                    res['url'] = job_url
                    self.save_result(job, res)
                self.url_queue.task_done()
            except HTTPError as http_err:
                logging.error(f'http error occurred: {http_err}')
            except Exception as e:
                logging.error(f"consumer got problem {e}")

    def save_result(self, job, result):
        try:
            post_id = self.db_collection.update(
                {'url': job['seed'] + job['url']},
                result,
                upsert=True)
            logging.debug(f"post successful, post id {post_id}")
        except pymongo.errors.ServerSelectionTimeoutError as e:
            logging.error("error saving results: {}".format(e))
        except Exception as e:
            logging.error("failed to save post to mongo", e)

    def save_result_to_db(self, result):
        pass

    async def produce(self, seed: dict) -> None:
        while self.to_run:
            await self.url_queue.put(self.config['pages'][seed['next_page']])
            logging.info(f"Producer added "
                         f"{{self.config['seeds'][0]['seed']}} to queue.")
            await asyncio.sleep(seed['interval'])

    async def crawl(self):
        await asyncio.gather(*self.schedulers)
        await self.url_queue.join()  # Implicitly awaits consumers, too
        # for c in self.downloaders:
        #     c.cancel()

    def start(self):
        try:
            self.loop.run_until_complete(self.crawl())
        except KeyboardInterrupt:
            logging.info("exiting on keyboard interrupt")
            self.stop()

    def stop(self):
        self.to_run = False


class TimeReformat:
    def __init__(self, parse_str, src_time_zone, res_format):
        self.parse_str = parse_str
        self.src_time_zone = src_time_zone
        self.res_format = res_format

    def reformat(self, in_time):
        in_time = str(in_time)
        try:
            t = arrow.get(str(in_time), self.parse_str)
            t = t.replace(tzinfo=self.src_time_zone)
            t = t.to(self.res_format).ctime()
            return t
        except Exception as e:
            logging.error("error formatting time {}".format(e))


if __name__ == "__main__":
    paste_bin_crawler = Crawler()
    paste_bin_crawler.start()
