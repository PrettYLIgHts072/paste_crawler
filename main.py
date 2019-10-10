import logging
from typing import Optional, List

import pymongo.collation
import pymongo.errors
import requests
import yaml
from lxml import html
from requests.exceptions import HTTPError

from utils import *


class Crawler:
    def __init__(self, settings_path: str = "crawler_config.yml"):
        try:
            self.config = yaml.safe_load(open(settings_path))
        except FileNotFoundError:
            logging.error(f"settings file cant be fount at: {settings_path}")
            print(f"settings file cant be fount at: {settings_path}")

        logging.basicConfig(format=self.config['logger_format'],
                            # level=logging.DEBUG,
                            level=logging.INFO,
                            datefmt=self.config['logging_datefmt'])
        self.t_format = TimeFormat(self.config['time_parse_str'],
                                   self.config['src_time_zone'],
                                   self.config['res_time_format'])
        self.to_run = True
        self.loop = asyncio.get_event_loop()
        self.url_queue = asyncio.Queue()
        self.db = DB(self.config['db'])
        self.headers = self.config['browser_headers']
        self.schedulers = \
            self.create_loop_event_tasks(self.scheduler, self.config['seeds'])
        self.downloaders = \
            self.create_loop_event_tasks(self.downloader,
                                         range(self.config['num_connections']))

    def create_loop_event_tasks(self, func, tasks):
        return [self.loop.create_task(func(t)) for t in tasks]

    async def crawl(self) -> None:
        await asyncio.gather(*self.schedulers)
        await self.url_queue.join()

    def start(self) -> None:
        try:
            self.loop.run_until_complete(self.crawl())
        except KeyboardInterrupt:
            logging.info("exiting on keyboard interrupt")
            self.stop()

    def stop(self):
        self.to_run = False

    async def scheduler(self, seed: dict) -> None:
        while self.to_run:
            await self.url_queue.put(self.config['pages'][seed['next_page']])
            logging.info(f"Producer added "
                         f"{{self.config['seeds'][0]['seed']}} to queue.")
            await asyncio.sleep(seed['interval'])

    async def downloader(self, _id: int) -> None:
        while True:
            try:
                await random_wait()
                job = await self.get_a_job()
                logging.info(f"Downloader {_id} got new job, features: "
                             f" {job['features']}")
                job_url = job['seed'] + job['url']
                content = await self.page_content(job_url)

                if job['next_page']:
                    await self.add_paste_urls_to_crawl(job, content)
                else:
                    paste = await self.get_paste_content(job, content)
                    paste = self.format_content(paste)
                    paste['url'] = job_url
                    self.save_result(paste)
                self.mark_job_done()
            except HTTPError as http_err:
                logging.error(f'http error occurred: {http_err}')
            except Exception as e:
                logging.error(f"consumer got problem {e}")

    def format_content(self, res: dict) -> dict:
        return {'time': self.t_format.reformat(res['time'][0]),
                'title': self.clean_untitled_unknown(res['title']),
                'author': self.clean_untitled_unknown(res['author']),
                'content': self.clean_trailing_spaces(res['content'][0])}

    async def page_content(self, url: str, retry: int = 2) -> Optional[bytes]:
        try:
            response = requests.get(url, headers=random.choice(self.headers))
            response.raise_for_status()
            return response.content
        except HTTPError as err:
            logging.error(f'http error occurred: {err} while getting: {url}')
            if retry > 0:
                await asyncio.sleep(120)
                await self.page_content(url, retry - 1)
            else:
                raise Exception('Didn\'t get {} page'.format(url))

    def get_next_job(self, job):
        return self.config['pages'][job['next_page']]

    async def add_paste_urls_to_crawl(self, job, content):
        paste_urls = [self.extract_features(content, rule)
                      for rule in job['features'].values()][0]
        logging.debug(f"push links to queue {paste_urls[:5]}")
        next_job = self.get_next_job(job)
        for r in paste_urls:
            next_job['url'] = str(r)
            await self.url_queue.put(next_job.copy())

    async def get_paste_content(self, job, content):
        return {feature: self.extract_features(content, rule)
                for (feature, rule) in job['features'].items()}

    @staticmethod
    def extract_features(page: bytes, xpath_rule: str) -> list:
        tree = html.fromstring(page)
        results_list = tree.xpath(xpath_rule)
        return results_list

    @staticmethod
    def clean_trailing_spaces(in_str: str):
        return '\n'.join(map(lambda line: line.strip(), in_str.split('\n')))

    @staticmethod
    def clean_untitled_unknown(text: [str, List[str]]) -> str:
        try:
            text = text[0]
        except IndexError:
            return ""
        if text == 'Untitled' or text == 'Guest' or \
                text == 'Unknown' or text == 'Anonymous':
            return ""
        return text

    async def get_a_job(self):
        return await self.url_queue.get()

    def mark_job_done(self):
        self.url_queue.task_done()

    def save_result(self, result):
        try:
            post_id = self.db.save(result)
            logging.debug(f"Successfully saved, post id {post_id['upserted']}")
        except pymongo.errors.ServerSelectionTimeoutError as e:
            logging.error("error saving results: {}".format(e))
        except Exception as e:
            logging.error("failed to save post to mongo", e)


if __name__ == "__main__":
    paste_bin_crawler = Crawler()
    paste_bin_crawler.start()
