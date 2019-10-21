import asyncio
import random

import arrow
from pymongo import MongoClient


async def random_wait(a: int = 0, b: int = 3):
    i = random.randint(a, b)
    await asyncio.sleep(i)


class DB:
    def __init__(self, db_conf: dict):
        self.mongo_url = db_conf['mongo_url']
        self.mongo_port = db_conf['mongo_port']
        self.db_name = db_conf['db_name']
        self.mongo_client = MongoClient(self.mongo_url, self.mongo_port)
        self.db_collection = \
            self.mongo_client[self.db_name][db_conf['db_collection']]

    def save(self, result):
        post_id = self.db_collection.update(
            {'url': result['url']},
            result,
            upsert=True)
        return post_id

    def __del__(self):
        self.mongo_client.close()


class TimeFormater:
    def __init__(self, time_parse_str, src_time_zone, res_time_format):
        self.parse_str = time_parse_str
        self.src_time_zone = src_time_zone
        self.res_format = res_time_format

    def reformat(self, in_time):
        in_time = str(in_time)
        t = arrow.get(str(in_time), self.parse_str)
        t = t.replace(tzinfo=self.src_time_zone)
        t = t.to(self.res_format).ctime()
        return t
