import requests
from cgrcompute.components.config import parse_config
from logging import getLogger
import json
from cgrcompute.components.config import get_config
import typing
from pymongo import MongoClient
from opensearchpy import OpenSearch


class ElasticService:

    def __init__(self):
        cfg = get_config()['elastic']
        self.client = OpenSearch(
            hosts=[{'host': cfg['host'], 'port': cfg['port']}],
            http_compress=True,
            http_auth=(cfg['username'], cfg['password']),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
        )

    def find_all_user_add_course(self):
        query1 = {
            'size': 1000,
            'sort': {
                'timestamp': {
                    "order": "desc"
                }
            },
            "query": {
                "match_phrase": {
                    "short_message": "user add course"
                }
            }
        }
        query2 = {
            'size': 1000,
            'sort': {
                'timestamp': {
                    "order": "desc"
                }
            },
            "query": {
                "match_phrase": {
                    "message": "user add course"
                }
            }
        }
        for e in self.find_scrolling(query1, "cgr-clientlogging"):
            s = e['_source']
            yield {'study_program': s['a_studyProgram'], 'course_id': s['a_courseNo'], 'device_id': s['device_id'], 'raw': e}
        for e in self.find_scrolling(query2, "cgr-legacy"):
            s = e['_source']
            yield {'study_program': s['a_studyProgram'], 'course_id': s['a_courseNo'], 'device_id': s['device_id'], 'raw': e}

    def find_scrolling(self, query, index):
        res = self.client.search(index=index, body=query, params={'scroll': '10m'})
        scroll_id = res['_scroll_id']
        while len(res['hits']['hits']) > 0:
            for hit in res['hits']['hits']:
                yield hit
            res = self.client.scroll(scroll_id=scroll_id, scroll='10m')
        self.client.clear_scroll(scroll_id=scroll_id)


class MongoService:

    def __init__(self):
        cfg = get_config()['mongo']
        url = cfg['url']
        dbname = cfg['database']
        self.db = MongoClient(url)[dbname]

    def get_course_abbr(self, course_no, semester, study_program, academic_year):
        c = self.db['courses'].find_one({
                'courseNo': course_no,
                'semester': semester,
                'studyProgram': study_program,
                'academicYear': academic_year
            })
        if c:
            return c['abbrName']
        else:
            return None

def get_mongo_service():
    global mongosrv
    try:
        return mongosrv
    except NameError:
        mongosrv = MongoService()
        return mongosrv
