import requests
from cgrcompute.components.config import parse_config
from logging import getLogger
import json
from cgrcompute.components.config import get_config
import typing
from pymongo import MongoClient

AUTH_COOKIE_NAME = 'authelia_session'

class DrillQueryResult:
    
    columns: list[str]
    rows: list[dict[str, typing.Any]]

    def __init__(self, result):
        self.columns = result['columns']
        self.rows = result['rows']

    def __repr__(self):
        return json.dumps({
                'columns': self.columns,
                'rows': self.rows
            })

class DrillQueryException(Exception):

    def __init__(self, msg):
        super().__init__(self, msg)


class DrillClient:
    
    def __init__(self):
        self.logger = getLogger('DrillClient')
        cfg = get_config()['drill']
        self.url = cfg['url']
        self.cookies = dict()
        if 'auth_proxy' in cfg:
            proxy = cfg['auth_proxy']
            username = cfg['auth_username']
            password = cfg['auth_password']
            self.logger.info("auth_proxy detected, trying to authenticate")
            resp = requests.post(proxy + '/api/firstfactor', json={
                    'username': username,
                    'password': password
                })
            resp.raise_for_status()
            cookie = resp.cookies[AUTH_COOKIE_NAME]
            self.logger.info('auth_proxy authenticated')
            self.cookies = resp.cookies
        self.checkstatus()
        self.logger.info('drill connected')

    
    def checkstatus(self):
        r = requests.get(self.url + '/profiles.json', cookies=self.cookies, allow_redirects=False)
        if r.status_code != 200:
            raise Exception('Drill status return is not 200')
        r.raise_for_status()

    def query(self, query) -> DrillQueryResult :
        req = {
            'queryType': 'SQL',
            'query': query
        }
        resp = requests.post(self.url + '/query.json', cookies=self.cookies, json=req)
        resp.raise_for_status()
        if resp.json()['queryState'] != 'COMPLETED':
            raise DrillQueryException('Query is not COMPLETED. query:' + query + ' query status is ' + resp.json()['queryState'])
        return DrillQueryResult(resp.json())


class MongoService:

    def __init__(self):
        cfg = get_config()['mongo']
        url = cfg['url']
        dbname = cfg['database']
        self.db = MongoClient(url)[dbname]

    def get_course_abbr(self, course_no, semester, study_program):
        c = self.db['courses'].find_one({
                'courseNo': course_no,
                'semester': semester,
                'studyProgram': study_program
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
