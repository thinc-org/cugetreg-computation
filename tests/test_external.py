import unittest
from cgrcompute.components.external import DrillClient, DrillQueryException, MongoService
from unittest.mock import patch, MagicMock
from pymongo import MongoClient


@unittest.skip('Skipped, Requires external connection')
class DrillClientTest(unittest.TestCase):

    def setUp(self):
        self.client = DrillClient()

    def test_check_status(self):
        self.client.checkstatus()

    def test_query(self):
        res = self.client.query("SELECT 'test' as msg")
        self.assertEqual(res.rows[0]['msg'], 'test')

    def test_error_query(self):
        self.assertRaises(DrillQueryException, lambda : self.client.query("syntax error query"))


class MongoServiceTest(unittest.TestCase):
    
    db = {
        'courses': MagicMock()
    }

    def setUp(self):
        with patch('cgrcompute.components.external.MongoClient') as mockc, patch('cgrcompute.components.external.get_config') as cfg:
            cfg.return_value = {
                        'mongo': {
                            'url': 'mongo://a',
                            'database': 'db'
                            }
                    }
            mockc.return_value = { 'db': self.db }
            self.srv = MongoService()
            mockc.assert_called_with('mongo://a')

    def test_use_correct_db(self):
        self.assertIs(self.db, self.srv.db)

    def test_get_course_abbr(self):
        self.db['courses'].find_one.return_value = {
                'abbrName': 'THAI WRIT WORK'
        }
        self.assertEqual('THAI WRIT WORK', self.srv.get_course_abbr(course_no = '0123105', semester = '1', study_program = 'S', academic_year='2564'))
        self.db['courses'].find_one.assert_called_with({
                'courseNo': '0123105',
                'semester': '1',
                'studyProgram': 'S',
                'academicYear': '2564'
            })

if __name__ == '__main__':
    unittest.main()
