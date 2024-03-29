import unittest
from cgrcompute.components.courserecommendation import *
from math import sqrt
from unittest.mock import MagicMock, patch
import cgrcompute.grpc.cgrcompute_pb2 as grpcmsg

class CosineSimRecommendationModelTest(unittest.TestCase):

    def test_infer_should_sum_vec(self):
        ccvec = {
            'courseA': {'courseP': 0.1,  'courseQ': 0.2, 'courseR': 0.3},
            'courseB': {'courseP': 0.7, 'courseQ': 0.1}
        }
        model = CosineSimRecommendationModel(ccvec)

        res = model.infer(['courseA'])
        self.assertAlmostEqual(res['courseP'], 0.1)
        self.assertAlmostEqual(res['courseQ'], 0.2)
        self.assertAlmostEqual(res['courseR'], 0.3)

        res = model.infer(['courseA', 'courseB'])
        self.assertAlmostEqual(res['courseP'], 0.8)
        self.assertAlmostEqual(res['courseQ'], 0.3)
        self.assertAlmostEqual(res['courseR'], 0.3)

    def test_infer_should_keep_max_20(self):
        ccvec = {
            'courseA': dict(('course' + str(i), i) for i in range(50))
        }
        model = CosineSimRecommendationModel(ccvec)
        res = model.infer(['courseA'])
        for i in range(40, 50):
            self.assertIn(('course' + str(i), i), res.items())

    def test_infer_shouldIgnoreUnk(self):
        model = CosineSimRecommendationModel({})
        res = model.infer(['something'])
        self.assertEqual(res, {})

    def test_train_shouldCalCosine(self):
        model = CosineSimRecommendationModel.train([{'a', 'b'},  {'a', 'b'}, {'a', 'c'}])
        self.assertAlmostEqual(model.ccmtx['a']['a'], 1)
        self.assertAlmostEqual(model.ccmtx['a']['b'], 2/sqrt(6))
        self.assertAlmostEqual(model.ccmtx['a']['c'], 1 / sqrt(3))
        self.assertAlmostEqual(model.ccmtx['b']['a'], 2/sqrt(6))
        self.assertAlmostEqual(model.ccmtx['b']['b'], 1)
        self.assertAlmostEqual(model.ccmtx['c']['a'], 1 / sqrt(3))
        self.assertAlmostEqual(model.ccmtx['c']['c'], 1)

class CourseRecommendationModelTest(unittest.TestCase):

    def with_mocked_internalmodel(self, infer_result):
        model = CourseRecommendationModel()
        model.model = CosineSimRecommendationModel({})
        model.model.infer = MagicMock(return_value=infer_result)
        return model

    def test_downloadobsvdata(self):

        def mock_es() -> ElasticService:
            mock = MagicMock()
            mock.find_all_user_add_course.return_value = [
                {'study_program': 'S', 'course_id': course_no, 'device_id': dev_id}
                for course_no in ['21101', '21102', '21103', '21104', '21105']
                for dev_id in ['1', '2', '3']
            ]
            return mock
        model = CourseRecommendationModel()
        res = model.downloadobsvdata(mock_es())
        self.assertEqual(3, len(res))
        expectedSet = set(('S', course_no) for course_no in ['21101', '21102', '21103', '21104', '21105'])
        for s in res:
            self.assertSetEqual(expectedSet, s)

    def test_infer_sorted(self):
        model = self.with_mocked_internalmodel({
                    ('S', 'test'): 0.1,
                    ('S', 'test2'): 0.2
                })
        res = model.infer([('S', 'ok')])
        model.model.infer.assert_called_with([('S', 'ok')])
        self.assertListEqual([('S', 'test2'), ('S', 'test')], res)

    def test_random(self):
        model = CourseRecommendationModel()
        model.model = CosineSimRecommendationModel({ 'test': [] })
        self.assertListEqual(['test'], model.random_infer())

class RecommendCourseTest(unittest.TestCase):

    def setUp(self):
        self.patch_mongo = patch('cgrcompute.components.courserecommendation.get_mongo_service')
        self.mongo = self.patch_mongo.start()
        self.mongo.return_value.get_course_abbr.return_value = 'HELLO'
        self.rec = MagicMock()
        self.rec.infer.return_value = [('S', '1g'), ('S', '2g')]
        self.rec.random_infer.return_value = [('A', '1k'), ('A', '2k')]
        self.cache = MagicMock()
        self.cache.get_or_create.return_value = self.rec

    def tearDown(self):
        self.patch_mongo.stop()

    def test_random(self):
        req = grpcmsg.CourseRecommendationRequest()
        req.variant = 'RANDOM'
        req.semesterKey.studyProgram = 'T'
        req.semesterKey.semester = '0'
        res = recommend_course(req, self.cache)
        self.assertEqual(res.courses[0].courseNameEn, 'HELLO')
        self.assertEqual(res.courses[0].key.courseNo, '1k')

    def test_infer(self):
        req = grpcmsg.CourseRecommendationRequest()
        req.variant = 'COSINE'
        req.semesterKey.studyProgram = 'T'
        req.semesterKey.semester = '0'
        res = recommend_course(req, self.cache)
        self.assertEqual(res.courses[0].key.courseNo, '1g')

    def test_no_already_selected(self):
        req = grpcmsg.CourseRecommendationRequest()
        req.variant = 'COSINE'
        req.semesterKey.studyProgram = 'T'
        req.semesterKey.semester = '0'
        req.semesterKey.academicYear = 'Y'
        c = req.selectedCourses.add()
        c.courseNo = '1g'
        c.semesterKey.CopyFrom(req.semesterKey)
        res = recommend_course(req, self.cache)
        self.assertEqual(res.courses[0].key.courseNo, '2g')

    def test_infer_fitered(self):
        req = grpcmsg.CourseRecommendationRequest()
        req.variant = 'COSINE'
        req.semesterKey.studyProgram = 'T'
        req.semesterKey.semester = '0'
        self.mongo.return_value.get_course_abbr.side_effect = [None, 'TEST']
        res = recommend_course(req, self.cache)
        self.assertEqual(res.courses[0].courseNameEn, 'TEST')
        self.assertEqual(res.courses[0].key.courseNo, '2g')
        self.assertEqual(len(res.courses), 1)

    def test_serialize(self):
        with patch('cgrcompute.components.courserecommendation.recommend_course') as p:
            p.return_value = grpcmsg.CourseRecommendationResponse()
            c = p.return_value.courses.add()
            c.courseNameEn = 'hello'
            req = grpcmsg.CourseRecommendationRequest()
            eres = recommend_course_serialized(req.SerializeToString(), None)
            res = grpcmsg.CourseRecommendationResponse()
            res.ParseFromString(eres)
            self.assertEqual('hello', res.courses[0].courseNameEn)



if __name__ == '__main__':
    unittest.main()
