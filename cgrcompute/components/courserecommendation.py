from scipy.sparse import lil_matrix
from sklearn.metrics.pairwise import cosine_similarity
from cgrcompute.components.external import DrillClient, get_mongo_service
from typing import Hashable
from logging import getLogger
from cgrcompute.grpc import cgrcompute_pb2 as grpcmsg
from cgrcompute.components.multiprocess import SharableCache
import random

class CosineSimRecommendationModel:

    def __init__(self, ccmtx):
        self.ccmtx = ccmtx

    @staticmethod
    def train(observations: list[set[Hashable]]) -> 'CosineSimRecommendationModel':
        items = list(set(c for o in observations for c in o))
        itemidx = dict((c, i) for (i, c) in enumerate(items))
        itemobsv = lil_matrix((len(items), len(observations)))
        for (i, o) in enumerate(observations):
            for c in o:
                itemobsv[itemidx[c], i] = 1
        sim = cosine_similarity(itemobsv, dense_output=False)
        ccmtx = dict()
        for (i, cid) in enumerate(items):
            neigh = []
            for c in sim.getrow(i).nonzero()[1]:
                neigh.append((items[c], sim[i, c]))
            neigh = sorted(neigh, key=lambda x: x[1])[-300:]
            ccmtx[cid] = dict(neigh)
        return CosineSimRecommendationModel(ccmtx)

    def infer(self, selected_item: list[Hashable]) -> dict[Hashable, float]:
        d = dict()
        for c in selected_item:
            try:
                cscr = self.ccmtx[c]
                for (pcid, scr) in cscr.items():
                    if pcid not in d:
                        d[pcid] = 0
                    d[pcid] += scr
            except KeyError:
                pass
        return dict(sorted(d.items(), key=lambda x: x[1])[-300:])

class CourseRecommendationModel:

    OBSV_QUERY = """
            select t.`a_studyProgram`, t.`a_courseNo`, t.`session_id` from
            (
                select g.`a_studyProgram`, g.`a_courseNo`, g.`session_id`, g.`message`
                from elastic.`graylog_0` g 
                order by g.`timestamp` desc
                limit 100000
                ) t
            where  t.`message` like 'user add course'
            """
    
    def __init__(self):
        self.model = None
        self.logger = getLogger('CourseRecommendationModel')

    def populate(self, drill: DrillClient):
        obsv = self.downloadobsvdata(drill)
        self.model = CosineSimRecommendationModel.train(obsv)

    def infer(self, selected_courses):
        res = self.model.infer(selected_courses)
        res = sorted(res.items(), key=lambda x:-x[1])
        return [course for course, score in res][:300]
    
    def downloadobsvdata(self, drill: DrillClient):
        self.logger.info('Download observation')
        data = drill.query(self.OBSV_QUERY)
        self.logger.info('Received {} observations'.format(len(data.rows)))
        obsv = dict()
        for e in data.rows:
            el = (e['a_studyProgram'], e['a_courseNo'])
            try:
                obsv[e['session_id']].add(el)
            except KeyError:
                obsv[e['session_id']] = set([el])
        obsv = [l for _, l in obsv.items() if len(l) > 4]
        self.logger.info('Retrieved {} qualified observation'.format(len(obsv)))
        return obsv

    def random_infer(self):
        return random.sample(list(self.model.ccmtx.keys()), min(len(self.model.ccmtx), 300))

def get_course_recommendation_model():
    model = CourseRecommendationModel()
    model.populate(DrillClient())
    return model

def recommend_course(req: grpcmsg.CourseRecommendationRequest, cache: SharableCache) -> grpcmsg.CourseRecommendationResponse:
    logger = getLogger('recommend_course')
    logger.info('Processing recommendation for {}'.format(req))
    model: CourseRecommendationModel = cache.get_or_create('recommend_course_model', get_course_recommendation_model)
    res = []
    if req.variant == 'RANDOM':
        res = model.random_infer()
    elif req.variant == 'COSINE':
        res = model.infer([(e.semesterKey.studyProgram, e.courseNo) for e in req.selectedCourse])
    else:
        raise Exception('{} variant is invalid'.format(req.variant))
    enriched_res = []
    mongo = get_mongo_service()
    for study_program, course_no in res:
        if len(enriched_res) > 10:
            break
        if course_no in [e.courseNo for e in req.selectedCourse]:
            continue

        abbr = mongo.get_course_abbr(course_no=course_no, study_program=req.semesterKey.studyProgram, semester=req.semesterKey.semester, academic_year=req.semesterKey.academicYear)
        if abbr:
            d = grpcmsg.CourseRecommendationResponse.CourseDetail()
            d.key.courseNo = course_no
            d.key.semesterKey.CopyFrom(req.semesterKey)
            d.courseNameEn = abbr
            enriched_res.append(d)
    resp = grpcmsg.CourseRecommendationResponse()
    resp.course.extend(enriched_res)
    return resp

def recommend_course_serialized(req: bytes, cache: SharableCache) -> bytes:
    r = grpcmsg.CourseRecommendationRequest()
    r.ParseFromString(req)
    res = recommend_course(r, cache)
    return res.SerializeToString()
