from scipy.sparse import lil_matrix
from sklearn.metrics.pairwise import cosine_similarity,linear_kernel
from sklearn.preprocessing import normalize
from cgrcompute.components.external import ElasticService, get_mongo_service
from typing import Hashable
from logging import getLogger
from cgrcompute.grpc import cgrcompute_pb2 as grpcmsg
from cgrcompute.components.multiprocess import SharableCache
import random
import time

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
    @staticmethod
    def train32(observations: list[set[Hashable]]) -> 'CosineSimRecommendationModel':
        items = list(set(c for o in observations for c in o))
        itemidx = dict((c, i) for (i, c) in enumerate(items))
        itemobsv = lil_matrix((len(items), len(observations)),dtype='float32')
        for (i, o) in enumerate(observations):
            for c in o:
                itemobsv[itemidx[c], i] = 1
        sim = cosine_similarity(itemobsv.astype('float32'), dense_output=False)
        ccmtx = dict()
        for (i, cid) in enumerate(items):
            neigh = []
            for c in sim.getrow(i).nonzero()[1]:
                neigh.append((items[c], sim[i, c]))
            neigh = sorted(neigh, key=lambda x: x[1])[-300:]
            ccmtx[cid] = dict(neigh)
        return CosineSimRecommendationModel(ccmtx)
    @staticmethod
    def train16(observations: list[set[Hashable]]) -> 'CosineSimRecommendationModel':
        items = list(set(c for o in observations for c in o))
        itemidx = dict((c, i) for (i, c) in enumerate(items))
        itemobsv = lil_matrix((len(items), len(observations)),dtype='float32')
        for (i, o) in enumerate(observations):
            for c in o:
                itemobsv[itemidx[c], i] = 1
        size = itemobsv.shape[0]
        chunk_size = 500
        ccmtx = dict()
        for start in range(0,size,chunk_size):
            end = min(start+chunk_size,size)
            sim = cosine_similarity(itemobsv[start:end],itemobsv,dense_output=False).astype('float16')
            for (i, cid) in enumerate(items[start:end]):
                neigh = []
                for c in sim.getrow(i).nonzero()[1]:
                    neigh.append((items[c], sim[i, c]))
                neigh = sorted(neigh, key=lambda x: x[1])[-300:]
                ccmtx[cid] = dict(neigh)
        return CosineSimRecommendationModel(ccmtx)
    @staticmethod
    def train8(observations: list[set[Hashable]]) -> 'CosineSimRecommendationModel':
        items = list(set(c for o in observations for c in o))
        itemidx = dict((c, i) for (i, c) in enumerate(items))
        itemobsv = lil_matrix((len(items), len(observations)),dtype='uint8')
        for (i, o) in enumerate(observations):
            for c in o:
                itemobsv[itemidx[c], i] = 1
        size = itemobsv.shape[0]
        chunk_size = 500
        cnorm = normalize(itemobsv)
        cnorm *=16
        ccmtx = dict()
        for start in range(0,size,chunk_size):
            end = min(start+chunk_size,size)
            sim = linear_kernel(cnorm[start:end],cnorm,dense_output=False).astype('uint8')
            for (i, cid) in enumerate(items[start:end]):
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
    
    def __init__(self):
        self.model = None
        self.model32 = None
        self.model16 = None
        self.model8 = None
        self.logger = getLogger('CourseRecommendationModel')

    def populate(self):
        self.logger.info("Started download {}".format(time.time()))
        obsv = self.downloadobsvdata()
        self.logger.info("Download completed {}. Start training".format(time.time()))
        self.model = CosineSimRecommendationModel.train(obsv)
        self.model32 = CosineSimRecommendationModel.train32(obsv)
        self.model16 = CosineSimRecommendationModel.train16(obsv)
        self.model8 = CosineSimRecommendationModel.train8(obsv)
        self.logger.info("Training completed {}".format(time.time()))

    def infer(self, selected_courses,variant):
        if variant =="COSINE":
            res = self.model.infer(selected_courses)
        elif variant =="COSINE32":
            res = self.model32.infer(selected_courses)
        elif variant =="COSINE16":
            res = self.model16.infer(selected_courses)
        elif variant =="COSINE8":
            res = self.model8.infer(selected_courses)
        res = sorted(res.items(), key=lambda x:-x[1])
        return [course for course, score in res][:300]
    
    def downloadobsvdata(self):
        self.logger.info('Download observation')
        es = ElasticService()
        cnt = 0
        obsv = dict()
        for e in es.find_all_user_add_course():
            course_key = (e['study_program'], e['course_id'])
            grouping = e['device_id']
            try:
                obsv[grouping].add(course_key)
            except KeyError:
                obsv[grouping] = set([course_key]) 

            cnt += 1
            if cnt % 10000 == 0:
                self.logger.info("Downloaded {} observations".format(cnt))
            if cnt >= 900000:
                 break
        self.logger.info('Received {} observations'.format(cnt))
        obsv = [l for _, l in obsv.items() if len(l) > 4]
        self.logger.info('Retrieved {} qualified observation'.format(len(obsv)))
        return obsv

    def random_infer(self):
        return random.sample(list(self.model.ccmtx.keys()), min(len(self.model.ccmtx), 300))

def get_course_recommendation_model():
    model = CourseRecommendationModel()
    model.populate()
    return model

def recommend_course(req: grpcmsg.CourseRecommendationRequest, cache: SharableCache) -> grpcmsg.CourseRecommendationResponse:
    logger = getLogger('recommend_course')
    model: CourseRecommendationModel = cache.get_or_create('recommend_course_model', get_course_recommendation_model)
    res = []
    if req.variant == 'RANDOM':
        res = model.random_infer()
    elif req.variant in ['COSINE','COSINE32','COSINE16','COSINE8']:
        res = model.infer([(e.semesterKey.studyProgram, e.courseNo) for e in req.selectedCourses],req.variant)
    else:
        raise Exception('{} variant is invalid'.format(req.variant))
    enriched_res = []
    mongo = get_mongo_service()
    for study_program, course_no in res:
        if len(enriched_res) > 10:
            break
        if course_no in [e.courseNo for e in req.selectedCourses]:
            continue

        abbr = mongo.get_course_abbr(course_no=course_no, study_program=req.semesterKey.studyProgram, semester=req.semesterKey.semester, academic_year=req.semesterKey.academicYear)
        if abbr:
            d = grpcmsg.CourseRecommendationResponse.CourseDetail()
            d.key.courseNo = course_no
            d.key.semesterKey.CopyFrom(req.semesterKey)
            d.courseNameEn = abbr
            enriched_res.append(d)
    resp = grpcmsg.CourseRecommendationResponse()
    resp.courses.extend(enriched_res)
    return resp

def recommend_course_serialized(req: bytes, cache: SharableCache) -> bytes:
    r = grpcmsg.CourseRecommendationRequest()
    r.ParseFromString(req)
    res = recommend_course(r, cache)
    return res.SerializeToString()
