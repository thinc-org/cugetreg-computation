import grpc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Manager
import sys
import os
from cgrcompute.components.multiprocess import SharableCache
from cgrcompute.components.courserecommendation import recommend_course_serialized
from logging import getLogger
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), 'grpc'))
from cgrcompute.grpc import cgrcompute_pb2_grpc, cgrcompute_pb2

POOL_SIZE = 4

logging.basicConfig(level=logging.NOTSET)

manager: Manager = None
pool: ProcessPoolExecutor = None

class CourseRecommendationServicer(cgrcompute_pb2_grpc.CourseRecommendationServicer):
    cache: SharableCache

    def __init__(self, cache):
        self.cache = cache
        self.logger = getLogger('CourseRecommendationServicer')

    def Recommend(self, request, context):
        self.logger.info('Processing request {}'.format(request))
        res =  cgrcompute_pb2.CourseRecommendationResponse()
        res.ParseFromString(pool.submit(recommend_course_serialized, request.SerializeToString(), self.cache).result())
        self.logger.info('Result {}'.format(res))
        return res

def create_server():
    global manager, pool
    manager = Manager()
    cache = SharableCache(manager)
    pool = ProcessPoolExecutor()
    server = grpc.server(ThreadPoolExecutor(max_workers=POOL_SIZE))
    server.add_insecure_port('[::]:50051')
    cgrcompute_pb2_grpc.add_CourseRecommendationServicer_to_server(CourseRecommendationServicer(cache), server)
    return server

def create_client():
    channel = grpc.insecure_channel('localhost:50051')
    return cgrcompute_pb2_grpc.CourseRecommendationStub(channel)

if __name__ == '__main__':
    logger = getLogger('main')
    logger.info("Starting CGR-Compute")
    server = create_server()
    server.start()
    server.wait_for_termination()
    manager.shutdown()
    pool.shutdown()
