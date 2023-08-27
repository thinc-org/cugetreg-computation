import grpc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Manager
from cgrcompute.components.multiprocess import SharableCache
from cgrcompute.components.courserecommendation import recommend_course_serialized
from cgrcompute.components.health import HealthServicer
from logging import getLogger
import logging
import time

from cgrcompute.grpc import cgrcompute_pb2_grpc, cgrcompute_pb2, health_pb2_grpc

POOL_SIZE = 4

logging.basicConfig(level=logging.INFO)

manager: Manager = None
pool: ProcessPoolExecutor = None


class CourseRecommendationServicer(cgrcompute_pb2_grpc.CourseRecommendationServicer):
    cache: SharableCache

    def __init__(self, cache):
        self.cache = cache
        self.logger = getLogger('CourseRecommendationServicer')

    def Recommend(self, request, context):
        start = time.time()
        self.logger.info("Processing Recommend")
        res =  cgrcompute_pb2.CourseRecommendationResponse()
        res.ParseFromString(pool.submit(recommend_course_serialized, request.SerializeToString(), self.cache).result())
        self.logger.info("Processed Recommend took {} s".format(time.time() - start))
        return res


def create_server():
    global manager, pool
    manager = Manager()
    cache = SharableCache(manager)
    pool = ProcessPoolExecutor()
    server = grpc.server(ThreadPoolExecutor(max_workers=POOL_SIZE))
    server.add_insecure_port('[::]:50051')
    cgrcompute_pb2_grpc.add_CourseRecommendationServicer_to_server(CourseRecommendationServicer(cache), server)
    health_pb2_grpc.add_HealthServicer_to_server(HealthServicer(pool), server)
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
