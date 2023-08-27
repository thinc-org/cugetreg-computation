import grpc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, BrokenExecutor
from multiprocessing import Manager
import sys
import os
from cgrcompute.components.multiprocess import SharableCache
from cgrcompute.components.courserecommendation import recommend_course_serialized
from logging import getLogger
import logging
import time
import threading

sys.path.append(os.path.join(os.path.dirname(__file__), 'grpc'))
from cgrcompute.grpc import cgrcompute_pb2_grpc, cgrcompute_pb2, health_pb2_grpc, health_pb2

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


class HealthServicer(health_pb2_grpc.HealthServicer):

    def __init__(self, pool: ProcessPoolExecutor):
        self.pool = pool
        self.logger = getLogger("HealthServicer")

    @staticmethod
    def ping() -> str:
        return "pong"

    def _check_pool(self) -> bool:
        # Pool can sometimes crash. probably due to OOM killing. So we must check that.
        try:
            task = self.pool.submit(HealthServicer.ping)
            assert task.result() == "pong"
            return True
        except BrokenExecutor:
            return False

    def _check_all(self) -> bool:
        return self._check_pool()

    def Check(self, request, context: grpc.ServicerContext):
        if request.service == "":
            ok = self._check_all()
            self.logger.info(f"check with status: {ok}")
            return health_pb2.HealthCheckResponse(
                status=health_pb2.HealthCheckResponse.ServingStatus.SERVING if ok else health_pb2.HealthCheckResponse.ServingStatus.NOT_SERVING
            )
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return health_pb2.HealthCheckResponse()

    def Watch(self, request, context: grpc.ServicerContext):
        if request.service != "":
            yield health_pb2.HealthCheckResponse(
                status=health_pb2.HealthCheckResponse.ServingStatus.SERVICE_UNKNOWN,
            )
            return
        self.logger.info("starting watch")
        closed_condition = threading.Condition()
        def on_close():
            with closed_condition:
                closed_condition.notify()
        context.add_callback(on_close)

        prv_status = None
        with closed_condition:
            while not closed_condition.wait(timeout=1):
                status = self._check_all()
                if status == prv_status:
                    continue
                self.logger.info(f"Service status changed {prv_status} -> {status}")
                yield health_pb2.HealthCheckResponse(
                    status=health_pb2.HealthCheckResponse.ServingStatus.SERVING if status else health_pb2.HealthCheckResponse.ServingStatus.NOT_SERVING
                )
                prv_status = status
        self.logger.info("end watch")

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
