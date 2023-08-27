import grpc
from concurrent.futures import ProcessPoolExecutor, BrokenExecutor
from logging import getLogger
import threading

from cgrcompute.grpc import health_pb2_grpc, health_pb2

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


