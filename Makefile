init:
	pip install -r requirements.txt

generate-grpc:
	python -m grpc_tools.protoc -Iproto --python_out=cgrcompute/grpc --grpc_python_out=cgrcompute/grpc proto/*

test:
	python -m unittest discover -s tests

run:
	python -m cgrcompute.server

.PHONY: init generate-grpc test run
