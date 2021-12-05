====================
CUGetReg's Computation Backend (cgrcompute)
====================

Overview
====================

CUGetReg Computation is a backend for performing CPU-intensive tasks. The system is designed with following ideas:

- **Complementing Main NodeJS Backend**: NodeJS is not good at CPU-intensive task, so by off-loading the tasks to this backend, the task can be done more efficiently.
- **Multi Processing**: This backend use Multi-Processing Pool to maximize throughput of tasks processing.
- **gRPC**: To expose API for the main backend. This backend use gRPC to provides self-describing API

Functionality
====================

Currently, this backend handles:

- Course Recommendation System


Running the server
====================

To start contributing to this repository:

1. Create VirtualEnv

::
        python -m venv myvenv
        source myvenv/bin/activate

2. Install the package

::
        make init

3. Config the server: Copy ``config.template.ini`` to ``config.ini`` and modify as needed.

4. Launch the server

::
        python -m cgrcompute.server


Development
====================

- Typing are usually describe either by unit-test or type-hint.
- Some functionality may requires external service, I'm working on how to fix that.
        - Drill can be proxied.
