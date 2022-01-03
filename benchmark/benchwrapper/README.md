# Benchwrapper

A small gRPC wrapper around the spanner client library. This allows the
benchmarking code to prod at spanner without speaking Python.

## Running
Run the following commands from python-spanner/ directory.
```
export SPANNER_EMULATOR_HOST=localhost:8080
python3 -m benchmark.benchwrapper.main --port 8080
