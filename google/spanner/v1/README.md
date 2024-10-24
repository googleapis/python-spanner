# Spanner Servicer

The Spanner server definition files were generated with these commands:

```shell
pip install grpcio-tools
git clone git@github.com:googleapis/googleapis.git
cd googleapis
python -m grpc_tools.protoc \
    -I . \
    --python_out=. --pyi_out=. --grpc_python_out=. \
    ./google/spanner/v1/*.proto
```

