from dataclasses import dataclass
from typing import Any

import gzip
import pickle
import base64


def decode_from_string(encoded_partition_id):
    gzip_bytes = base64.b64decode(bytes(encoded_partition_id, "utf-8"))
    partition_id_bytes = gzip.decompress(gzip_bytes)
    return pickle.loads(partition_id_bytes)


def encode_to_string(batch_transaction_id, batch_result):
    partition_id = PartitionId(batch_transaction_id, batch_result)
    partition_id_bytes = pickle.dumps(partition_id)
    gzip_bytes = gzip.compress(partition_id_bytes)
    return str(base64.b64encode(gzip_bytes), "utf-8")


@dataclass
class BatchTransactionId:
    transaction_id: str
    session_id: str
    read_timestamp: Any


@dataclass
class PartitionId:
    batch_transaction_id: BatchTransactionId
    batch_result: Any
