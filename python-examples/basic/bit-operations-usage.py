import base64

import msgpack
import requests

REST_BASE = 'http://localhost:8080/v1'
KVS_ENDPOINT = REST_BASE + '/kvs'
OPERATE_ENDPOINT = REST_BASE + '/operate'

namespace = 'test'
setname = 'foo'
userkey = 'bit'

operate_record_uri = '{base}/{ns}/{setname}/{userkey}'.format(
    base=OPERATE_ENDPOINT, ns=namespace, setname=setname, userkey=userkey)

kvs_record_uri = '{base}/{ns}/{setname}/{userkey}'.format(
    base=KVS_ENDPOINT, ns=namespace, setname=setname, userkey=userkey)

pack_request_header = {'Content-Type': "application/msgpack"}
pack_response_header = {'Accept': "application/msgpack"}

# Init the record
byte_list = [1, 2, 3, 4]
byte_array = bytearray(byte_list)
bins = {'bitOp': byte_array}

# Store the packed content
# KV bytearrays can only be sent using MessagePack
requests.post(kvs_record_uri, msgpack.packb(bins, use_bin_type=True), headers=pack_request_header)

response = requests.get(kvs_record_uri)
bit_op = [b for b in base64.b64decode((response.json()['bins']['bitOp']).encode('ascii'))]
print(bit_op)
# [1, 2, 3, 4]

# Bit insert
b_array = bytearray([100])
bit_insert_operation = [
    {
        'operation': 'BIT_INSERT',
        'opValues': {
            'bin': 'bitOp',
            'byteOffset': 1,
            'value': base64.b64encode(b_array).decode("ascii")
        }
    }
]

requests.post(operate_record_uri, json=bit_insert_operation)
response = requests.get(kvs_record_uri)
bit_op = [b for b in base64.b64decode((response.json()['bins']['bitOp']).encode('ascii'))]
print(bit_op)
# [1, 100, 2, 3, 4]

# Bit not
bit_not_operation = [
    {
        'operation': 'BIT_NOT',
        'opValues': {
            'bin': 'bitOp',
            'bitOffset': 16,
            'bitSize': 16
        }
    }
]

requests.post(operate_record_uri, json=bit_not_operation)
response = requests.get(kvs_record_uri)
bit_op = [b for b in base64.b64decode((response.json()['bins']['bitOp']).encode('ascii'))]
print(bit_op)
# [1, 100, 253, 252, 4]

# Delete the record
requests.delete(kvs_record_uri)
