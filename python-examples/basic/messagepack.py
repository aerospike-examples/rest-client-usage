import requests
import msgpack


def pack(obj):
    '''
    msgpack an object using the binary type
    '''
    return msgpack.packb(obj, use_bin_type=True)


def unpack(obj):
    '''
    Unpack object turning message pack strings into unicode
    '''
    return msgpack.unpackb(obj, encoding='UTF-8')

request_uri = 'http://localhost:8080/v1/kvs/test/demo/mp'

pack_request_header = {'Content-Type': "application/msgpack"}
pack_response_header = {'Accept': "application/msgpack"}

# Python 3
# For Python 27 specify 'ba' as bytearray('1234', 'UTF-8')
bins = {'ba': b'1234', 'a': {1: 2, 3: 4}}

# Store the packed content
requests.post(request_uri, pack(bins), headers=pack_request_header)

# The request without specifying msgpack return format.
# The map keys are converted to strings, and the bytte array is urlsafe base64 encoded.
print("content with Accept: application/json")
print(requests.get(request_uri).json())
# {'ttl': 2591545, 'bins': {'ba': 'MTIzNA==', 'a': {'3': 4, '1': 2}}, 'generation': 1}


response = requests.get(request_uri, headers=pack_response_header)
print("Content with Accept: application/msgpack")
print(unpack(response.content))

# {'ttl': 2591958, 'bins': {'ba': b'1234', 'a': {1: 2, 3: 4}}, 'generation': 1}
