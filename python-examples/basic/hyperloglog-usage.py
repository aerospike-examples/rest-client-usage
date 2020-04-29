import requests

REST_BASE = 'http://localhost:8080/v1'
KVS_ENDPOINT = REST_BASE + '/kvs'
OPERATE_ENDPOINT = REST_BASE + '/operate'

namespace = 'test'
setname = 'foo'
userkey = 'hll'

operate_record_uri = '{base}/{ns}/{setname}/{userkey}'.format(
    base=OPERATE_ENDPOINT, ns=namespace, setname=setname, userkey=userkey)

kvs_record_uri = '{base}/{ns}/{setname}/{userkey}'.format(
    base=KVS_ENDPOINT, ns=namespace, setname=setname, userkey=userkey)

# Init the record
hll_values = ["a", "b", "c", "d", "e", "f", "g", "g"]
hll_init_operation = [
    {
        'operation': 'HLL_INIT',
        'opValues': {
            'bin': 'hllBin',
            'indexBitCount': 8,
            'minHashBitCount': 8
        }
    },
    {
        'operation': 'HLL_ADD',
        'opValues': {
            'bin': 'hllBin',
            'values': hll_values
        }
    }
]

requests.post(operate_record_uri, json=hll_init_operation)

# HLL refresh count
hll_count_operation = [
    {
        'operation': 'HLL_SET_COUNT',
        'opValues': {
            'bin': 'hllBin'
        }
    }
]

response = requests.post(operate_record_uri, json=hll_count_operation)
print(response.json()['bins'])
# {'hllBin': 7}

# Delete the record
requests.delete(kvs_record_uri)
