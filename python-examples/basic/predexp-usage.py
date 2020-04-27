import base64

import requests

namespace = 'test'
setname = 'foo'
userkey = 'bar'

KVS_URL = 'http://localhost:8080/v1/kvs'

# In this example I put all metadata checks first, because Aerospike doesn't have an optimizer
# that knows how to shuffle predicates around more efficiently. This way, if the data is on an SSD,
# we can save on reading the record.
exp = b'DIGEST_MODULO(3, ==, 1) or LAST_UPDATE(>=, 1577880000) or (c >= 11 and not c < 20)'

encoded_exp = base64.b64encode(exp)

predexp_uri = '{base}/{ns}/{setname}/{userkey}'.format(
    base=KVS_URL, ns=namespace, setname=setname, userkey=userkey)

response = requests.get(predexp_uri, {"predexp": encoded_exp})
print(response.json())
