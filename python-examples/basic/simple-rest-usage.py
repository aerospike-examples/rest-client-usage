import requests

REST_BASE = 'http://localhost:8080/v1'
KVS_ENDPOINT = REST_BASE + '/kvs'
OPERATE_ENDPOINT = REST_BASE + '/operate'

# Components of the Key
namespace = 'test'
setname = 'users'
userkey = 'bob'

record_uri = '{base}/{ns}/{setname}/{userkey}'.format(
    base=KVS_ENDPOINT, ns=namespace, setname=setname, userkey=userkey)

# The content to be stored into Aerospike.
bins = {
    'name': 'Bob',
    'id': 123,
    'color': 'Purple',
    'languages': ['Python', 'Java', 'C'],
}


# Store the record
res = requests.post(record_uri, json=bins)

# Get the record
# It is a map: {
#    'bins': {},
#    'generation': #,
#    'ttl': #
# }
response = requests.get(record_uri)
print("*** The Original Record ***")
print(response.json())

# Change the value of the 'color' bin
update_bins = {'color': 'Orange'}
requests.patch(record_uri, json=update_bins)

# Get the updated Record. Only the 'color' bin has changed
response = requests.get(record_uri)
print("*** The updated Record ***")
print(response.json())

# Replace the record with a new version
replacement_bins = {'single': 'bin'}
requests.put(record_uri, json=replacement_bins)

# Get the new Record.
response = requests.get(record_uri)
print("*** The Replaced Record ***")
print(response.json())

# Delete the record.
response = requests.delete(record_uri)

# Try to get the deleted . We will receive a 404.
response = requests.get(record_uri)

print('*** The response code for a GET on a non existent record is {} ***'.format(response.status_code))

# The response also includes a JSON error object
print("*** The Error object is: ***")
print(response.json())

# The rest client also supports more complicated operations.
# For example, suppose that we are storing information about users.
# Initially we store their name, and the length of their name.
# Our first user is 'Bob'
base_record = {'name': 'Bob', 'name_length': 3}
requests.post(record_uri, json=base_record)

# Suppose we want to append some characters to the name
# Now we want to add to the name of our user, keep the name length field accurate,
# and add a new bin containing the company for which the user works.

# We can specify the operations. As an array of JSON objects.
# These operations change the user's name to 'Bob Roberts', update the length
# Of the name accordingly, and indicate that he works for Aerospike.
# For a list of operations, and example usage see https://github.com/aerospike/aerospike-client-rest/blob/master/docs/operate.md

operations = [
    {
        'operation': 'APPEND',
        'opValues': {
            'bin': 'name',
            'value': ' Roberts'
        }
    },
    {
        'operation': 'ADD',
        'opValues': {
            'bin': 'name_length',
            'incr': len(' Roberts')
            }
    },
    {
        'operation': 'PUT',
        'opValues': {
            'bin': 'company',
            'value': 'Aerospike'
        }
    }
]

operate_record_uri = '{base}/{ns}/{setname}/{userkey}'.format(
    base=OPERATE_ENDPOINT, ns=namespace, setname=setname, userkey=userkey)

# Perform the operations on the record.
requests.post(operate_record_uri, json=operations)

# Fetch the updated record.
response = requests.get(record_uri)
print("*** The Record after applying operations ***")

print(response.json())

