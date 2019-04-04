import requests
import base64

class RestClientAPIError(Exception):
    pass


class RecordNotFoundError(RestClientAPIError):
    pass


class RecordExistsError(RestClientAPIError):
    pass


class ASRestClientConnector(object):
    '''
    Class used to talk to Aerospike Rest Client
    '''

    def __init__(self, base_uri='http://localhost:8080'):
        '''constructor
           The constructor builds a base endpoint for RestClient operations of the form:
           base_uri/v1
        Args:
            base_uri (str) optional: The address on which the Rest client is listening.
                Default: `'http://localhost:8080'`
        '''

        # Build the base rest endpoint: base_uri/v1
        base_uri = base_uri + '/' if base_uri[-1] != '/' else base_uri
        self.rest_endpoint = base_uri + 'v1'
        self.kvs_endpoint = self.rest_endpoint + '/kvs'
        self.operate_endpoint = self.rest_endpoint + '/operate'

    def get_record(self, namespace, setname, userkey, **query_params):
        '''Retrieve a map representation of a record stored in aerospike

        Args:
            namespace (str): The namespace for the record.
            setname (str, int): The setname for the record.
            userkey (str) optional: The userkey of the record.
            query_params (Map[str:str]) optional: A Map of query params.
        Returns:
            dict: A dictionary containing entries for 'bins', 'generation' and 'ttl'
                example: {'bins': {'a': 1, 'b': 'c'}, 'generation': 2, 'ttl': 1234}
        Raises:
            RecordNotFoundError: If the specified record does not exist.
            RestClientAPIError: If an error is encountered communicating with the Endpoint.
        '''
        record_uri = self._get_record_uri(self.kvs_endpoint, namespace, setname, userkey)
        response = requests.get(record_uri, params=query_params)

        if response.ok:
            return response.json()

        self.raise_from_response(response, msg='Get record failed: ')

    def create_record(self, namespace, setname, userkey, bins, **query_params):
        '''Store a new record in the Aerospike database.

        Args:
            namespace (str): The namespace for the record.
            setname (str, int): The setname for the record.
            userkey (str) optional: The userkey of the record.
            bins (dict[str:any]): A dictionary containing the bins to store in the record
            query_params (Map[str:str]) optional: A Map of query params.
        Raises:
            RecordExistsError: If the specified record already exists.
            RestClientAPIError: If an error is encountered communicating with the Endpoint.
        '''
        record_uri = self._get_record_uri(self.kvs_endpoint, namespace, setname, userkey)
        response = requests.post(record_uri, json=bins, params=query_params)

        if response.ok:
            return

        self.raise_from_response(response, msg='Create record failed: ')

    def update_record(self, namespace, setname, userkey, bins, **query_params):
        '''Update an existing record
        Args:
            namespace (str): The namespace for the record.
            setname (str, int): The setname for the record.
            userkey (str) optional: The userkey of the record.
            bins (dict[str:any]): A dictionary containing the bins to update in the record.
                These may also contain bins which do not yet exist.
            query_params (Map[str:str]) optional: A Map of query params.
        Raises:
            RecordNotFoundError: If the specified record does not exist.
            RestClientAPIError: If an error is encountered communicating with the Endpoint.
        '''
        record_uri = self._get_record_uri(self.kvs_endpoint, namespace, setname, userkey)
        response = requests.patch(record_uri, json=bins, params=query_params)

        if response.ok:
            return

        self.raise_from_response(response, msg='Update record failed: ')

    def replace_record(self, namespace, setname, userkey, bins, **query_params):
        '''Replace an existing record in the Aerospike database.

        Args:
            namespace (str): The namespace for the record.
            setname (str, int): The setname for the record.
            userkey (str) optional: The userkey of the record.
            bins (dict[str:any]): A dictionary containing the bins to store in the record
            query_params (Map[str:str]) optional: A Map of query params.
        Raises:
            RecordNotFoundError: If the specified record does not yet exist.
            RestClientAPIError: If an error is encountered communicating with the Endpoint.
        '''
        record_uri = self._get_record_uri(self.kvs_endpoint, namespace, setname, userkey)
        response = requests.put(record_uri, json=bins, params=query_params)

        if response.ok:
            return

        self.raise_from_response(response, msg='Replace record failed: ')

    def delete_record(self, namespace, setname, userkey, **query_params):
        '''Delete a record from the Aerospike database

        Args:
            namespace (str): The namespace for the record.
            setname (str, int): The setname for the record.
            userkey (str) optional: The userkey of the record.
            query_params (Map[str:str]) optional: A Map of query params.
        Raises:
            RecordNotFoundError: If the specified record does not exist.
            RestClientAPIError: If an error is encountered communicating with the Endpoint.
        '''
        record_uri = self._get_record_uri(self.kvs_endpoint, namespace, setname, userkey)
        response = requests.delete(record_uri, params=query_params)

        if response.ok:
            return

        self.raise_from_response(response, msg='Delete record failed: ')

    def operate_record(self, namespace, setname, userkey, operations, **query_params):
        '''Perform a series of operations on the specified record.

        Args:
            namespace (str): The namespace for the record.
            setname (str, int): The setname for the record.
            userkey (str) optional: The userkey of the record.
            operations (list[dict[str:any]]): A list of operation dicts.
            query_params (Map[str:str]) optional: A Map of query params.
        Example:
            ops = [{'operation': 'READ', 'opValues': {'bin': 'b1'}}]
            returned_rec = client.operate_record('test', 'demo', '1' ops)
        Returns:
            dict: A dictionary containing entries for 'bins', 'generation' and 'ttl'
                example: {'bins': {'b1': 12345}, 'generation': 2, 'ttl': 1234}
        Raises:
            RestClientAPIError: If an error is encountered when performing the operations
        '''
        operate_uri = self._get_record_uri(self.operate_endpoint, namespace, setname, userkey)
        response = requests.post(operate_uri, json=operations, params=query_params)

        if response.ok:
            return response.json()

        self.raise_from_response(response, msg='Operate on record failed: ')

    @staticmethod
    def _get_record_uri(endpoint, namespace, setname, userkey):

        return '{endpoint}/{ns}/{setname}/{key}'.format(
            endpoint=endpoint, ns=namespace, setname=setname, key=str(userkey)
        )

    @staticmethod
    def raise_from_response(response, msg=''):

        if response.status_code == 404:
            raise RecordNotFound(msg + response.text)

        if response.status_code == 409:
            raise RecordExistsError(response.text)

        raise RestClientAPIError(msg + response.text)

    @staticmethod
    def encode_bytes_key(bytes_key):
        return base64.urlsafe_b64encode(bytes_key)
