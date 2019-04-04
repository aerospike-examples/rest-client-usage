from . import restclientconnector
from . import user
from . import constants

class UserConnector(object):

    '''
    User Connector class. Provides an interface to store
    User objects into the aerospike database
    '''

    def __init__(self, client, namespace, setname):
        '''constructor

        Args:
            namespace (String): The Aerospike Namespace to be used to store users.
            setname (String): The Aerospike Set to be used to store users
            client (ASRestClientConnector): A connector instance which will be utilized to perform
                REST operations.
        '''
        self.namespace = namespace
        self.setname = setname
        self.client = client
    
    def create_user(self, user, errror_if_exists=True):
        '''
        Description:
            Store a user into the aerospike database. It will not update an existing user.
        Args:
            user (User): A user object to be stored into the Aerospike database. The `user.id`
                field will be cast to a string before being used as the key.
            error_if_exists (bool): A flag indicating whether an exception should be raised if
                a user with a matching id already exists in the Database. Default: `True`

        Raises:
            RestClientAPIError: If an error occurs when speaking to the API.
        '''

        userkey = user.id
        bins = {
            'id': user.id,
            'name': user.name,
            'email': user.email,
            'interests': user.interests,
        }

        try:
            self.client.create_record(self.namespace, self.setname, userkey, bins)
        except restclientconnector.RecordExistsError as ree:
            if errror_if_exists:
                raise ree

    def get_user(self, user_id):
        '''
        Description
            Retrieves a User instance populated with information stored in the Aerospike Database. If
            a user is not found None will be returned.
        Args:
            user_id: A unique id for a user. It will be converted to a String before being used to look up
                a user.
        Returns:
            User, None: Returns a new User instance if the user is found in Aerospike, else None

        Raises:
            RestClientAPIError: If an error occurs when speaking to the API.
        '''

        try:
            user_details = self.client.get_record(self.namespace, self.setname, user_id)['bins']
            return user.User(
                user_details['id'], user_details['name'],
                user_details['name'], user_details['interests'])
        except restclientconnector.RecordNotFoundError as ree:
            return None

    def add_interest(self, user_id, interest):
        '''
        Description
            Adds an interest to the list of interests for a User stored in the database. This will not create a
            new user.
        Args:
            user_id: A unique id for a user. It will be converted to a String before being used to look up
                a user.
            interest (string): An interest to append to the list of interestss for the user
        Returns:
            list[string]: The updated list of interests for the user.

        Raises:
            RestClientAPIError: If an error occurs when speaking to the API.
        '''
        add_interest_ops = self._add_interest_and_retrieve_ops(interest)

        # Add update only to prevent creation of a new user
        response = self.client.operate_record(
            self.namespace, self.setname, user_id,
            add_interest_ops, recordExistsAction=constants.UPDATE_ONLY)
        new_interests = response['bins']['interests']
        # The response contains one entry for the length of interests, the second is the new list of interests
        return new_interests[1]

    @staticmethod
    def _add_interest_and_retrieve_ops(interest):
        '''Build a list of operations to add an interest to a user's list of interests in the Aerospike Database

        Args:
            interest (str): The interest to add to the users list
        Returns:
            list[map] : A list of operations to be passed to the ASRestClientConnector.operate_record method
        '''
        ops = [
            {
                constants.OPERATION_NAME: constants.LIST_APPEND_OP,
                constants.OPERATION_VALUES: {
                    'bin': 'interests',
                    'value': interest
                }
            },
            {
                constants.OPERATION_NAME: constants.READ_OP,
                constants.OPERATION_VALUES: {
                    'bin': 'interests'
                }
            }
        ]
        return ops
