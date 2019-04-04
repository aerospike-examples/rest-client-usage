import sys

from asrestclient.restclientconnector import ASRestClientConnector as ASRC
from asrestclient.restclientconnector import RecordExistsError
from asrestclient.user_connector import UserConnector
from asrestclient.user import User

def main(interest='aerospike'):
    # Our Aerospike RestClient is running at "http://localhost:8080"
    user_connector = UserConnector(ASRC('http://localhost:8080'), 'test', 'users')

    user1 = User('123456', 'Bob Roberts', 'Bob@NotAValid.com.email.com', ['cooking', 'gardening', 'sewing'])
    user2 = User('6545321', 'Alice Allison', 'Alice@NotAValid.com.email.com', ['programming', 'gardening', 'mathematics'])

    try:
        user_connector.create_user(user1)
    # If the user already existed, just ignore it
    except RecordExistsError as ree:
        pass

    try:
        user_connector.create_user(user2)
    # If the user already existed, just ignore it
    except RecordExistsError as ree:
        pass

    retrieved_user1 = user_connector.get_user(user1.id)
    print("***The first user retrieved from the database is***")
    print(retrieved_user1)

    retrieved_user2 = user_connector.get_user(user2.id)
    print("\n***The second user retrieved from the database is***")
    print(retrieved_user2)

    new_interests = user_connector.add_interest(user1.id, interest)
    print("\n***Updated interests are:***")
    print(new_interests)

    retrieved_user1 = user_connector.get_user(user1.id)
    print("\n***The first user retrieved from the database is***")
    print(retrieved_user1)

if __name__ == '__main__':
    main()
