
class User(object):
    '''
    A basic class representing a user
    '''
    def __init__(self, id, name, email, interests=None):

        interests = [] if not interests else interests

        self.id = id
        self.name = name
        self.email = email
        self.interests = interests

    def __repr__(self):
        return 'User({id}, {name}, {email}, {interests})'.format(
            id=repr(self.id),
            name=repr(self.name),
            email=repr(self.email),
            interests=('[' + ', '.join([repr(interest) for interest in self.interests]) + ']')
        )
