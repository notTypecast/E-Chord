

class Storage:
    """
    Storage class
    Used to store (key, value) pairs on a node
    """

    def __init__(self):
        """
        Initializes a storage object
        """
        self.keys_to_ids = {}
        self.keys_to_vals = {}

    def __getitem__(self, key):
        """
        Returns the value of the key in dictionary (if it exists)
        Raises KeyError if it doesn't
        :param key: the key
        :return: the value
        """
        return self.keys_to_vals[key]

    def add_key(self, key, value, key_id):
        """
        Adds a new key to storage
        :param key: the key
        :param value: the value
        :param key_id: the ID of the key
        :return: None
        """
        self.keys_to_ids[key] = key_id
        self.keys_to_vals[key] = value

    def __delitem__(self, key):
        """
        Removes a key (if it exists) from storage
        :param key: the key
        :return: None
        """

        if key in self.keys_to_ids:
            del self.keys_to_ids[key]
        if key in self.keys_to_vals:
            del self.keys_to_vals[key]

    def __contains__(self, key):
        """
        Returns true or false, depending on whether the key exists in storage
        :param key: the key to check for
        :return: bool
        """
        return key in self.keys_to_vals

