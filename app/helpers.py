import os
import pickle

class FileDict(dict):

    def __init__(self, path):
        self.path = path

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except:
            pass
        try:
            with open(os.path.join(self.path, str(key)), 'rb') as f:
                return pickle.load(f)
        except:
            raise KeyError(key + " Not Found")

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        with open(os.path.join(self.path, str(key)), 'wb') as f:
            pickle.dump(value, f)
