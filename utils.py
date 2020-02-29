import pickle
import base64

class TaskDef:
    def __init__(self, source_file, other_files, function):
        self.source_file = source_file
        self.other_files = other_files
        self.function = function

def pack(obj):
    return base64.b64encode(pickle.dumps(obj)).decode("utf-8")

def unpack(obj_string):
    return pickle.loads(base64.b64decode(obj_string.encode("utf-8")))