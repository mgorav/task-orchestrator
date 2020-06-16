from contextlib import contextmanager
import os, fnmatch


@contextmanager
def open_file(path, mode):
    try:
        f = open(path, mode)
        yield f
    finally:
        f.close()


def find_file(file_name, init_scanning_path='.'):
    for root, dirs, files in os.walk(init_scanning_path):
        if file_name in files:
            return os.path.join(root, file_name)


def find_files(file_pattern, init_scanning_path):
    found_files = []
    for root, dirs, files in os.walk(init_scanning_path):
        for name in files:
            if fnmatch.fnmatch(name, file_pattern):
                found_files.append(os.path.join(root, name))
    return found_files
