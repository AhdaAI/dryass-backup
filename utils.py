import os
import hashlib
import json


def get_file_hash(path):
    """Return SHA256 hash of a file."""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_folder_hash(path):
    """Return combined hash of all files in a folder."""
    hasher = hashlib.sha256()
    for root, _, files in os.walk(path):
        for fname in sorted(files):
            fpath = os.path.join(root, fname)
            hasher.update(get_file_hash(fpath).encode())
    return hasher.hexdigest()


def load_metadata(meta_file):
    if os.path.exists(meta_file):
        with open(meta_file, "r") as f:
            return json.load(f)
    return {}


def save_metadata(meta_file, data):
    with open(meta_file, "w") as f:
        json.dump(data, f, indent=2)


def get_size(path) -> int:
    """
    Parameters
    -------
    path (Path)
        The path to file or folder.

    Returns
    -------
    int (bytes)
        Return the size of a file or folder in bytes.
    """
    if os.path.isfile(path):
        return os.path.getsize(path)
    total_size = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            total_size += os.path.getsize(os.path.join(dirpath, f))
    return total_size
