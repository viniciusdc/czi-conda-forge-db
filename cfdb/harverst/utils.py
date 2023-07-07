import os
import glob


def recursive_ls(root):
    """
    Recursively lists JSON files in subdirectories of a given root directory.

    Args:
        root (str): Root directory to start listing files from.

    Yields:
        tuple: A tuple containing package name and the relative path of each JSON file found.
    """
    packages = os.listdir(root)
    for package in packages:
        files = glob.glob(f"{root}/{package}/*/*/*.json")
        for file_path in files:
            relative_path = file_path.replace(f"{root}/{package}/", "")
            yield package, relative_path
