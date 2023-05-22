import hashlib
import json
from pathlib import Path
from typing import List


def hash_file(filename: str) -> str:
    """
    Returns the SHA-1 hash of the file passed into it.

    Args:
        filename (str): The path to the file.

    Returns:
        str: The hexadecimal representation of the file's SHA-1 hash.
    """
    h = hashlib.sha1()

    with open(filename, "rb") as file:
        chunk = file.read(1024)
        while chunk:
            h.update(chunk)
            chunk = file.read(1024)

    return h.hexdigest()


def process_batch(batch_files: List[Path], tmp_file: Path) -> None:
    """
    Process a batch of files, calculate their hashes, and write the list of file paths
    and hashes to the temporary file.

    Args:
        batch_files (List[Path]): The list of files in the batch.
        tmp_file (Path): The path to the temporary file.
    """
    with open(tmp_file, "w") as f:
        for file in batch_files:
            file_hash = hash_file(file)
            f.write(f"{file},{file_hash}\n")


def retrieve_associated_feedstock_from_output_blob(file: Path):
    """
    Retrieves the associated feedstocks from the output blob file.

    Args:
        file (Path): The path to the output blob file.

    Returns:
        List[str]: A list of associated feedstock names.
    """
    with open(file, "r") as f:
        payload = json.loads(f.read())

    _, associated_feedstocks = payload.popitem()
    return associated_feedstocks
