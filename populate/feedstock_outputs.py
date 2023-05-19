from pathlib import Path
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import select
from log import logger
from tempfile import TemporaryDirectory
import os
import glob

from schema import FeedstockOutput
import hashlib


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


def transverse_files(path: Path, output_dir: Path = None) -> List[Path]:
    """
    Traverses  a directory of JSON files, generating a list of dictionaries
    with file paths and hashes. These dictionaries are written to an output directory.

    The hashes allow comparison between the directory and a database for necessary updates.
    Files are processed in batches of 1000 to optimize memory usage.

    Args:
        path (Path): The path to the directory containing the JSON files.
        output_dir (Path, optional): The output directory to store the list of dictionaries.
        If not provided, the current directory will be used. Defaults to None.

    Returns:
        List[Path]: A list of paths to the stored files.
    """
    files = glob.glob(str(path / "**/*.json"), recursive=True)
    num_of_files = len(files)
    num_of_batches = num_of_files // 1000

    if output_dir is None:
        output_dir = Path(".")

    stored_files = []

    for i in range(num_of_batches):
        tmp_file = output_dir / f"batch_{i}.json"
        with open(tmp_file, "w") as f:
            for file in files[i * 1000 : (i + 1) * 1000]:
                file_hash = hash_file(file)
                f.write(f"{file},{file_hash}\n")
        stored_files.append(tmp_file)

    return stored_files


def _compare_files(feedstock_outputs, stored_files):
    """
    Compares the feedstock outputs from the database with the stored files, and returns a set of files that were not present in the database or have changed hashes.

    Args:
        feedstock_outputs (List[Tuple]): List of tuples containing the path, hash, and id of feedstock outputs from the database.
        stored_files (List[Path]): List of paths to the stored files.

    Returns:
        Set[Path]: A set of file paths that were not present in the database or have changed hashes.
    """
    db_files = set((Path(row[0]), row[1]) for row in feedstock_outputs)
    stored_files_set = set()

    for stored_file in stored_files:
        with open(stored_file, "r") as f:
            for line in f:
                file_path, file_hash = line.strip().split(",")
                stored_files_set.add((Path(file_path), file_hash))

    changed_files = {
        file_path
        for file_path, file_hash in stored_files_set
        if file_path not in db_files or file_hash != db_files[file_path]
    }

    return changed_files


def update(session: Session, path: Path):
    logger.info("Updating feedstocks...")
    # Query the database for all feedstocks, to check the diff between the stored data and the current data
    # to avoid unnecessary updates, we will check the hash of the file with the stored hash to see if it has changed and needs to be updated
    # For the contents that will be updated, in order to avoid storing a lot of data in an object within the context python memory, we will store in a temp
    # file (splitted by batches) and then we will read the file and store the data in the database accordingly

    # create temp dir and batch files based on the number of feedstocks to be updated, the update will be sequential for now to avoid race conditions with the sqlite database, once we move to postgres we can parallelize the update

    _tmp_dir = TemporaryDirectory()
    tmp_dir = Path(_tmp_dir.name)

    # run query to get all feedstocks: select by path, hash and id
    feedstock_outputs = session.execute(
        select(
            FeedstockOutput.path, FeedstockOutput.hash, FeedstockOutput.id
        ).select_from(FeedstockOutput)
    ).fetchall()

    # run the os walk transverse function to retrieve all the files in the feedstock outputs folder for comparison
    stored_files = transverse_files(path, tmp_dir)

    # compare the files in the database with the files in the stored files
    changed_files = _compare_files(feedstock_outputs, stored_files)
