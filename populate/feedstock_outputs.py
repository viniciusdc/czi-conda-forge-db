from pathlib import Path
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import select
from log import logger
from tempfile import TemporaryDirectory
import glob
import concurrent.futures

from schema import FeedstockOutputs, Packages, Feedstocks, uniq_id
import hashlib
import json
import tqdm
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)

from memory_profiler import profile


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


def process_batch(batch_files, tmp_file):
    with open(tmp_file, "w") as f:
        for file in batch_files:
            file_hash = hash_file(file)
            f.write(f"{file},{file_hash}\n")


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
    # check if given path is a directory
    if not path.is_dir():
        raise NotADirectoryError(f"{path} is not a directory.")

    files = []
    for file in glob.iglob(f"{path}/**/*.json", recursive=True):
        files.append(file)

    logger.debug(f"# of JSON blob files: {len(files)}")

    num_of_files = len(files)
    num_of_batches = num_of_files // 1000

    if num_of_files % 1000 != 0:
        num_of_batches += 1

    if output_dir is None:
        output_dir = Path(".")

    stored_files = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i in range(num_of_batches):
            tmp_file = output_dir / f"batch_{i}.json"
            logger.debug(f"Creating temporary file {tmp_file}...")
            batch_files = files[i * 1000 : (i + 1) * 1000]
            batch_files.reverse()

            executor.submit(process_batch, batch_files, tmp_file)

            stored_files.append(tmp_file)

    del files

    logger.debug(f"# of stored files: {len(stored_files)}")

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

    logger.debug(f"# of changed files: {len(changed_files)}")

    return changed_files


progress = Progress(
    TextColumn("[progress.description]{task.description}"),
    BarColumn(
        bar_width=None,
        pulse_style="bright_black",
    ),
    TaskProgressColumn(),
    TimeRemainingColumn(),
    MofNCompleteColumn(),
    expand=True,
)


def update(session: Session, path: Path):
    logger.info("Updating feedstocks...")
    # Query the database for all feedstocks, to check the diff between the stored data and the current data
    # to avoid unnecessary updates, we will check the hash of the file with the stored hash to see if it has changed and needs to be updated
    # For the contents that will be updated, in order to avoid storing a lot of data in an object within the context python memory, we will store in a temp
    # file (splitted by batches) and then we will read the file and store the data in the database accordingly

    # create temp dir and batch files based on the number of feedstocks to be updated, the update will be sequential for now to avoid race conditions with the sqlite database, once we move to postgres we can parallelize the update

    logger.debug("Creating temporary directory...")
    _tmp_dir = TemporaryDirectory()
    tmp_dir = Path(_tmp_dir.name)

    logger.info("Querying database for feedstock outputs...")
    # run query to get all feedstocks: select by path, hash and id
    feedstock_outputs = session.query(
        FeedstockOutputs.path, FeedstockOutputs.hash, FeedstockOutputs.id
    ).all()

    logger.info(f"Traversing files in {path}...")
    # run the os walk transverse function to retrieve all the files in the feedstock outputs folder for comparison
    stored_files = transverse_files(path, tmp_dir)

    logger.info("Comparing files...")
    # compare the files in the database with the files in the stored files
    changed_files = _compare_files(feedstock_outputs, stored_files)

    with progress:
        for idx, file in enumerate(
            progress.track(changed_files, description="Updating feedstocks...")
        ):
            associated_package_name = file.stem

            # Query the database to get the package by name
            package = (
                session.query(Packages)
                .filter(Packages.name == associated_package_name)
                .first()
            )  # ths should be unique, thus we can use first()

            if not package:
                # if there is no package, then there will be no feedstock
                logger.debug(
                    f"Package [bold teal]{associated_package_name}[/] not found in database. Proceeding to create it and its feedstock outputs."
                )

                # Create the package in the database
                package = Packages(
                    name=associated_package_name,
                )
                session.add(package)

            # update the feedstock outputs in the database
            with open(file, "r") as f:
                # structure of the file {"feedstocks": ["21cmfast"]}
                payload = json.loads(f.read())
                _, associated_feedstocks = payload.popitem()

            logger.debug(
                f"Associated package name: [bold teal]{associated_package_name}[/] :: Associated feedstocks: {associated_feedstocks}"
            )

            # Update the feedstock outputs in the database
            for feedstock_name in associated_feedstocks:
                # Query the database to get the feedstock by name and package_id
                feedstock = (
                    session.query(Feedstocks)
                    .filter(
                        Feedstocks.name == feedstock_name,
                        # Feedstocks.package_name == package.name,
                    )
                    .first()  # this should be unique, thus we can use first()
                )

                if feedstock:
                    # Update the feedstock output in the database
                    # There can be multiple feedstocks for a given package but only one package for a given feedstock
                    # We will query the FeedstockOutputs table based on the Index(feedstock.name, package.name)
                    feedstock_output = (
                        session.query(FeedstockOutputs)
                        .filter(
                            FeedstockOutputs.feedstock_name == feedstock.name,
                            FeedstockOutputs.package_name == package.name,
                        )
                        .first()
                    )
                    logger.debug(
                        f"Feedstock [bold teal]{feedstock_name}[/] found in database. Proceeding to update its feedstock output."
                    )

                    if feedstock_output:
                        # It is expected based on our previous filtering of the diff, that the only thing that has changed is the hash
                        # feedstock_output.path = file.relative_to(path)

                        feedstock_output.hash = hash_file(file)

                        # Update existing feedstock output in the database
                        session.add(feedstock_output)

                    else:
                        new_feedstock_output = FeedstockOutputs(
                            id=uniq_id(),
                            path=file.relative_to(path).as_posix(),
                            feedstock_name=feedstock.name,
                            package_name=package.name,
                            hash=hash_file(file),
                        )
                        session.add(new_feedstock_output)
                else:
                    logger.debug(
                        f"Feedstock [bold teal]{feedstock_name}[/] not found in database. Proceeding to create it and its feedstock output."
                    )
                    # Create the feedstock in the database
                    new_feedstock = Feedstocks(
                        name=feedstock_name,
                        # package_name=package.name,
                    )
                    session.add(new_feedstock)

                    # Create the feedstock output in the database
                    new_feedstock_output = FeedstockOutputs(
                        id=uniq_id(),
                        path=file.relative_to(path).as_posix(),
                        feedstock_name=new_feedstock.name,
                        package_name=package.name,
                        hash=hash_file(file),
                    )
                    session.add(new_feedstock_output)

            if idx % 100 == 0:
                # every 100 files, commit the changes to the database
                session.commit()

        session.commit()
