from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy.orm import Session

from cfdb.populate.utils import (
    traverse_files,
    retrieve_associated_feedstock_from_output_blob,
)
from cfdb.log import logger, progressBar
from cfdb.models.schema import Artifacts, ArtifactsFilePaths, Packages


def _compare_files(artifacts, stored_files, root_dir):
    """
    Compares the Artifacts outputs from the database with the stored files (harvested), and returns a set of files that were not present in the database or were updated.

    Args:

    Returns:
    """
    db_files = {(Path(row[0]), row[1]) for row in artifacts}
    stored_files_set = set()

    for stored_file in stored_files:
        with open(stored_file, "r") as f:
            for line in f:
                file_path, file_hash = line.strip().split(",")
                stored_files_set.add((Path(file_path).relative_to(root_dir), file_hash))

    changed_files = stored_files_set - db_files

    if len(changed_files) > 0:
        logger.info(f"Detected {len(changed_files)} modified files.")

    return changed_files


def _update_feedstock_outputs(
    session: Session,
) -> Session:
    """
    Update or create the artifacts in the database based on the comparison between the stored data and the current data.

    Args:
        session (Session): The SQLAlchemy session object.

    Returns:
        Session: The updated SQLAlchemy session object.
    """
    artifacts = session.query(Artifacts).all()

    return session


def update(session: Session, path: Path):
    """
    Updates all artifacts in the database based on the recent changes from the harvested data.

    Args:
        session (Session): The database session.
        path (Path): The path to the directory containing the JSON files. From "harvesting".
    """
    _tmp_dir = TemporaryDirectory()
    tmp_dir = Path(_tmp_dir.name)

    logger.info("Querying database for Recent Artifacts...")
    artifacts = session.query(Artifacts.path, Artifacts.hash, Artifacts.name).all()

    logger.info(f"Traversing files in {path}...")
    stored_files = traverse_files(path, tmp_dir)

    logger.info("Comparing files...")
    changed_files = _compare_files(artifacts, stored_files, root_dir=path)

    if len(changed_files) == 0:
        logger.info("No changes detected. Exiting...")
        return

    with progressBar:
        for idx, (file, file_hash) in enumerate(
            progressBar.track(changed_files, description="Updating feedstocks...")
        ):
            # print(retrieve_associated_feedstock_from_output_blob(file))

            package_name, channel, arch, artifact_name = str(file).split("/")
            logger.info(
                f"Updating {package_name} :: {channel} :: {arch} :: {artifact_name}"
            )

            package = (
                session.query(Packages).filter(Packages.name == package_name).first()
            )

            # Create a new artifact record
            new_artifact = Artifacts(
                path=str(file),
                hash=file_hash,
                name=file.stem,
                package_name=package_name,
                platform=arch,
            )

            # Add the new artifact to the session
            session.add(new_artifact)

            session.commit()

    # with progressBar:
    #     for idx, (file, file_hash) in enumerate(
    #         progressBar.track(changed_files, description="Updating artifacts...")
    #     ):
    #         ...

    #         if idx % 100 == 0:
    #             session.commit()

    #     session.commit()


# def update(session: Session, path: Path):
#     """
#     Updates all artifacts in the database based on the recent changes from the harvested data.

#     Args:
#         session (Session): The database session.
#         path (Path): The path to the directory containing the JSON files. From "harvesting".
#     """
#     _tmp_dir = TemporaryDirectory()
#     tmp_dir = Path(_tmp_dir.name)

#     logger.info("Querying database for Recent Artifacts...")
#     artifacts = session.query(Artifacts.path, Artifacts.hash, Artifacts.name).all()

#     logger.info(f"Traversing files in {path}...")
#     stored_files = traverse_files(path, tmp_dir)

#     logger.info("Comparing files...")
#     changed_files = _compare_files(artifacts, stored_files, root_dir=path)

#     if len(changed_files) == 0:
#         logger.info("No changes detected. Exiting...")
#         return

#     with progressBar:
#         for idx, (file, file_hash) in enumerate(
#             progressBar.track(changed_files, description="Updating feedstocks...")
#         ):
#             associated_package_name = file.stem
#             associated_feedstocks = retrieve_associated_feedstock_from_output_blob(
#                 file=path / file  # Need to use the absolute path here
#             )
#             logger.debug(
#                 f"Associated package name: '{associated_package_name}' :: Associated feedstocks: '{associated_feedstocks}'"
#             )
#             package = (
#                 session.query(Packages)
#                 .filter(Packages.name == associated_package_name)
#                 .first()
#             )

#             if not package:
#                 logger.debug(
#                     f"Package '{associated_package_name}' not found in database. Proceeding to create it and its feedstock outputs."
#                 )
#                 package = Packages(
#                     name=associated_package_name,
#                 )
#                 session.add(package)

#             for file_name in associated_files:
#                 session = _update_artifact_files(
#                     session=session,
#                     file_rel_path=file,
#                     file_hash=file_hash,
#                     package_name=package.name,
#                 )

#             if idx % 100 == 0:
#                 session.commit()

#             # Add the artifact to the Artifacts table
#             artifact_name = f"{package.name}/{file.name}"
#             artifact = Artifacts(
#                 name=artifact_name,
#                 path=str(file),
#                 hash=file_hash,
#             )
#             session.add(artifact)

#     session.commit()
