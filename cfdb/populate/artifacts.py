from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy.orm import Session

from cfdb.log import logger, progressBar
from cfdb.models.schema import Artifacts, ArtifactsFilePaths, Packages


def _compare_files():
    """
    Compares the Artifacts outputs from the database with the stored files (harvested), and returns a set of files that were not present in the database or were updated.

    Args:

    Returns:
    """
    ...


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

    logger.info("Comparing files...")
    changed_files = _compare_files(...)

    if len(changed_files) == 0:
        logger.info("No changes detected. Exiting...")
        return

    with progressBar:
        for idx, (file, file_hash) in enumerate(
            progressBar.track(changed_files, description="Updating artifacts...")
        ):
            ...

            if idx % 100 == 0:
                session.commit()

        session.commit()
