from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Set, Tuple

from sqlalchemy.orm import Session

from cfdb.log import logger, progressBar
from cfdb.models.schema import ImportToPackageMaps, Packages
from cfdb.populate.utils import transverse_files


def _compare_files(
    feedstock_outputs: List[Tuple[str, str, int]],
    stored_files: List[Path],
    root_dir: Path,
) -> Set[Path]:
    # (package_name.partition, hash)
    db_files = {(Path(f"{row[0]}.{row[1]}.json"), row[2]) for row in feedstock_outputs}
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


def update(session: Session, path: Path):
    """
    Updates Import to Package maps in the database  based on the comparison between the stored data and the current data.

    Args:
        session (Session): The SQLAlchemy session object.
        path (Path): The path to import to package maps directory containing the JSON blobs. (relative to the root directory of "libcfgraph" or viable alternative).
    """
    logger.info("Updating feedstocks...")
    logger.debug("Creating temporary directory...")
    _tmp_dir = TemporaryDirectory()
    tmp_dir = Path(_tmp_dir.name)

    logger.info("Querying database for current mappings...")
    _database_mappings = session.query(
        ImportToPackageMaps.parent_package_name,
        ImportToPackageMaps.partition,
        ImportToPackageMaps.hash,
    ).all()

    logger.info(f"Traversing files in {path}...")
    stored_files = transverse_files(path, tmp_dir)

    logger.info("Comparing files...")
    changed_files = _compare_files(_database_mappings, stored_files, root_dir=path)
