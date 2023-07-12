import typer
from typer.core import TyperGroup
from click import Context
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cfdb.models.schema import Base
from cfdb.populate import artifacts, feedstock_outputs, import_to_package_maps
from cfdb.log import logger
from pathlib import Path


class CFDBHandler:
    """
    CFDBHandler class handles the database operations for CFDB.

    Args:
        db_url (str): The URL of the database.

    Attributes:
        db_url (str): The URL of the database.
        engine (Engine): SQLAlchemy Engine object.
        Session (sessionmaker): SQLAlchemy sessionmaker object.

    Methods:
        update_feedstock_outputs: Update the feedstock outputs in the database.
        update_artifacts: Update the artifacts in the database.
    """

    def __init__(self, db_url):
        self.db_url = db_url
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def update_feedstock_outputs(self, path):
        """
        Update the feedstock outputs in the database.

        Args:
            path (str): Path to the feedstock outputs directory.
        """
        session = self.Session()
        feedstock_outputs.update(session, path=Path(path))
        session.commit()

    def update_artifacts(self, path):
        """
        Update the artifacts in the database.
        """
        session = self.Session()
        artifacts.update(session, path=Path(path))
        session.commit()

    def update_import_to_package_maps(self, path):
        """
        Update the import to package maps in the database.

        Args:
            path (str): Path to the import to package maps directory.
        """
        session = self.Session()
        import_to_package_maps.update(session, path=Path(path))
        session.commit()


class OrderCommands(TyperGroup):
    def list_commands(self, ctx: Context):
        """Return list of commands in the order appear."""
        return list(self.commands)


app = typer.Typer(
    cls=OrderCommands,
    help="CFDB is a tool to manage the Conda Forge database.",
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.command()
def update_feedstock_outputs(
    path: str = typer.Option(
        ..., "--path", "-p", help="Path to the feedstock outputs directory."
    )
):
    """
    Update the feedstock outputs in the database based on the local path to the feedstock outputs cloned from Conda Forge. Path to the feedstock outputs directory. The path should point to the 'outputs' folder inside the 'feedstock-outputs' root directory.


    Example:
        To update the feedstock outputs, use the following command:
        $ cfdb update_feedstock_outputs --path /path/to/feedstock-outputs/outputs
    """
    db_handler = CFDBHandler("sqlite:///cf-database.db")
    db_handler.update_feedstock_outputs(path)


@app.command()
def update_import_to_package_maps(
    path: str = typer.Option(
        ..., "--path", "-p", help="Path to the import to package maps directory."
    )
):
    """
    Update the import to package maps in the database based on the local path to the
    import to package maps cloned from conda-forge. Path to the import to package
    maps directory. The path should point to the 'import_to_package_maps' folder
    inside the 'libcfgraph' root directory or any viable alternative.

    Example:
        To update the import to package maps, use the following command:
        $ cfdb update_import_to_package_maps --path /path/to/libcfgraph/import_to_package_maps
    """
    db_handler = CFDBHandler("sqlite:///cf-database.db")
    db_handler.update_import_to_package_maps(path)


@app.command()
def update_artifacts(
    path: str = typer.Option(
        ..., "--path", "-p", help="Path to the artifacts directory."
    )
):
    """
    Update the artifacts in the database.
    """
    db_handler = CFDBHandler("sqlite:///cf-database.db")
    db_handler.update_artifacts(path)


if __name__ == "__main__":
    app()
