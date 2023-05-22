import typer
from typer.core import TyperGroup
from click import Context
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cfdb.models.schema import Base
from cfdb.populate import artifacts, feedstock_outputs
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

    def update_artifacts(self):
        """
        Update the artifacts in the database.
        """
        session = self.Session()
        artifacts.update(session)
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
    Update the feedstock outputs in the database.
    """
    db_handler = CFDBHandler("sqlite:///cf-database.db")
    db_handler.update_feedstock_outputs(path)


@app.command()
def update_artifacts():
    """
    Update the artifacts in the database.
    """
    db_handler = CFDBHandler("sqlite:///cf-database.db")
    db_handler.update_artifacts()


if __name__ == "__main__":
    app()
