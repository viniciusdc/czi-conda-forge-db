import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cfdb.models.schema import Base
from cfdb.populate import (
    artifacts,
    feedstock_outputs,
)
from cfdb.log import logger
from pathlib import Path


class CFDBHandler:
    def __init__(self, db_url):
        self.db_url = db_url
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def update_feedstock_outputs(self, path):
        session = self.Session()
        feedstock_outputs.update(session, path=Path(path))
        session.commit()

    def update_artifacts(self):
        session = self.Session()
        artifacts.update(session)
        session.commit()


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--path",
    type=click.Path(exists=True),
    required=True,
    help="Path to feedstock outputs",
)
def update_feedstock_outputs(path):
    db_handler = CFDBHandler("sqlite:///cf-database.db")
    db_handler.update_feedstock_outputs(path)


@cli.command()
def update_artifacts():
    db_handler = CFDBHandler("sqlite:///cf-database.db")
    db_handler.update_artifacts()


def run():
    cli()


if __name__ == "__main__":
    run()
