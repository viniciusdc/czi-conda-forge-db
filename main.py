from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from schema import Base
from populate import (
    artifacts,
    feedstock_outputs,
)
from log import logger
from pathlib import Path


def update(session):
    feedstock_outputs.update(
        session,
        path=Path("/home/vinicius/Conda-Forge") / "feedstock-outputs" / "outputs",
    )
    session.commit()

    artifacts.update(session)
    session.commit()


def run():
    engine = create_engine("sqlite:///cf-database.db")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    update(session)


if __name__ == "__main__":
    run()
