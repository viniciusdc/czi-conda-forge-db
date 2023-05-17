from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from schema import Base
from populate import (
    artifacts,
    feedstock_outputs,
)


def update(session):
    print("Updating feedstocks...")
    feedstock_outputs.update(session)
    session.commit()

    print("Updating packages...")
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
