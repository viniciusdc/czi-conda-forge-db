import uuid
from sqlalchemy import LargeBinary, Index

from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base

try:
    from sqlalchemy.orm import DeclarativeBase  # type: ignore

    class Base(DeclarativeBase):
        pass

except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

UUID = LargeBinary(length=16)


def uniq_id():
    _uid = uuid.uuid3(uuid.uuid1(), uuid.uuid4().hex)
    return _uid.bytes


class Feedstocks(Base):
    """
    Feedstocks are the source of the packages.

    attributes:
        name: str(index) - primary key
        package_name: str - foreign key to packages
    """

    __tablename__ = "feedstocks"
    name = Column(String, index=True, primary_key=True)
    # package_name = Column(Integer, ForeignKey("packages.name"))

    def __repr__(self):
        return f"<Feedstock(name={self.name})>"


class Packages(Base):
    """
    Packages are the artifacts that are built from the feedstocks.

    attributes:
        name: str(index) - primary key
    """

    __tablename__ = "packages"
    name = Column(String, index=True, primary_key=True)
    # artifact_id = Column(Integer, ForeignKey("artifacts.id"))

    def __repr__(self):
        return f"<Package(name={self.name})>"


class FeedstockOutputs(Base):
    """
    Feedstock outputs are the files mappings from the feedstocks to the packages.

    attributes:
        id: UUID(index) - primary key
        path: str
        feedstock_name: str - foreign key to feedstocks
        package_name: str - foreign key to packages
        hash: str
    """

    __tablename__ = "feedstock_outputs"

    id = Column(UUID, primary_key=True)
    path = Column(String)
    feedstock_name = Column(Integer, ForeignKey("feedstocks.name"))
    package_name = Column(Integer, ForeignKey("packages.name"))
    hash = Column(String)


Index(
    "feedstock_output_index",
    FeedstockOutputs.feedstock_name,
    FeedstockOutputs.package_name,
    unique=True,
)


class Artifacts(Base):
    __tablename__ = "artifacts"
    name = Column(String, primary_key=True, index=True)
    package_name = Column(String, ForeignKey("packages.name"))
    platform = Column(String, primary_key=True)
    version = Column(String)
    relational_id = Column(Integer, ForeignKey("relations_map_file_paths.id"))

    def __repr__(self):
        return f"<Artifact(name={self.name}, platform={self.platform}, version={self.version})>"


class ArtifactsFilePaths(Base):
    __tablename__ = "artifacts_file_paths"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer)
    dir = Column(String)


class RelationsMapFilePaths(Base):
    __tablename__ = "relations_map_file_paths"
    id = Column(Integer, primary_key=True)
    file_path = Column(String, ForeignKey("artifacts_file_paths.id"))


if __name__ == "__main__":
    from eralchemy import render_er

    ## Draw from SQLAlchemy base
    render_er(Base, "erd_cf.png")
