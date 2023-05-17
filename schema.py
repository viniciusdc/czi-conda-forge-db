import uuid
from sqlalchemy import LargeBinary

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base

try:
    from sqlalchemy.orm import DeclarativeBase  # type: ignore

    class Base(DeclarativeBase):
        pass

except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

UUID = LargeBinary(length=16)


class Feedstocks(Base):
    __tablename__ = "feedstocks"
    id = Column(UUID, primary_key=True, index=True, default=uuid.uuid4)
    name = Column(String, index=True)
    package_id = Column(Integer, ForeignKey("packages.id"))

    def __repr__(self):
        return f"<Feedstock(name={self.name})>"


class Packages(Base):
    __tablename__ = "packages"
    id = Column(UUID, primary_key=True, index=True, default=uuid.uuid4)
    name = Column(String, index=True)
    artifact_id = Column(Integer, ForeignKey("artifacts.id"))

    def __repr__(self):
        return f"<Package(name={self.name})>"


class FeedstockOutputs(Base):
    __tablename__ = "feedstock_outputs"

    id = Column(UUID, primary_key=True, index=True, default=uuid.uuid4)
    path = Column(String)
    feedstock_id = Column(Integer, ForeignKey("feedstocks.id"))
    package_id = Column(Integer, ForeignKey("packages.id"))


class Artifacts(Base):
    __tablename__ = "artifacts"
    id = Column(UUID, primary_key=True, index=True, default=uuid.uuid4)
    name = Column(String, primary_key=True)
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
