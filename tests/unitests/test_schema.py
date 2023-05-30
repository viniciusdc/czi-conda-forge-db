import pytest
from cfdb.models.schema import (
    Artifacts,
    ArtifactsFilePaths,
    FeedstockOutputs,
    Feedstocks,
    Packages,
    RelationsMapFilePaths,
)


@pytest.fixture
def sample_feedstock() -> Feedstocks:
    return Feedstocks(name="feedstock_1")


@pytest.fixture
def sample_package() -> Packages:
    return Packages(name="package_1")


@pytest.fixture
def sample_feedstock_output(sample_feedstock, sample_package) -> FeedstockOutputs:
    return FeedstockOutputs(
        id="output_1",
        feedstock_name=sample_feedstock.name,
        package_name=sample_package.name,
        hash="123456",
    )


@pytest.fixture
def sample_artifact() -> Artifacts:
    return Artifacts(
        name="artifact_1", platform="linux", version="1.0", package_name="package_1"
    )


@pytest.fixture
def sample_artifacts_file_path() -> ArtifactsFilePaths:
    return ArtifactsFilePaths(id=1, parent_id=2, dir="/path/to/artifact")


@pytest.fixture
def sample_relations_map_file_path() -> RelationsMapFilePaths:
    return RelationsMapFilePaths(id=1, file_path=2)


def test_feedstocks(sample_feedstock):
    assert sample_feedstock.name == "feedstock_1"


def test_packages(sample_package):
    assert sample_package.name == "package_1"


def test_feedstock_outputs(sample_feedstock_output, sample_feedstock, sample_package):
    assert sample_feedstock_output.id == "output_1"
    assert sample_feedstock_output.feedstock_name == sample_feedstock.name
    assert sample_feedstock_output.package_name == sample_package.name
    assert sample_feedstock_output.hash == "123456"


def test_artifacts(sample_artifact):
    assert sample_artifact.name == "artifact_1"
    assert sample_artifact.platform == "linux"
    assert sample_artifact.version == "1.0"
    assert sample_artifact.package_name == "package_1"


def test_artifacts_file_paths(sample_artifacts_file_path):
    assert sample_artifacts_file_path.id == 1
    assert sample_artifacts_file_path.parent_id == 2
    assert sample_artifacts_file_path.dir == "/path/to/artifact"


def test_relations_map_file_paths(sample_relations_map_file_path):
    assert sample_relations_map_file_path.id == 1
    assert sample_relations_map_file_path.file_path == 2
