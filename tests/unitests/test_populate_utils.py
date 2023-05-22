import hashlib
import json
import tempfile
from pathlib import Path

import pytest

from cfdb.populate.utils import (
    hash_file,
    process_batch,
    retrieve_associated_feedstock_from_output_blob,
)


def test_hash_file():
    # Create a temporary file with some content
    with tempfile.NamedTemporaryFile() as temp_file:
        content = b"Hello, World!"
        temp_file.write(content)
        temp_file.seek(0)

        # Calculate the expected hash using hashlib
        expected_hash = hashlib.sha1(content).hexdigest()

        # Calculate the hash using the hash_file function
        actual_hash = hash_file(temp_file.name)

        # Assert that the actual hash matches the expected hash
        assert actual_hash == expected_hash


def test_process_batch():
    # Create a temporary directory and some sample files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        files = [
            temp_dir / "file1.txt",
            temp_dir / "file2.txt",
            temp_dir / "file3.txt",
        ]
        for file in files:
            file.write_text("Test file")

        # Create a temporary file to store the results
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file_path = Path(temp_file.name)

            # Process the batch of files
            process_batch(files, temp_file_path)

            # Read the contents of the temporary file
            contents = temp_file_path.read_text()

            # Assert that each file in the batch is listed along with its hash in the contents
            for file in files:
                file_hash = hash_file(file)
                assert f"{file},{file_hash}\n" in contents


def test_retrieve_associated_feedstock_from_output_blob():
    # Create a temporary file with a JSON payload
    with tempfile.NamedTemporaryFile() as temp_file:
        payload = {
            "feedstocks": ["feedstock1", "feedstock2"],
        }

        temp_file.write(json.dumps(payload).encode())
        temp_file.seek(0)

        # Retrieve the associated feedstocks from the temporary file
        associated_feedstocks = retrieve_associated_feedstock_from_output_blob(
            temp_file.name
        )

        # Assert that the retrieved feedstocks match the expected feedstocks
        expected_feedstocks = ["feedstock1", "feedstock2"]
        assert associated_feedstocks == expected_feedstocks
