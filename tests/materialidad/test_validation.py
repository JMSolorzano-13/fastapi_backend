"""Tests for file name validation security."""

import pytest
from chalice import BadRequestError

from chalicelib.blueprints.attachment import validate_file_name


class TestValidateFileName:
    """Test suite for file name validation to prevent path traversal and injection attacks."""

    def test_valid_file_names(self):
        """Test that valid file names pass validation."""
        valid_names = [
            "document.pdf",
            "file_name.txt",
            "my-file_2024.docx",
            "file.with.dots.pdf",
            "simple.txt",
            "file-with-dashes.pdf",
            "file_with_underscores.txt",
            "file123.pdf",
            "FILE.PDF",
        ]

        for file_name in valid_names:
            # Should not raise any exception
            result = validate_file_name(file_name)
            assert result == file_name

    def test_path_traversal_attacks(self):
        """Test that path traversal attempts are rejected."""
        invalid_names = [
            "../../../etc/passwd",
            "../../passwd",
            "file/../other.txt",
            "../file.txt",
            "file/../../etc/passwd",
        ]

        for file_name in invalid_names:
            with pytest.raises(BadRequestError, match="path separator"):
                validate_file_name(file_name)

    def test_forward_slash_rejected(self):
        """Test that file names with forward slashes are rejected."""
        with pytest.raises(BadRequestError, match="path separator"):
            validate_file_name("file/name.txt")

        with pytest.raises(BadRequestError, match="path separator"):
            validate_file_name("path/to/file.txt")

    def test_backslash_rejected(self):
        """Test that file names with backslashes are rejected."""
        with pytest.raises(BadRequestError, match="path separator"):
            validate_file_name("file\\name.txt")

        with pytest.raises(BadRequestError, match="path separator"):
            validate_file_name("path\\to\\file.txt")

    def test_empty_string_rejected(self):
        """Test that empty file name is rejected."""
        with pytest.raises(BadRequestError, match="cannot be empty"):
            validate_file_name("")

    def test_null_byte_rejected(self):
        """Test that file names with null bytes are rejected."""
        with pytest.raises(BadRequestError, match="null byte"):
            validate_file_name("\x00file.txt")

        with pytest.raises(BadRequestError, match="null byte"):
            validate_file_name("file\x00.txt")

        with pytest.raises(BadRequestError, match="null byte"):
            validate_file_name("file.txt\x00")

    def test_current_directory_rejected(self):
        """Test that '.' is rejected."""
        with pytest.raises(BadRequestError, match="cannot be '.' or '..'"):
            validate_file_name(".")

    def test_parent_directory_rejected(self):
        """Test that '..' is rejected."""
        with pytest.raises(BadRequestError, match="cannot be '.' or '..'"):
            validate_file_name("..")

    def test_hidden_files_allowed(self):
        """Test that hidden files (starting with .) are allowed."""
        result = validate_file_name(".hidden_file")
        assert result == ".hidden_file"

        result = validate_file_name(".gitignore")
        assert result == ".gitignore"
