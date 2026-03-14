"""Unit tests for S3 DirectoryBucket / S3 Express One Zone support."""

import datetime
import uuid

import pytest
from moto.s3.models import FakeBucket, s3_backends


def _uid() -> str:
    """Short unique suffix to avoid global bucket namespace collisions."""
    return uuid.uuid4().hex[:8]


def _dir_name(prefix: str = "db") -> str:
    """Generate a unique directory bucket name."""
    return f"{prefix}-{_uid()}--use1-az5--x-s3"


@pytest.fixture
def s3_backend():
    """Get a fresh S3 backend for testing."""
    backend = s3_backends["123456789012"]["global"]
    initial_buckets = set(backend.buckets.keys())
    yield backend
    for name in list(backend.buckets.keys()):
        if name not in initial_buckets:
            bucket = backend.buckets[name]
            for key in list(bucket.keys.keys()):
                del bucket.keys[key]
            backend.delete_bucket(name)


class TestFakeBucketDirectoryFields:
    """Tests for FakeBucket directory bucket metadata fields."""

    def test_default_bucket_is_not_directory(self):
        """Regular buckets have bucket_type=None and is_directory_bucket=False."""
        bucket = FakeBucket(f"reg-{_uid()}", "123456789012", "us-east-1")
        assert bucket.bucket_type is None
        assert bucket.is_directory_bucket is False
        assert bucket.location_type is None
        assert bucket.location_name is None
        assert bucket.data_redundancy is None

    def test_regular_bucket_arn_format(self):
        """Regular buckets use arn:aws:s3:::name format."""
        name = f"arn-{_uid()}"
        bucket = FakeBucket(name, "123456789012", "us-east-1")
        assert bucket.arn == f"arn:aws:s3:::{name}"

    def test_directory_bucket_fields(self):
        """Directory buckets store type, location, and redundancy metadata."""
        bucket = FakeBucket(_dir_name(), "123456789012", "us-east-1")
        bucket.bucket_type = "Directory"
        bucket.location_type = "AvailabilityZone"
        bucket.location_name = "use1-az5"
        bucket.data_redundancy = "SingleAvailabilityZone"

        assert bucket.is_directory_bucket is True
        assert bucket.bucket_type == "Directory"
        assert bucket.location_type == "AvailabilityZone"
        assert bucket.location_name == "use1-az5"
        assert bucket.data_redundancy == "SingleAvailabilityZone"

    def test_directory_bucket_arn_format(self):
        """Directory buckets use arn:aws:s3express:region:account:bucket/name format."""
        name = _dir_name()
        bucket = FakeBucket(name, "123456789012", "us-east-1")
        bucket.bucket_type = "Directory"
        assert bucket.arn == f"arn:aws:s3express:us-east-1:123456789012:bucket/{name}"


class TestCreateDirectoryBucket:
    """Tests for creating directory buckets via the S3 backend."""

    def test_create_directory_bucket(self, s3_backend):
        """Creating a directory bucket stores all metadata."""
        bucket = s3_backend.create_bucket(
            _dir_name(),
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
            data_redundancy="SingleAvailabilityZone",
        )
        assert bucket.is_directory_bucket
        assert bucket.location_type == "AvailabilityZone"
        assert bucket.location_name == "use1-az5"
        assert bucket.data_redundancy == "SingleAvailabilityZone"

    def test_create_regular_bucket_still_works(self, s3_backend):
        """Regular bucket creation is unaffected by directory bucket support."""
        bucket = s3_backend.create_bucket(f"normal-{_uid()}", "us-east-1")
        assert not bucket.is_directory_bucket
        assert bucket.bucket_type is None

    def test_directory_bucket_name_validation(self, s3_backend):
        """Directory bucket names must end with --<az>--x-s3."""
        bucket = s3_backend.create_bucket(
            _dir_name("valid"),
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        assert bucket.is_directory_bucket

    def test_create_multiple_directory_buckets(self, s3_backend):
        """Can create multiple directory buckets in the same backend."""
        b1 = s3_backend.create_bucket(
            _dir_name("first"),
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        b2 = s3_backend.create_bucket(
            _dir_name("second"),
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az6",
        )
        assert b1.is_directory_bucket
        assert b2.is_directory_bucket
        assert b1.name != b2.name


class TestListDirectoryBuckets:
    """Tests for listing directory buckets (excludes regular buckets)."""

    def test_list_excludes_regular_buckets(self, s3_backend):
        """Only directory buckets are returned, not regular buckets."""
        s3_backend.create_bucket(f"regular-{_uid()}", "us-east-1")
        dir_name = _dir_name()
        s3_backend.create_bucket(
            dir_name,
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        buckets, _token = s3_backend.list_directory_buckets()
        dir_names = [b.name for b in buckets]
        assert dir_name in dir_names
        # Regular buckets should not be in the list
        for b in buckets:
            assert b.is_directory_bucket

    def test_list_multiple_directory_buckets(self, s3_backend):
        """Returns all directory buckets."""
        created = []
        for _i in range(3):
            name = _dir_name()
            s3_backend.create_bucket(
                name,
                "us-east-1",
                bucket_type="Directory",
                location_type="AvailabilityZone",
                location_name="use1-az5",
            )
            created.append(name)
        buckets, _token = s3_backend.list_directory_buckets()
        bucket_names = {b.name for b in buckets}
        for name in created:
            assert name in bucket_names

    def test_list_pagination(self, s3_backend):
        """Pagination works with continuation tokens."""
        for i in range(5):
            s3_backend.create_bucket(
                f"page{i:02d}-{_uid()}--use1-az5--x-s3",
                "us-east-1",
                bucket_type="Directory",
                location_type="AvailabilityZone",
                location_name="use1-az5",
            )
        buckets, token = s3_backend.list_directory_buckets(max_buckets=2)
        assert len(buckets) <= 2
        # If there are more than 2 directory buckets total, we should get a token
        all_dir, _ = s3_backend.list_directory_buckets(max_buckets=1000)
        if len(all_dir) > 2:
            assert token is not None


class TestCreateSession:
    """Tests for S3 Express One Zone CreateSession."""

    def test_create_session_returns_credentials(self, s3_backend):
        """CreateSession returns valid-looking credentials."""
        name = _dir_name("session")
        s3_backend.create_bucket(
            name,
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        result = s3_backend.create_session(name)
        assert "Credentials" in result
        creds = result["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds
        assert "Expiration" in creds
        assert creds["AccessKeyId"].startswith("ASIA")

    def test_create_session_with_read_only_mode(self, s3_backend):
        """SessionMode parameter is accepted."""
        name = _dir_name("ro")
        s3_backend.create_bucket(
            name,
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        result = s3_backend.create_session(name, session_mode="ReadOnly")
        assert "Credentials" in result

    def test_create_session_on_regular_bucket_fails(self, s3_backend):
        """CreateSession should fail on non-directory buckets."""
        name = f"regular-{_uid()}"
        s3_backend.create_bucket(name, "us-east-1")
        with pytest.raises(Exception):
            s3_backend.create_session(name)

    def test_create_session_nonexistent_bucket_fails(self, s3_backend):
        """CreateSession on nonexistent bucket raises error."""
        with pytest.raises(Exception):
            s3_backend.create_session(f"nonexistent-{_uid()}--use1-az5--x-s3")

    def test_create_session_credentials_expire_in_future(self, s3_backend):
        """Session credentials should expire in the future."""
        name = _dir_name("future")
        s3_backend.create_bucket(
            name,
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        result = s3_backend.create_session(name)
        expiration = result["Credentials"]["Expiration"]
        if isinstance(expiration, str):
            exp_dt = datetime.datetime.fromisoformat(expiration)
        else:
            exp_dt = expiration
        if exp_dt.tzinfo is None:
            exp_dt = exp_dt.replace(tzinfo=datetime.UTC)
        assert exp_dt > datetime.datetime.now(tz=datetime.UTC)


class TestRenameObject:
    """Tests for RenameObject (directory buckets only)."""

    def _create_dir_bucket_with_object(self, s3_backend, key="old-key", body=b"hello"):
        """Helper to create a directory bucket with an object."""
        name = _dir_name("rename")
        s3_backend.create_bucket(
            name,
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        s3_backend.put_object(name, key, body)
        return name

    def test_rename_object(self, s3_backend):
        """Renaming moves object to new key and removes old key."""
        bname = self._create_dir_bucket_with_object(s3_backend)
        s3_backend.rename_object(bname, "new-key", f"{bname}/old-key")

        # New key should exist
        obj = s3_backend.get_object(bname, "new-key")
        assert obj is not None

        # Old key should be gone from bucket keys
        bucket = s3_backend.get_bucket(bname)
        assert "old-key" not in bucket.keys

    def test_rename_preserves_content(self, s3_backend):
        """Renamed object retains its content."""
        bname = self._create_dir_bucket_with_object(s3_backend, body=b"test content")
        s3_backend.rename_object(bname, "renamed", f"{bname}/old-key")
        obj = s3_backend.get_object(bname, "renamed")
        assert obj.value == b"test content"

    def test_rename_nonexistent_source_fails(self, s3_backend):
        """Renaming a nonexistent object raises an error."""
        name = _dir_name("empty")
        s3_backend.create_bucket(
            name,
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        with pytest.raises(Exception):
            s3_backend.rename_object(name, "new", f"{name}/nonexistent")

    def test_rename_on_regular_bucket_fails(self, s3_backend):
        """Rename is only for directory buckets."""
        name = f"reg-{_uid()}"
        s3_backend.create_bucket(name, "us-east-1")
        s3_backend.put_object(name, "obj", b"data")
        with pytest.raises(Exception):
            s3_backend.rename_object(name, "new-obj", f"{name}/obj")


class TestAppendWrites:
    """Tests for append writes via WriteOffsetBytes (directory buckets only)."""

    def _make_dir_bucket(self, s3_backend):
        """Helper to create a directory bucket."""
        name = _dir_name("append")
        s3_backend.create_bucket(
            name,
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        return name

    def test_first_write_at_offset_zero(self, s3_backend):
        """First write with offset 0 creates the object."""
        bname = self._make_dir_bucket(s3_backend)
        s3_backend.put_object(bname, "file.txt", b"hello", write_offset_bytes=0)
        obj = s3_backend.get_object(bname, "file.txt")
        assert obj.value == b"hello"

    def test_append_at_correct_offset(self, s3_backend):
        """Append at the correct offset extends the object."""
        bname = self._make_dir_bucket(s3_backend)
        s3_backend.put_object(bname, "file.txt", b"hello", write_offset_bytes=0)
        s3_backend.put_object(bname, "file.txt", b" world", write_offset_bytes=5)
        obj = s3_backend.get_object(bname, "file.txt")
        assert obj.value == b"hello world"

    def test_multiple_appends(self, s3_backend):
        """Multiple sequential appends work correctly."""
        bname = self._make_dir_bucket(s3_backend)
        s3_backend.put_object(bname, "log.txt", b"line1\n", write_offset_bytes=0)
        s3_backend.put_object(bname, "log.txt", b"line2\n", write_offset_bytes=6)
        s3_backend.put_object(bname, "log.txt", b"line3\n", write_offset_bytes=12)
        obj = s3_backend.get_object(bname, "log.txt")
        assert obj.value == b"line1\nline2\nline3\n"

    def test_append_wrong_offset_fails(self, s3_backend):
        """Append at wrong offset raises error."""
        bname = self._make_dir_bucket(s3_backend)
        s3_backend.put_object(bname, "file.txt", b"hello", write_offset_bytes=0)
        with pytest.raises(Exception):
            s3_backend.put_object(bname, "file.txt", b" world", write_offset_bytes=99)

    def test_first_write_nonzero_offset_fails(self, s3_backend):
        """First write to nonexistent object with non-zero offset fails."""
        bname = self._make_dir_bucket(s3_backend)
        with pytest.raises(Exception):
            s3_backend.put_object(bname, "new.txt", b"data", write_offset_bytes=10)


class TestHeadBucket:
    """Tests for HeadBucket with directory bucket metadata."""

    def test_head_regular_bucket(self, s3_backend):
        """HeadBucket on regular bucket returns the bucket."""
        name = f"regular-{_uid()}"
        s3_backend.create_bucket(name, "us-east-1")
        bucket = s3_backend.head_bucket(name)
        assert bucket.name == name
        assert not bucket.is_directory_bucket

    def test_head_directory_bucket(self, s3_backend):
        """HeadBucket on directory bucket returns location metadata."""
        name = _dir_name("head")
        s3_backend.create_bucket(
            name,
            "us-east-1",
            bucket_type="Directory",
            location_type="AvailabilityZone",
            location_name="use1-az5",
        )
        bucket = s3_backend.head_bucket(name)
        assert bucket.is_directory_bucket
        assert bucket.location_type == "AvailabilityZone"
        assert bucket.location_name == "use1-az5"
