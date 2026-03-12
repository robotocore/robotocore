"""Tests for media asset management."""


class TestUploadMedia:
    """Uploading and reading media assets."""

    def test_upload_media_metadata(self, cms):
        asset = cms.upload_media(
            data=b"png-data-here",
            filename="logo.png",
            content_type="image/png",
            alt_text="Company logo",
            tags=["branding", "logo"],
        )
        assert asset.asset_id
        assert asset.key.startswith("media/")
        assert asset.key.endswith("logo.png")
        assert asset.content_type == "image/png"
        assert asset.size == len(b"png-data-here")
        assert asset.alt_text == "Company logo"
        assert set(asset.tags) == {"branding", "logo"}

    def test_upload_media_read_back(self, cms):
        asset = cms.upload_media(
            data=b"jpeg-bytes",
            filename="photo.jpg",
            alt_text="A photo",
        )
        fetched = cms.get_media(asset.asset_id)
        assert fetched is not None
        assert fetched.asset_id == asset.asset_id
        assert fetched.alt_text == "A photo"

    def test_upload_media_download_data(self, cms):
        original_data = b"this-is-the-file-content"
        asset = cms.upload_media(
            data=original_data,
            filename="document.pdf",
            content_type="application/pdf",
        )
        downloaded = cms.get_media_data(asset)
        assert downloaded == original_data


class TestListMedia:
    """Listing media assets."""

    def test_list_media_library(self, cms):
        cms.upload_media(data=b"a", filename="a.jpg")
        cms.upload_media(data=b"b", filename="b.jpg")
        cms.upload_media(data=b"c", filename="c.jpg")
        media_list = cms.list_media()
        assert len(media_list) >= 3
        filenames = {m.key.split("/")[-1] for m in media_list}
        assert "a.jpg" in filenames
        assert "b.jpg" in filenames
        assert "c.jpg" in filenames


class TestMediaVersioning:
    """S3 versioning for media assets."""

    def test_reupload_creates_versions(self, cms, sample_media):
        cms.reupload_media(sample_media, b"version-2-data")
        cms.reupload_media(sample_media, b"version-3-data")
        versions = cms.media_versions(sample_media)
        assert len(versions) >= 3

    def test_latest_version_is_newest(self, cms, sample_media):
        cms.reupload_media(sample_media, b"final-version")
        data = cms.get_media_data(sample_media)
        assert data == b"final-version"


class TestDeleteMedia:
    """Deleting media assets."""

    def test_delete_media_removes_metadata(self, cms, sample_media):
        asset_id = sample_media.asset_id
        cms.delete_media(asset_id, actor="admin")
        assert cms.get_media(asset_id) is None

    def test_delete_media_removes_s3_object(self, cms):
        asset = cms.upload_media(data=b"to-delete", filename="temp.jpg")
        cms.delete_media(asset.asset_id)
        # S3 object should be gone (or have a delete marker with versioning)
        import pytest

        with pytest.raises(Exception):
            cms.get_media_data(asset)
