"""Unit tests for EC2 capacity profile management."""

from robotocore.services.ec2.capacity import (
    CapacityProfile,
    CapacityStore,
    SpotRequestState,
)


class TestCapacityProfile:
    """Tests for CapacityProfile dataclass."""

    def test_capacity_profile_creation(self):
        """Test creating a capacity profile."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=5,
        )
        assert profile.instance_type == "g5.xlarge"
        assert profile.availability_zone == "us-east-1a"
        assert profile.total_capacity == 10
        assert profile.available_capacity == 5
        assert profile.spot_available is True
        assert profile.spot_price == 0.05
        assert profile.enabled is True

    def test_capacity_profile_to_dict(self):
        """Test converting capacity profile to dict."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=5,
            spot_available=False,
            spot_price=0.10,
            enabled=False,
        )
        data = profile.to_dict()
        assert data["instance_type"] == "g5.xlarge"
        assert data["availability_zone"] == "us-east-1a"
        assert data["total_capacity"] == 10
        assert data["available_capacity"] == 5
        assert data["spot_available"] is False
        assert data["spot_price"] == 0.10
        assert data["enabled"] is False

    def test_capacity_profile_from_dict(self):
        """Test creating capacity profile from dict."""
        data = {
            "instance_type": "g5.xlarge",
            "availability_zone": "us-east-1a",
            "total_capacity": 10,
            "available_capacity": 5,
            "spot_available": False,
            "spot_price": 0.10,
            "enabled": False,
        }
        profile = CapacityProfile.from_dict(data)
        assert profile.instance_type == "g5.xlarge"
        assert profile.availability_zone == "us-east-1a"
        assert profile.total_capacity == 10
        assert profile.available_capacity == 5
        assert profile.spot_available is False
        assert profile.spot_price == 0.10
        assert profile.enabled is False

    def test_capacity_profile_post_init_adjusts_available(self):
        """Test that available_capacity is capped at total_capacity."""
        profile = CapacityProfile(
            instance_type="t2.micro",
            availability_zone="us-east-1a",
            total_capacity=5,
            available_capacity=10,  # More than total
        )
        assert profile.available_capacity == 5


class TestSpotRequestState:
    """Tests for SpotRequestState dataclass."""

    def test_spot_request_creation(self):
        """Test creating a spot request state."""
        request = SpotRequestState(
            request_id="sir-1234567890abcdef0",
            instance_type="t2.micro",
            availability_zone="us-east-1a",
        )
        assert request.request_id == "sir-1234567890abcdef0"
        assert request.instance_type == "t2.micro"
        assert request.availability_zone == "us-east-1a"
        assert request.state == "open"
        assert request.status == "pending-evaluation"
        assert request.instance_id is None

    def test_spot_request_to_dict(self):
        """Test converting spot request to dict."""
        request = SpotRequestState(
            request_id="sir-1234567890abcdef0",
            instance_type="t2.micro",
            availability_zone="us-east-1a",
            state="active",
            status="fulfilled",
            instance_id="i-1234567890abcdef0",
        )
        data = request.to_dict()
        assert data["request_id"] == "sir-1234567890abcdef0"
        assert data["instance_type"] == "t2.micro"
        assert data["availability_zone"] == "us-east-1a"
        assert data["state"] == "active"
        assert data["status"] == "fulfilled"
        assert data["instance_id"] == "i-1234567890abcdef0"

    def test_spot_request_from_dict(self):
        """Test creating spot request from dict."""
        data = {
            "request_id": "sir-1234567890abcdef0",
            "instance_type": "t2.micro",
            "availability_zone": "us-east-1a",
            "state": "active",
            "status": "fulfilled",
            "status_message": "Your Spot request is fulfilled.",
            "instance_id": "i-1234567890abcdef0",
            "created_at": 1234567890.0,
        }
        request = SpotRequestState.from_dict(data)
        assert request.request_id == "sir-1234567890abcdef0"
        assert request.instance_type == "t2.micro"
        assert request.availability_zone == "us-east-1a"
        assert request.state == "active"
        assert request.status == "fulfilled"
        assert request.status_message == "Your Spot request is fulfilled."
        assert request.instance_id == "i-1234567890abcdef0"
        assert request.created_at == 1234567890.0


class TestCapacityStore:
    """Tests for CapacityStore."""

    def setup_method(self):
        """Set up a fresh capacity store for each test."""
        self.store = CapacityStore()

    def test_set_and_get_profile(self):
        """Test setting and getting a capacity profile."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        retrieved = self.store.get_profile("123456789012", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved is not None
        assert retrieved.instance_type == "g5.xlarge"
        assert retrieved.total_capacity == 10

    def test_get_profile_not_found(self):
        """Test getting a profile that doesn't exist."""
        retrieved = self.store.get_profile("123456789012", "us-east-1", "unknown", "us-east-1a")
        assert retrieved is None

    def test_delete_profile(self):
        """Test deleting a capacity profile."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        deleted = self.store.delete_profile("123456789012", "us-east-1", "g5.xlarge", "us-east-1a")
        assert deleted is True

        retrieved = self.store.get_profile("123456789012", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved is None

    def test_delete_profile_not_found(self):
        """Test deleting a profile that doesn't exist."""
        deleted = self.store.delete_profile("123456789012", "us-east-1", "unknown", "us-east-1a")
        assert deleted is False

    def test_list_profiles(self):
        """Test listing capacity profiles."""
        profile1 = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
        )
        profile2 = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1b",
            total_capacity=5,
        )
        self.store.set_profile("123456789012", "us-east-1", profile1)
        self.store.set_profile("123456789012", "us-east-1", profile2)

        profiles = self.store.list_profiles("123456789012", "us-east-1")
        assert len(profiles) == 2

    def test_consume_capacity(self):
        """Test consuming capacity."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=10,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        success = self.store.consume_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 3
        )
        assert success is True

        retrieved = self.store.get_profile("123456789012", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved.available_capacity == 7

    def test_consume_capacity_insufficient(self):
        """Test consuming more capacity than available."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=5,
            available_capacity=2,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        success = self.store.consume_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 5
        )
        assert success is False

    def test_release_capacity(self):
        """Test releasing capacity."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=5,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        self.store.release_capacity("123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 2)

        retrieved = self.store.get_profile("123456789012", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved.available_capacity == 7

    def test_release_capacity_capped_at_total(self):
        """Test that releasing capacity doesn't exceed total."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=9,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        self.store.release_capacity("123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 5)

        retrieved = self.store.get_profile("123456789012", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved.available_capacity == 10

    def test_check_capacity_sufficient(self):
        """Test checking capacity when sufficient."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=5,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        success, error = self.store.check_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 3
        )
        assert success is True
        assert error == ""

    def test_check_capacity_insufficient(self):
        """Test checking capacity when insufficient."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=2,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        success, error = self.store.check_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 5
        )
        assert success is False
        assert error == "InsufficientInstanceCapacity"

    def test_check_capacity_disabled(self):
        """Test checking capacity when offering is disabled."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=10,
            enabled=False,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        success, error = self.store.check_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 1
        )
        assert success is False
        assert error == "Unsupported"

    def test_check_spot_capacity_available(self):
        """Test checking spot capacity when available."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=5,
            spot_available=True,
            spot_price=0.10,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        available, price = self.store.check_spot_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a"
        )
        assert available is True
        assert price == 0.10

    def test_check_spot_capacity_not_available(self):
        """Test checking spot capacity when not available."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=5,
            spot_available=False,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        available, price = self.store.check_spot_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a"
        )
        assert available is False
        assert price is None

    def test_check_spot_capacity_no_capacity(self):
        """Test checking spot capacity when no capacity left."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=0,
            spot_available=True,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        available, price = self.store.check_spot_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a"
        )
        assert available is False
        assert price is None

    def test_chaos_override_insufficient(self):
        """Test chaos override for InsufficientInstanceCapacity."""
        self.store.set_chaos_override({"error_code": "InsufficientInstanceCapacity"})

        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=100,
            available_capacity=100,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        success, error = self.store.check_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 1
        )
        assert success is False
        assert error == "InsufficientInstanceCapacity"

    def test_chaos_override_unsupported(self):
        """Test chaos override for Unsupported."""
        self.store.set_chaos_override({"error_code": "Unsupported"})

        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=100,
            available_capacity=100,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        success, error = self.store.check_capacity(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a", 1
        )
        assert success is False
        assert error == "Unsupported"

    def test_spot_request_tracking(self):
        """Test adding and retrieving spot requests."""
        request = SpotRequestState(
            request_id="sir-1234567890abcdef0",
            instance_type="t2.micro",
            availability_zone="us-east-1a",
            state="active",
            status="fulfilled",
        )
        self.store.add_spot_request("123456789012", "us-east-1", request)

        retrieved = self.store.get_spot_request(
            "123456789012", "us-east-1", "sir-1234567890abcdef0"
        )
        assert retrieved is not None
        assert retrieved.request_id == "sir-1234567890abcdef0"
        assert retrieved.state == "active"

    def test_list_spot_requests(self):
        """Test listing spot requests."""
        request1 = SpotRequestState(
            request_id="sir-1234567890abcdef0",
            instance_type="t2.micro",
            availability_zone="us-east-1a",
        )
        request2 = SpotRequestState(
            request_id="sir-0987654321fedcba0",
            instance_type="t2.small",
            availability_zone="us-east-1b",
        )
        self.store.add_spot_request("123456789012", "us-east-1", request1)
        self.store.add_spot_request("123456789012", "us-east-1", request2)

        requests = self.store.list_spot_requests("123456789012", "us-east-1")
        assert len(requests) == 2

    def test_update_spot_request(self):
        """Test updating a spot request."""
        request = SpotRequestState(
            request_id="sir-1234567890abcdef0",
            instance_type="t2.micro",
            availability_zone="us-east-1a",
            state="open",
        )
        self.store.add_spot_request("123456789012", "us-east-1", request)

        updated = self.store.update_spot_request(
            "123456789012", "us-east-1", "sir-1234567890abcdef0", state="active"
        )
        assert updated is True

        retrieved = self.store.get_spot_request(
            "123456789012", "us-east-1", "sir-1234567890abcdef0"
        )
        assert retrieved.state == "active"

    def test_clear_spot_request(self):
        """Test clearing a spot request."""
        request = SpotRequestState(
            request_id="sir-1234567890abcdef0",
            instance_type="t2.micro",
            availability_zone="us-east-1a",
        )
        self.store.add_spot_request("123456789012", "us-east-1", request)

        cleared = self.store.clear_spot_request(
            "123456789012", "us-east-1", "sir-1234567890abcdef0"
        )
        assert cleared is True

        retrieved = self.store.get_spot_request(
            "123456789012", "us-east-1", "sir-1234567890abcdef0"
        )
        assert retrieved is None

    def test_account_isolation(self):
        """Test that profiles are isolated by account."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
        )
        self.store.set_profile("111111111111", "us-east-1", profile)

        # Different account should not see the profile
        retrieved = self.store.get_profile("222222222222", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved is None

        # Same account should see it
        retrieved = self.store.get_profile("111111111111", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved is not None

    def test_region_isolation(self):
        """Test that profiles are isolated by region."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        # Different region should not see the profile
        retrieved = self.store.get_profile("123456789012", "us-west-2", "g5.xlarge", "us-east-1a")
        assert retrieved is None

    def test_export_import_state(self):
        """Test exporting and importing state."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
            available_capacity=5,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        request = SpotRequestState(
            request_id="sir-1234567890abcdef0",
            instance_type="t2.micro",
            availability_zone="us-east-1a",
            state="active",
        )
        self.store.add_spot_request("123456789012", "us-east-1", request)

        # Export state
        state = self.store.export_state()
        assert "profiles" in state
        assert "spot_requests" in state

        # Create new store and import
        new_store = CapacityStore()
        new_store.load_state(state)

        # Verify profiles imported
        retrieved_profile = new_store.get_profile(
            "123456789012", "us-east-1", "g5.xlarge", "us-east-1a"
        )
        assert retrieved_profile is not None
        assert retrieved_profile.total_capacity == 10
        assert retrieved_profile.available_capacity == 5

        # Verify spot requests imported
        retrieved_request = new_store.get_spot_request(
            "123456789012", "us-east-1", "sir-1234567890abcdef0"
        )
        assert retrieved_request is not None
        assert retrieved_request.state == "active"

    def test_reset_all(self):
        """Test resetting all state."""
        profile = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
        )
        self.store.set_profile("123456789012", "us-east-1", profile)

        self.store.reset()

        retrieved = self.store.get_profile("123456789012", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved is None

    def test_reset_account_region(self):
        """Test resetting specific account/region."""
        profile1 = CapacityProfile(
            instance_type="g5.xlarge",
            availability_zone="us-east-1a",
            total_capacity=10,
        )
        profile2 = CapacityProfile(
            instance_type="t2.micro",
            availability_zone="us-west-2a",
            total_capacity=5,
        )
        self.store.set_profile("123456789012", "us-east-1", profile1)
        self.store.set_profile("123456789012", "us-west-2", profile2)

        # Reset only us-east-1
        self.store.reset("123456789012", "us-east-1")

        # us-east-1 should be cleared
        retrieved = self.store.get_profile("123456789012", "us-east-1", "g5.xlarge", "us-east-1a")
        assert retrieved is None

        # us-west-2 should still exist
        retrieved = self.store.get_profile("123456789012", "us-west-2", "t2.micro", "us-west-2a")
        assert retrieved is not None
