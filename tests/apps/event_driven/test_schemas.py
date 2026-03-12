"""
Tests for the event schema registry backed by DynamoDB.

Verifies schema registration, versioning, retrieval, validation,
and listing operations.
"""

from .app import EventRouter
from .models import Event, EventSchema


class TestSchemaRegistry:
    def test_register_and_retrieve_schema(
        self, event_router: EventRouter, schema_table: str, unique_name: str
    ):
        """Register an event schema and retrieve it by name and source."""
        schema = EventSchema(
            name="OrderCreated",
            source="order-service",
            detail_type="OrderCreated",
            json_schema={
                "type": "object",
                "required": ["order_id", "amount"],
                "properties": {
                    "order_id": {"type": "string"},
                    "amount": {"type": "number"},
                    "customer": {"type": "string"},
                },
            },
        )
        registered = event_router.register_schema(schema)
        assert registered.version == 1

        retrieved = event_router.get_schema("OrderCreated", "order-service")
        assert retrieved is not None
        assert retrieved.name == "OrderCreated"
        assert retrieved.source == "order-service"
        assert retrieved.version == 1
        assert "order_id" in retrieved.json_schema["required"]

    def test_update_schema_increments_version(
        self, event_router: EventRouter, schema_table: str, unique_name: str
    ):
        """Registering a schema with the same name auto-increments the version."""
        schema_v1 = EventSchema(
            name="PaymentProcessed",
            source="payment-service",
            detail_type="PaymentProcessed",
            json_schema={
                "type": "object",
                "required": ["payment_id"],
                "properties": {
                    "payment_id": {"type": "string"},
                },
            },
        )
        v1 = event_router.register_schema(schema_v1)
        assert v1.version == 1

        schema_v2 = EventSchema(
            name="PaymentProcessed",
            source="payment-service",
            detail_type="PaymentProcessed",
            json_schema={
                "type": "object",
                "required": ["payment_id", "amount"],
                "properties": {
                    "payment_id": {"type": "string"},
                    "amount": {"type": "number"},
                    "currency": {"type": "string"},
                },
            },
        )
        v2 = event_router.register_schema(schema_v2)
        assert v2.version == 2

        # Latest version should be v2
        latest = event_router.get_schema("PaymentProcessed", "payment-service")
        assert latest is not None
        assert latest.version == 2
        assert "amount" in latest.json_schema["required"]

    def test_retrieve_specific_version(
        self, event_router: EventRouter, schema_table: str, unique_name: str
    ):
        """Retrieve a specific version of a schema."""
        for i in range(3):
            schema = EventSchema(
                name="InventoryChanged",
                source="inventory-service",
                detail_type="InventoryChanged",
                json_schema={
                    "type": "object",
                    "required": ["sku"],
                    "properties": {
                        "sku": {"type": "string"},
                        "version_marker": {"type": "integer"},
                    },
                },
            )
            event_router.register_schema(schema)

        v1 = event_router.get_schema("InventoryChanged", "inventory-service", version=1)
        assert v1 is not None
        assert v1.version == 1

        v2 = event_router.get_schema("InventoryChanged", "inventory-service", version=2)
        assert v2 is not None
        assert v2.version == 2

    def test_validate_event_against_schema_passes(
        self, event_router: EventRouter, schema_table: str, unique_name: str
    ):
        """Validate an event that conforms to the registered schema."""
        schema = EventSchema(
            name="OrderCreated",
            source="order-service",
            detail_type="OrderCreated",
            json_schema={
                "type": "object",
                "required": ["order_id", "amount"],
                "properties": {
                    "order_id": {"type": "string"},
                    "amount": {"type": "number"},
                },
            },
        )
        event_router.register_schema(schema)

        event = Event(
            source="order-service",
            detail_type="OrderCreated",
            detail={"order_id": "ORD-123", "amount": 49.99},
        )
        valid, error = event_router.validate_event(event)
        assert valid is True
        assert error == ""

    def test_validate_event_missing_required_field_fails(
        self, event_router: EventRouter, schema_table: str, unique_name: str
    ):
        """Validate an event missing a required field — should fail."""
        schema = EventSchema(
            name="OrderCreated",
            source="order-service",
            detail_type="OrderCreated",
            json_schema={
                "type": "object",
                "required": ["order_id", "amount"],
                "properties": {
                    "order_id": {"type": "string"},
                    "amount": {"type": "number"},
                },
            },
        )
        event_router.register_schema(schema)

        event = Event(
            source="order-service",
            detail_type="OrderCreated",
            detail={"order_id": "ORD-123"},  # missing 'amount'
        )
        valid, error = event_router.validate_event(event)
        assert valid is False
        assert "amount" in error

    def test_validate_event_wrong_type_fails(
        self, event_router: EventRouter, schema_table: str, unique_name: str
    ):
        """Validate an event with wrong field type — should fail."""
        schema = EventSchema(
            name="OrderCreated",
            source="order-service",
            detail_type="OrderCreated",
            json_schema={
                "type": "object",
                "required": ["order_id"],
                "properties": {
                    "order_id": {"type": "string"},
                    "amount": {"type": "number"},
                },
            },
        )
        event_router.register_schema(schema)

        event = Event(
            source="order-service",
            detail_type="OrderCreated",
            detail={"order_id": "ORD-123", "amount": "not-a-number"},
        )
        valid, error = event_router.validate_event(event)
        assert valid is False
        assert "amount" in error

    def test_validate_event_no_schema_passes(
        self, event_router: EventRouter, schema_table: str, unique_name: str
    ):
        """Validation with no registered schema is permissive (passes)."""
        event = Event(
            source="unknown-service",
            detail_type="SomeEvent",
            detail={"anything": "goes"},
        )
        valid, error = event_router.validate_event(event)
        assert valid is True

    def test_list_schemas_by_source(
        self, event_router: EventRouter, schema_table: str, unique_name: str
    ):
        """List schemas filtered by source."""
        event_router.register_schema(
            EventSchema(
                name="OrderCreated",
                source="order-service",
                detail_type="OrderCreated",
                json_schema={"type": "object", "properties": {}},
            )
        )
        event_router.register_schema(
            EventSchema(
                name="OrderShipped",
                source="order-service",
                detail_type="OrderShipped",
                json_schema={"type": "object", "properties": {}},
            )
        )
        event_router.register_schema(
            EventSchema(
                name="PaymentReceived",
                source="payment-service",
                detail_type="PaymentReceived",
                json_schema={"type": "object", "properties": {}},
            )
        )

        order_schemas = event_router.list_schemas(source="order-service")
        assert len(order_schemas) >= 2
        assert all(s.source == "order-service" for s in order_schemas)

        payment_schemas = event_router.list_schemas(source="payment-service")
        assert len(payment_schemas) >= 1
        assert payment_schemas[0].source == "payment-service"

    def test_list_all_schemas(self, event_router: EventRouter, schema_table: str, unique_name: str):
        """List all schemas across all sources."""
        event_router.register_schema(
            EventSchema(
                name="EventA",
                source="svc-a",
                detail_type="EventA",
                json_schema={"type": "object"},
            )
        )
        event_router.register_schema(
            EventSchema(
                name="EventB",
                source="svc-b",
                detail_type="EventB",
                json_schema={"type": "object"},
            )
        )

        all_schemas = event_router.list_schemas()
        assert len(all_schemas) >= 2
        sources = {s.source for s in all_schemas}
        assert "svc-a" in sources
        assert "svc-b" in sources
