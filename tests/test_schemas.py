"""Unit tests for Pydantic schema contracts."""
from __future__ import annotations
from datetime import date
import pytest
from pydantic import ValidationError
from src.utils.schemas import OrderRow, CustomerRow, ProductRow


class TestOrderRow:
    def _valid(self, **overrides):
        base = dict(order_id="ORD-1", customer_id="C1", product_id="P1",
                    order_date=date(2025, 1, 1), quantity=2,
                    unit_price=9.99, status="shipped")
        base.update(overrides)
        return base

    def test_valid_order(self):
        row = OrderRow(**self._valid())
        assert row.order_id == "ORD-1"

    def test_status_normalised_to_lowercase(self):
        row = OrderRow(**self._valid(status="Shipped"))
        assert row.status == "shipped"

    def test_zero_quantity_rejected(self):
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(quantity=0))

    def test_negative_quantity_rejected(self):
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(quantity=-1))

    def test_negative_price_rejected(self):
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(unit_price=-0.01))

    def test_invalid_status_rejected(self):
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(status="refunded"))


class TestCustomerRow:
    def _valid(self, **overrides):
        base = dict(customer_id="C1", first_name="Alice", last_name="Smith",
                    email="alice@example.com", city="NYC", country="US",
                    signup_date=date(2024, 1, 1), tier="gold")
        base.update(overrides)
        return base

    def test_valid_customer(self):
        row = CustomerRow(**self._valid())
        assert row.customer_id == "C1"

    def test_email_normalised_to_lowercase(self):
        row = CustomerRow(**self._valid(email="Alice@Example.COM"))
        assert row.email == "alice@example.com"

    def test_bad_email_rejected(self):
        with pytest.raises(ValidationError):
            CustomerRow(**self._valid(email="not-an-email"))

    def test_tier_defaults_to_standard(self):
        data = self._valid()
        data.pop("tier")
        row = CustomerRow(**data)
        assert row.tier == "standard"


class TestProductRow:
    def _valid(self, **overrides):
        base = dict(product_id="P1", name="Widget", category="Electronics",
                    unit_cost=10.0, supplier_id="SUP-A",
                    updated_at="2025-01-01T00:00:00")
        base.update(overrides)
        return base

    def test_valid_product(self):
        row = ProductRow(**self._valid())
        assert row.product_id == "P1"

    def test_negative_cost_rejected(self):
        with pytest.raises(ValidationError):
            ProductRow(**self._valid(unit_cost=-1.0))

    def test_zero_cost_allowed(self):
        row = ProductRow(**self._valid(unit_cost=0.0))
        assert row.unit_cost == 0.0


# ── FR-2: Schema Validation ───────────────────────────────────────────────────

import logging
from src.transform.schema_check import check_schema
from src.utils.exceptions import SchemaError


class TestSchemaValidation:
    """Tests covering FR-2.1, FR-2.2, and FR-2.3."""

    def _logger(self):
        return logging.getLogger("test")

    # ── FR-2.1: each source has an explicit Pydantic schema ──────────────────
    
    # This test case makes sure that the schema for orders works. It creates 
    # a valid order row with all required fields and makes sure it was accepted 
    # with no errors. 
    def test_fr2_1_order_schema_exists(self):
        """OrderRow can be imported and instantiated from schemas.py."""
        from src.utils.schemas import OrderRow
        from datetime import date
        row = OrderRow(order_id="O1", customer_id="C1", product_id="P1",
                       order_date=date(2025, 1, 1), quantity=1,
                       unit_price=9.99, status="shipped")
        assert row.order_id == "O1"

    # This test case makes sure that the schema for customers works. It creates 
    # a valid order row with all required fields and makes sure it was accepted 
    # with no errors.
    def test_fr2_1_customer_schema_exists(self):
        """CustomerRow can be imported and instantiated from schemas.py."""
        from src.utils.schemas import CustomerRow
        from datetime import date
        row = CustomerRow(customer_id="C1", first_name="Alice", last_name="Smith",
                          email="alice@example.com", city="NYC", country="US",
                          signup_date=date(2024, 1, 1), tier="gold")
        assert row.customer_id == "C1"

    # This test case makes sure that the schema for orders works. It creates 
    # a valid order row with all required fields and makes sure it was accepted 
    # with no errors.
    def test_fr2_1_product_schema_exists(self):
        """ProductRow can be imported and instantiated from schemas.py."""
        from src.utils.schemas import ProductRow
        row = ProductRow(product_id="P1", name="Widget", category="Electronics",
                         unit_cost=10.0, supplier_id="SUP-A",
                         updated_at="2025-01-01T00:00:00")
        assert row.product_id == "P1"

    # ── FR-2.2: missing required column → SchemaError raised ─────────────────

    def test_fr2_2_missing_column_raises_schema_error(self):
        """A column present in expected but absent in actual raises SchemaError."""
        actual   = ["order_id", "customer_id", "product_id", "order_date", "quantity", "status"]
        expected = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "status"]
        with pytest.raises(SchemaError):
            check_schema(actual, expected, "orders", self._logger())

    def test_fr2_2_error_message_names_missing_column(self):
        """SchemaError message tells you exactly which column is missing."""
        actual   = ["order_id", "quantity", "status"]
        expected = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "status"]
        with pytest.raises(SchemaError, match="unit_price"):
            check_schema(actual, expected, "orders", self._logger())

    def test_fr2_2_multiple_missing_columns_all_reported(self):
        """All missing columns are named in the error, not just the first one."""
        actual   = ["order_id"]
        expected = ["order_id", "quantity", "unit_price", "status"]
        with pytest.raises(SchemaError, match="quantity|unit_price|status"):
            check_schema(actual, expected, "orders", self._logger())

    def test_fr2_2_exact_columns_does_not_raise(self):
        """When actual columns exactly match expected, no error is raised."""
        cols = ["order_id", "customer_id", "quantity"]
        check_schema(cols, cols, "orders", self._logger())  # should not raise

    # ── FR-2.3: extra unexpected column → warning logged, continues ───────────

    def test_fr2_3_extra_column_does_not_raise(self):
        """An extra column in actual (additive drift) must not raise any exception."""
        actual   = ["order_id", "quantity", "status", "mystery_column"]
        expected = ["order_id", "quantity", "status"]
        check_schema(actual, expected, "orders", self._logger())  # should not raise

    def test_fr2_3_extra_column_warning_logged(self, caplog):
        """An extra column must emit a WARNING-level log message."""
        actual   = ["order_id", "quantity", "new_col"]
        expected = ["order_id", "quantity"]
        with caplog.at_level(logging.WARNING):
            check_schema(actual, expected, "orders", self._logger())
        assert any("new_col" in r.message for r in caplog.records)

    def test_fr2_3_warning_message_names_extra_column(self, caplog):
        """The warning message must name the unexpected column explicitly."""
        actual   = ["order_id", "surprise_field"]
        expected = ["order_id"]
        with caplog.at_level(logging.WARNING):
            check_schema(actual, expected, "orders", self._logger())
        assert any("surprise_field" in r.message for r in caplog.records)
