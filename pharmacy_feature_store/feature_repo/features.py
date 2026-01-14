"""Feature definitions for the pharmacy sales feature store using Feast."""

from feast import Entity, FeatureView, Field
from feast.types import String, Float64, Int64
from feast.value_type import ValueType
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from datetime import timedelta

# ============================================
# ENTITIES
# ============================================

distributor = Entity(
    name="distributor",
    join_keys=["distributor"],
    value_type=ValueType.STRING,
    description="Pharmaceutical distributor",
)

city = Entity(
    name="city",
    join_keys=["city"],
    value_type=ValueType.STRING,
    description="City where the pharmacy distributor is located",
)

product = Entity(
    name="product_name",
    join_keys=["product_name"],
    value_type=ValueType.STRING,
    description="Name of the pharmaceutical product",
)

# ============================================
# DATA SOURCE
# ============================================

pharmacy_sales_source = PostgreSQLSource(
    name="pharmacy_sales_source",
    query="""
        SELECT
            distributor,
            city,
            product_name,
            product_class,
            channel,
            sub_channel,
            year,
            month,
            quantity as total_quantity,
            sales as total_sales,
            price as avg_price,
            sales_team,
            NOW() as event_timestamp
        FROM silver.pharmacy_sales
    """,
    timestamp_field="event_timestamp",
    description="Pharmacy sales data from PostgreSQL silver schema",
)

# ============================================
# FEATURE VIEW
# ============================================

sales_feature_view = FeatureView(
    name="sales_features",
    entities=[distributor, city, product],
    schema=[
        Field(name="product_class", dtype=String),
        Field(name="channel", dtype=String),
        Field(name="sub_channel", dtype=String),
        Field(name="year", dtype=Int64),
        Field(name="month", dtype=String),
        Field(name="total_quantity", dtype=Int64),
        Field(name="total_sales", dtype=Float64),
        Field(name="avg_price", dtype=Float64),
        Field(name="sales_team", dtype=String),
    ],
    source=pharmacy_sales_source,
    ttl=timedelta(days=365),
    online=True,
    tags={"team": "data_science", "project": "pharmacy_sales"},
)
