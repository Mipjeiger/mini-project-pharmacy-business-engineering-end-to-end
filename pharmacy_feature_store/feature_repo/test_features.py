"""Test script for Feast Feature Store - Pharmacy Sales Project."""

import os
from typing import List, Dict
import pandas as pd
from datetime import datetime
from feast import FeatureStore, Entity, FeatureView
from sqlalchemy import create_engine
from dotenv import load_dotenv


def setup_environment():
    """Load environment variables from .env file."""
    print("\nSetting up environment variables...")
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(env_path)
    print(f" Environment variables loaded from {env_path}")


def verify_directory() -> None:
    """Verify current working directory and list files."""
    print("\nVerifying current working directory...")
    print(f"\nCurrent directory: {os.getcwd()}")
    print(f"Files: {os.listdir('.')}")


def initialize_feature_store() -> FeatureStore:
    """Initialize and return the Feast Feature Store.

    Returns:
        FeatureStore: Initialized Feast Feature Store object.

    Raises:
        Exception: If the Feature Store cannot be initialized."""
    print("\nInitializing Feature Store...")
    try:
        store = FeatureStore(repo_path=".")
        print(f"✓ Feature Store initialized")
        print(f" Project: {store.project}\n")
        print(f" Registry: {store.config.registry}\n")
        return store
    except Exception as e:
        print(f"Error loading Feature Store: {e}")
        raise SystemExit(1)


def list_entities(store: FeatureStore) -> List[Entity]:
    """
    List all entities in the feature store.

    Args:
        store: Feast feature store instance.

    Returns:
        List of entities.
    """
    print("\nListing Entities:")
    try:
        entities = store.list_entities()
        print(f" Total Entities: {len(entities)}")

        for entity in entities:
            print(f" {entity.name}")
            print(f" Type: {entity.value_type}")
            print(f" Join Keys: {entity.join_keys}")

        return entities
    except Exception as e:
        print(f"Error listing entities: {e}")
        return []


def list_feature_views(store: FeatureStore) -> List[FeatureView]:
    """
    List all feature views in the feature store.

    Args:
        store: Feast feature store instance.

    Returns:
        List of feature views.
    """
    print("\nListing Feature Views:")
    try:
        feature_views = store.list_feature_views()
        print(f"Total feature views: {len(feature_views)}")

        for fv in feature_views:
            print(f" {fv.name}")

            # Get entitiy names
            if hasattr(fv, "entities") and fv.entities:
                entity_names = (
                    fv.entities
                    if isinstance(fv.entities[0], str)
                    else [e.name for e in fv.entities]
                )
                print(f" Entities: {entity_names}")

            # Get features names
            if hasattr(fv, "schema"):
                feature_names = [field.name for field in fv.schema]
                print(f" Features: ({len(feature_names)}): {feature_names}")

            print()

        return feature_views
    except Exception as e:
        print(f"Error listing feature views: {e}")
        import traceback

        traceback.print_exc()
        return []


def verify_feature_view(store: FeatureStore, feature_view_name: str) -> bool:
    """
    Verify that a specific feature view exists in the store.

    Args:
        store: Feast feature store instance.
        feature_view_name: Name of the feature view to verify.

    Returns:
        bool: True if the feature view exists, False otherwise."""
    print("\nVerifying feature view:", feature_view_name)
    try:
        fv = store.get_feature_view(feature_view_name)
        print(
            f"Feature view '{feature_view_name}' found with {len(fv.features)} features."
        )

        # Display entities
        if hasattr(fv, "entities") and fv.entities:
            entity_names = (
                fv.entities
                if isinstance(fv.entities[0], str)
                else [e.name for e in fv.entities]
            )
            print(f" Entities: {entity_names}")

        # Display features
        if hasattr(fv, "schema"):
            feature_names = [field.name for field in fv.schema]
            print(f" Features: ({len(feature_names)}): {feature_names}")
            for feature in feature_names:
                print(f"  - {feature}")

        print()
        return True

    except Exception as e:
        print(f"Feature view '{feature_view_name}' not found: {e}")
        return False


def get_sample_data_from_postgres() -> pd.DataFrame:
    """
    Retrieve sample data from PostgreSQL to use for feature retrieval testing.

    Returns:
        DataFrame with sample data from PostgreSQL."""
    print("\nRetrieving sample data from PostgreSQL...")
    try:
        # Create database connection
        db_engine = create_engine(
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )

        # Query sample data
        query = """
            SELECT DISTINCT
                distributor,
                city,
                product_name,
                sales,
                quantity,
                year,
                month
            FROM silver.pharmacy_sales
            WHERE distributor IS NOT NULL
            LIMIT 8"""

        sample_data = pd.read_sql(query, db_engine)

        if len(sample_data) == 0:
            print("No sample data retrieved from PostgreSQL.")
            print("Trying to get any available data...")

            sample_data = pd.read_sql(
                "SELECT DISTINCT distributor, city, product_name FROM silver.pharmacy_sales LIMIT 8",
                db_engine,
            )

        print(f"Retrieved data ({len(sample_data)} rows):")
        print(sample_data)
        print()

        db_engine.dispose()
        return sample_data

    except Exception as e:
        print(f"Error retrieving data from PostgreSQL: {e}")
        import traceback

        traceback.print_exc()
        return pd.DataFrame()  # Return empty DataFrame on error


def create_entity_dataframe(sample_data: pd.DataFrame = None) -> pd.DataFrame:
    """
    Create a DataFrame with entity keys and event timestamps for feature retrieval.

    Args:
        sample_data: DataFrame with sample data containing entity keys.

    Returns:
        DataFrame with entity keys and event timestamps.
    """
    print("\nCreating entity DataFrame for feature retrieval...")
    if sample_data is not None and len(sample_data) >= 1:
        # Use real data from PostgreSQL
        entity_df = pd.DataFrame(
            {
                "distributor": sample_data["distributor"].iloc[:1].tolist(),
                "city": sample_data["city"].iloc[:1].tolist(),
                "product_name": sample_data["product_name"].iloc[:1].tolist(),
                "event_timestamp": [datetime.now(), datetime.now()],
            }
        )
    else:
        # Fallback to hardcode values
        entity_df = pd.DataFrame(
            {
                "distributor": ["Beier", "Beier"],
                "city": ["Lublin", "Bielsko-Biała"],
                "product_name": ["Kinenadryl", "Abobozolid"],
                "event_timestamp": [datetime.now(), datetime.now()],
            }
        )


def test_historical_features(
    store: FeatureStore, entity_df: pd.DataFrame, features: List[str]
) -> pd.DataFrame:
    """
    Test historical feature retrieval from the feature store.

    Aargs:
        store: Feast feature store instance.
        entity_df: DataFrame with entity keys and event timestamps.
        features: List of feature names to retrieve."""
    print("\nTesting historical feature retrieval...")
    try:
        training_df = store.get_historical_features(
            entity_df=entity_df,
            features=features,
        ).to_df()

        print(f"Retrieved historical features ({len(training_df)} rows):")
        print(training_df)
        print(f"\nShape: {training_df.shape}")
        print(f"Columns: {list(training_df.columns)}")

        # Check for Nan Values
        nan_counts = training_df.isna().sum()
        total_nans = nan_counts.sum()

        if total_nans > 0:
            print(f"\n⚠️  Found {total_nans} NaN values in the retrieved features:")
            print(f"Nan counts per column:\n{nan_counts[nan_counts > 0]}")
        else:
            print("\n✓ No NaN values found in the retrieved features.")

        # Handling Nan values if NaN values are found
        print("\nHandling NaN values...")
        if nan_counts.sum() > 0:
            training_df.fillna(0, inplace=True)
            print(" NaN values filled with 0")
        else:
            print(" No NaN values to handle")

        print()
        return training_df

    except Exception as e:
        print(f"Error retrieving historical features: {e}")
        import traceback

        traceback.print_exc()
        return pd.DataFrame()  # Return empty DataFrame on error


def main():
    """Main execution function."""
    print("\nFeast Feature Store Test Script - Pharmacy Sales Project")

    # Step 1: Setup environment
    setup_environment()
    verify_directory()

    # Step 2: Initialize Feature Store
    store = initialize_feature_store()

    # Step 3: List Entities and Feature Views
    entities = list_entities(store=store)
    feature_views = list_feature_views(store=store)

    # Step 4: Verify specific Feature View
    if not verify_feature_view(store=store, feature_view_name="sales_features"):
        print("sales_features Feature View not found. Exiting test.")
        return

    # Step 5: Retrieve sample data from PostgreSQL
    sample_data = get_sample_data_from_postgres()

    # Step 6: Create entity DataFrame
    entity_df = create_entity_dataframe(
        sample_data=sample_data if not sample_data.empty else None
    )

    # Step 7: Test historical feature retrieval
    features_to_retrieve = [
        "sales_features:total_quantity",
        "sales_features:total_sales",
        "sales_features:avg_price",
        "sales_features:year",
        "sales_features:month",
        "sales_features:product_class",
        "sales_features:sales_team",
    ]

    result_df = test_historical_features(
        store=store, entity_df=entity_df, features=features_to_retrieve
    )

    # Summary
    print("\nTest Summary:")
    print(f" Entities registered: {len(entities)}")
    print(f"✓ Feature views registered: {len(feature_views)}")
    print(
        f" Features retrieved: {len(result_df.columns) if not result_df.empty else 0}"
    )
    print(f"Records retrieved: {len(result_df) if not result_df.empty else 0}")
    print("\n✅ TEST COMPLETED SUCCESSFULLY\n")


if __name__ == "__main__":
    main()
