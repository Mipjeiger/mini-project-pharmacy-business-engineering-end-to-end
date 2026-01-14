"""Debugging script for Feast feature store setup."""

import sys
import os
from pathlib import Path

print("=" * 60)
print("FEAST DEBUGGING")
print("=" * 60)

# Check directory
print(f"\nCurrent directory: {os.getcwd()}")
print(f"Files: {os.listdir('.')}")

# Check if features.py exists
features_file = Path("features.py")
print(f"\nfeatures.py exists: {features_file.exists()}")
if features_file.exists():
    print(f"features.py size: {features_file.stat().st_size} bytes")

# Try to import features module
print("\n" + "=" * 60)
print("TESTING IMPORT")
print("=" * 60)

try:
    import features

    print("✓ features module imported")

    # Check what's defined
    defined_objects = [name for name in dir(features) if not name.startswith("_")]
    print(f"\nDefined objects ({len(defined_objects)}):")
    for obj in defined_objects:
        print(f"  - {obj}")

    # Check specific objects
    print("\nChecking required objects:")
    required = ["distributor", "city", "product", "sales_feature_view"]
    for req in required:
        if hasattr(features, req):
            obj = getattr(features, req)
            print(f"  ✓ {req}: {type(obj).__name__}")
        else:
            print(f"  ✗ {req}: NOT FOUND")

except ImportError as e:
    print(f"✗ Import failed: {e}")
    import traceback

    traceback.print_exc()

# Try to load Feature Store
print("\n" + "=" * 60)
print("TESTING FEATURE STORE")
print("=" * 60)

try:
    from feast import FeatureStore

    store = FeatureStore(repo_path=".")
    print(f"✓ Feature Store loaded")
    print(f"  Project: {store.project}")
    print(f"  Registry: {store.config.registry}")

    # List entities
    entities = store.list_entities()
    print(f"\n  Entities ({len(entities)}):")
    for e in entities:
        print(f"    - {e.name}")

    # List feature views
    fvs = store.list_feature_views()
    print(f"\n  Feature Views ({len(fvs)}):")
    for fv in fvs:
        print(f"    - {fv.name}")

    if len(entities) == 0 and len(fvs) == 0:
        print("\n⚠️  Registry is empty! Run 'feast apply' to register features.")

except Exception as e:
    print(f"✗ Feature Store error: {e}")
    import traceback

    traceback.print_exc()

print("\n" + "=" * 60)
