from feast import FeatureStore
import features

print("=" * 60)
print("MANUAL FEAST APPLY")
print("=" * 60)

# Initialize Feature Store
store = FeatureStore(repo_path=".")
print("✓ Feature Store initialized")
print(f"  Project: {store.project}\n")

# Apply all at once
print("Applying features...")
store.apply(
    [features.distributor, features.city, features.product, features.sales_feature_view]
)

print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

# List entities
entities = store.list_entities()
print(f"\nEntities ({len(entities)}):")
for e in entities:
    print(f"  ✓ {e.name}")

# List feature views
fvs = store.list_feature_views()
print(f"\nFeature Views ({len(fvs)}):")
for fv in fvs:
    print(f"  ✓ {fv.name}")
    print(f"    Features: {len(fv.features)} total")

print("\n" + "=" * 60)
print("✅ SUCCESS")
print("=" * 60)
print("\nRun: python3 test_features.py")
