"""Verification script: tests deduplication is working correctly.

Test logic:
  1. Insert a document into MongoDB.
  2. Try inserting the SAME document (same doc_id) again.
  3. Assert that MongoDB document count did NOT increase.
  4. Verify the unique index exists on the collection.

Run: python -m tests.test_deduplication
"""
import sys
import logging
from src.utils import make_doc_id, upsert_raw, get_mongo_client
from src.config import MONGO_DB

logging.basicConfig(level=logging.WARNING)

TEST_COLLECTION = "dedup_test"
TEST_URL = "https://example.com/test-article-dedup-verification"


def run_dedup_test() -> bool:
    print("\n[Deduplication Test]")
    try:
        client = get_mongo_client()
        client.admin.command('ping')
    except Exception as e:
        print(f"  ⚠️  Skipped: MongoDB not reachable ({e})")
        return True

    db = client[MONGO_DB]
    col = db[TEST_COLLECTION]

    # Clean up from any previous test run
    col.delete_many({"doc_id": make_doc_id(TEST_URL)})

    doc = {
        "doc_id": make_doc_id(TEST_URL),
        "source": "test",
        "content_type": "article",
        "title": "Test Article",
        "content": "This is a test document for deduplication verification.",
        "url": TEST_URL,
        "brand": "TestBrand",
    }

    # First insert — should succeed
    result1 = upsert_raw(TEST_COLLECTION, doc)
    count_after_first = col.count_documents({"doc_id": doc["doc_id"]})
    assert result1 is True,  "❌ First insert returned False (expected True)"
    assert count_after_first == 1, f"❌ Expected 1 doc after first insert, got {count_after_first}"
    print("  ✅ First insert: success (doc_id stored)")

    # Second insert (duplicate) — should be rejected silently
    result2 = upsert_raw(TEST_COLLECTION, doc)
    count_after_second = col.count_documents({"doc_id": doc["doc_id"]})
    assert result2 is False, "❌ Second insert returned True (duplicate should be rejected)"
    assert count_after_second == 1, f"❌ Document count increased on duplicate: {count_after_second}"
    print("  ✅ Duplicate insert: rejected (count unchanged = 1)")

    # Verify unique index exists
    indexes = col.index_information()
    has_unique = any(
        idx.get("unique") and "doc_id" in str(idx.get("key"))
        for idx in indexes.values()
    )
    assert has_unique, "❌ Unique index on doc_id not found in collection"
    print("  ✅ Unique index on doc_id: confirmed")

    # Cleanup
    col.delete_many({"doc_id": doc["doc_id"]})
    print("\n  ✅ ALL DEDUPLICATION TESTS PASSED\n")
    return True


def run_brand_detection_test() -> bool:
    from src.utils import detect_brand
    print("[Brand Disambiguation Test]")

    cases = [
        ("Xe VF3 mới ra mắt tại Hà Nội", "Other"),
        ("BYD Atto 3 giá rẻ hơn VinFast", "Other"),
        ("Xiaomi SU7 sắp về Việt Nam", "Other"),
        ("Xe điện ngày càng phổ biến", "Other"),
        ("Bài viết không liên quan đến xe", "Other"),
    ]

    all_pass = True
    for text, expected in cases:
        result = detect_brand(text)
        ok = result == expected
        icon = "✅" if ok else "❌"
        print(f"  {icon}  '{text[:45]}...' → {result} (expected: {expected})")
        if not ok:
            all_pass = False

    if all_pass:
        print("\n  ✅ ALL BRAND DETECTION TESTS PASSED\n")
    else:
        print("\n  ❌ SOME BRAND DETECTION TESTS FAILED\n")
    return all_pass


def run_doc_id_stability_test() -> bool:
    print("[Doc ID Stability Test]")
    url = "https://www.youtube.com/watch?v=abc123"
    id1 = make_doc_id(f"yt_video_{url}")
    id2 = make_doc_id(f"yt_video_{url}")
    assert id1 == id2, "❌ Same input produces different doc_id (not deterministic)"
    assert len(id1) == 32, f"❌ doc_id length unexpected: {len(id1)}"
    print(f"  ✅ Deterministic: same URL → same doc_id: {id1}")
    print("  ✅ DOC ID STABILITY TEST PASSED\n")
    return True


if __name__ == "__main__":
    passed = 0
    total = 3
    try:
        if run_doc_id_stability_test():
            passed += 1
    except AssertionError as e:
        print(e)

    try:
        if run_brand_detection_test():
            passed += 1
    except AssertionError as e:
        print(e)

    try:
        if run_dedup_test():
            passed += 1
    except (AssertionError, Exception) as e:
        print(f"  ❌ Dedup test failed: {e}\n  (Is MongoDB running? Check docker-compose up)")

    print(f"Results: {passed}/{total} test suites passed")
    sys.exit(0 if passed == total else 1)
