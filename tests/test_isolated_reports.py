"""Regression tests for independent and concurrent report saves."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
from types import SimpleNamespace

from plugin.storage import store
from verifier.checks import read_receipt


def test_two_reports_remain_valid_in_same_workspace(tmp_path):
    # Originally all saves overwrote reports/result.json, invalidating old receipts.
    first = {"batch": "all-good", "statuses": ["succeeded"], "failed": []}
    second = {"batch": "retries", "statuses": ["failed"], "failed": ["B"]}
    p1, d1 = store(tmp_path, first)
    p2, d2 = store(tmp_path, second)
    assert p1 != p2
    assert read_receipt(tmp_path, SimpleNamespace(path=p1, digest=d1)) == (first, None)
    assert read_receipt(tmp_path, SimpleNamespace(path=p2, digest=d2)) == (second, None)


def test_concurrent_saves_do_not_clobber(tmp_path):
    payloads = [{"batch": f"batch-{i}", "index": i} for i in range(12)]
    with ThreadPoolExecutor(max_workers=6) as executor:
        receipts = list(executor.map(lambda payload: store(tmp_path, payload), payloads))
    for payload, (path, digest) in zip(payloads, receipts):
        assert read_receipt(tmp_path, SimpleNamespace(path=path, digest=digest)) == (payload, None)


def test_forged_paths_and_corrupt_files_are_rejected(tmp_path):
    path, digest = store(tmp_path, {"okay": True})
    assert read_receipt(tmp_path, SimpleNamespace(path="../elsewhere.json", digest=digest))[0] is None
    assert read_receipt(tmp_path, SimpleNamespace(path=path, digest="bad"))[0] is None
    (tmp_path / path).write_text('{corrupted', encoding="utf-8")
    saved, reason = read_receipt(tmp_path, SimpleNamespace(path=path, digest=digest))
    assert saved is None
    assert reason == "report digest does not match file"


def test_malformed_json_with_correct_digest_is_rejected(tmp_path):
    malformed = b"{not valid json"
    digest = hashlib.sha256(malformed).hexdigest()
    path = f"reports/result-{digest}.json"
    (tmp_path / "reports").mkdir()
    (tmp_path / path).write_bytes(malformed)
    saved, problem = read_receipt(tmp_path, SimpleNamespace(path=path, digest=digest))
    assert saved is None
    assert problem == "report is not valid JSON"
