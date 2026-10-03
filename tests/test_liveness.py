"""Tests for adapter liveness checks (no network: requests is mocked)."""

from unittest.mock import MagicMock, patch

import requests

from apartment_finder.adapters.balder import LISTING_BASE_URL, BalderAdapter
from apartment_finder.adapters.lejebolig import LejeboligAdapter


def _response(status_code: int, json_data=None) -> MagicMock:
    resp = MagicMock(status_code=status_code)
    resp.json.return_value = json_data
    return resp


def _ctx_response(status_code: int) -> MagicMock:
    """A response usable as `with requests.get(...) as r`."""
    resp = _response(status_code)
    resp.__enter__.return_value = resp
    return resp


@patch("apartment_finder.adapters.base.time.sleep")
@patch("apartment_finder.adapters.base.requests.get")
def test_default_check_marks_only_404_and_410_gone(mock_get, _sleep):
    codes = {"a": 200, "b": 404, "c": 410, "d": 403, "e": 503, "f": 200}
    mock_get.side_effect = lambda url, **kw: _ctx_response(codes[url])
    adapter = LejeboligAdapter({}, {"lejebolig": {}})

    assert adapter.check_status(list(codes)) == {
        "a": "unknown", "b": "gone", "c": "gone",
        "d": "unknown", "e": "unknown", "f": "unknown",
    }


@patch("apartment_finder.adapters.base.time.sleep")
@patch("apartment_finder.adapters.base.requests.get")
def test_default_check_treats_network_errors_as_unknown(mock_get, _sleep):
    mock_get.side_effect = requests.ConnectionError("boom")
    adapter = LejeboligAdapter({}, {"lejebolig": {}})

    assert adapter.check_status(["x"]) == {"x": "unknown"}


@patch("apartment_finder.adapters.base.time.sleep")
@patch("apartment_finder.adapters.base.requests.get")
def test_default_check_all_404_batch_is_inconclusive(mock_get, _sleep):
    mock_get.side_effect = lambda url, **kw: _ctx_response(404)
    adapter = LejeboligAdapter({}, {"lejebolig": {}})
    urls = [f"u{n}" for n in range(6)]

    assert adapter.check_status(urls) == {url: "unknown" for url in urls}


@patch("apartment_finder.adapters.base.LIVENESS_TIME_BUDGET_SECONDS", 0)
@patch("apartment_finder.adapters.base.requests.get")
def test_default_check_stops_at_time_budget(mock_get):
    adapter = LejeboligAdapter({}, {"lejebolig": {}})

    assert adapter.check_status(["a", "b"]) == {}
    mock_get.assert_not_called()


@patch("apartment_finder.adapters.balder.requests.post")
def test_balder_marks_units_missing_from_available_set_gone(mock_post):
    mock_post.return_value = _response(
        200, {"results": [{"hits": [{"slug": "live-unit"}], "estimatedTotalHits": 1}]}
    )
    live = f"{LISTING_BASE_URL}/live-unit"
    rented = f"{LISTING_BASE_URL}/rented-unit"

    assert BalderAdapter({}, {}).check_status([live, rented]) == {
        live: "active",
        rented: "gone",
    }


@patch("apartment_finder.adapters.balder.requests.post")
def test_balder_empty_or_failed_response_marks_nothing(mock_post):
    url = f"{LISTING_BASE_URL}/some-unit"
    adapter = BalderAdapter({}, {})

    mock_post.return_value = _response(200, {"results": [{"hits": []}]})
    assert adapter.check_status([url]) == {}

    mock_post.side_effect = requests.ConnectionError("down")
    assert adapter.check_status([url]) == {}


@patch("apartment_finder.adapters.balder.requests.post")
def test_balder_truncated_result_marks_nothing(mock_post):
    mock_post.return_value = _response(
        200, {"results": [{"hits": [{"slug": "live-unit"}], "estimatedTotalHits": 1500}]}
    )
    url = f"{LISTING_BASE_URL}/maybe-live-past-the-cut"

    assert BalderAdapter({}, {}).check_status([url]) == {}


@patch("apartment_finder.adapters.balder.requests.post")
def test_balder_malformed_json_marks_nothing(mock_post):
    resp = _response(200)
    resp.json.side_effect = ValueError("not json")
    mock_post.return_value = resp

    assert BalderAdapter({}, {}).check_status([f"{LISTING_BASE_URL}/x"]) == {}
