"""Offline tests for SEEK discovery - no network calls."""

import httpx

from applypilot.discovery import seek

SAMPLE = {
    "data": [
        {
            "id": "80000001",
            "title": "Graduate Environmental Scientist",
            "advertiser": {"description": "Acme Environmental"},
            "locations": [{"label": "Sydney NSW"}],
            "salaryLabel": "$75,000 – $85,000 + super",
            "teaser": "Join our contaminated land team.",
            "bulletPoints": ["Field sampling", "Report writing"],
            "listingDate": "2026-11-20T01:00:00Z",
        },
        {
            "id": "80000002",
            "title": "Senior Manager Sustainability",
            "companyName": "BigCo",
            "location": "Melbourne",
            "area": "CBD & Inner Suburbs",
        },
        {
            "id": "80000003",
            "title": "Environmental Officer",
            "advertiser": {"description": "Wellington Council"},
            "locations": [{"label": "Wellington, New Zealand"}],
        },
        {"id": None, "title": "broken row"},
    ]
}


def test_parse_search_response():
    jobs = seek.parse_search_response(SAMPLE)
    assert len(jobs) == 3
    j = jobs[0]
    assert j["url"] == "https://www.seek.com.au/job/80000001"
    assert j["company"] == "Acme Environmental"
    assert j["location"] == "Sydney NSW"
    assert "+ super" in j["salary"]
    assert "- Field sampling" in j["description"]
    assert jobs[1]["company"] == "BigCo"
    assert jobs[1]["location"] == "CBD & Inner Suburbs, Melbourne"


def test_run_seek_discovery_end_to_end(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    monkeypatch.setattr(seek, "get_connection", lambda: _conn(db))
    monkeypatch.setattr(seek, "init_db", lambda: _init(db))
    monkeypatch.setattr(seek.time, "sleep", lambda s: None)

    calls = []

    def handler(request: httpx.Request):
        calls.append(dict(request.url.params))
        page = int(request.url.params["page"])
        return httpx.Response(200, json=SAMPLE if page == 1 else {"data": []})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = {
        "seek": {"enabled": True, "pages_per_query": 3, "days_old": 3},
        "queries": [{"query": "environmental officer", "tier": 1}],
        "locations": [{"label": "sydney", "location": "Sydney NSW"}],
        "location_accept": ["NSW", "VIC", "Melbourne", "Australia"],
        "location_reject_non_remote": ["New Zealand"],
        "exclude_titles": ["senior manager"],
    }
    stats = seek.run_seek_discovery(cfg, client=client)

    assert calls[0]["where"] == "All Sydney NSW"
    assert calls[0]["daterange"] == "3"
    assert len(calls) == 2            # stops after an empty page
    assert stats["new"] == 1          # senior manager + NZ filtered out
    assert stats["filtered"] == 2

    # Re-running is idempotent
    stats2 = seek.run_seek_discovery(cfg, client=client)
    assert stats2["new"] == 0 and stats2["existing"] == 1


def test_disabled_by_default():
    assert seek.run_seek_discovery({"queries": []})["skipped"] is True


def _conn(db):
    from applypilot.database import get_connection
    return get_connection(db)


def _init(db):
    from applypilot.database import init_db
    return init_db(db)
