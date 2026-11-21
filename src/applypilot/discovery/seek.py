"""SEEK (seek.com.au) job discovery.

JobSpy has no SEEK support, so this module queries SEEK's public job-search
endpoint (the same JSON the seek.com.au search page loads) and stores results
in the ApplyPilot jobs table.

Discovery only. It never logs in and never applies. Full descriptions are left
empty so the normal `enrich` stage fetches each job page, exactly like other
sources.

Config (searches.yaml):

    seek:
      enabled: true
      pages_per_query: 2        # 20-ish results per page
      days_old: 3               # SEEK "daterange" filter
      delay_seconds: 2.0        # pause between requests - be polite
      where_map:                # optional: location label -> SEEK "where" text
        sydney: "All Sydney NSW"
        remote_au: "All Australia"

Queries, tiers, locations, location filters and exclude_titles are shared
with the JobSpy config, so one searches.yaml drives every source.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx

from applypilot import config
from applypilot.database import get_connection, init_db

log = logging.getLogger(__name__)

SEEK_SEARCH_URL = "https://www.seek.com.au/api/jobsearch/v5/search"
SEEK_JOB_URL = "https://www.seek.com.au/job/{id}"
SITE_LABEL = "seek"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.seek.com.au/",
}

_DEFAULT_WHERE = {
    "sydney": "All Sydney NSW",
    "melbourne": "All Melbourne VIC",
    "brisbane": "All Brisbane QLD",
    "canberra": "All Canberra ACT",
    "perth": "All Perth WA",
    "adelaide": "All Adelaide SA",
    "remote_au": "All Australia",
}


# -- Parsing -----------------------------------------------------------------

def _first(*values: Any) -> Any:
    """Return the first value that is not None/empty."""
    for v in values:
        if v not in (None, "", [], {}):
            return v
    return None


def _location_text(job: dict) -> str | None:
    """SEEK has shipped a few location shapes over time; handle all of them."""
    locs = job.get("locations")
    if isinstance(locs, list) and locs:
        labels = [l.get("label") for l in locs if isinstance(l, dict) and l.get("label")]
        if labels:
            return ", ".join(labels)
    loc = job.get("location")
    if isinstance(loc, dict):
        loc = loc.get("label")
    area = job.get("area")
    if isinstance(area, dict):
        area = area.get("label") or area.get("description")
    parts = [p for p in (area, loc) if isinstance(p, str) and p]
    return ", ".join(dict.fromkeys(parts)) or None


def _company_text(job: dict) -> str | None:
    adv = job.get("advertiser")
    adv_name = adv.get("description") if isinstance(adv, dict) else None
    return _first(job.get("companyName"), adv_name)


def _salary_text(job: dict) -> str | None:
    sal = job.get("salary")
    if isinstance(sal, dict):
        sal = sal.get("label")
    return _first(job.get("salaryLabel"), sal)


def _is_remote(job: dict) -> bool:
    arr = job.get("workArrangements")
    text = json.dumps(arr).lower() if arr else ""
    return "remote" in text or "work from home" in text


def parse_search_response(payload: dict) -> list[dict]:
    """Turn a SEEK search JSON payload into ApplyPilot job dicts."""
    out: list[dict] = []
    for job in payload.get("data") or []:
        if not isinstance(job, dict):
            continue
        job_id = job.get("id")
        title = job.get("title")
        if not job_id or not title:
            continue

        bullets = job.get("bulletPoints") or []
        teaser = job.get("teaser") or ""
        description = "\n".join([teaser, *[f"- {b}" for b in bullets if isinstance(b, str)]]).strip()

        location = _location_text(job)
        if _is_remote(job):
            location = f"{location} (Remote)" if location else "Remote"

        out.append({
            "url": SEEK_JOB_URL.format(id=job_id),
            "title": title.strip(),
            "company": _company_text(job),
            "salary": _salary_text(job),
            "description": description or None,
            "location": location,
            "listed_at": job.get("listingDate"),
        })
    return out


# -- Filters (shared semantics with jobspy.py) -------------------------------

def _location_ok(location: str | None, accept: list[str], reject: list[str]) -> bool:
    if not location:
        return True
    loc = location.lower()
    if any(r in loc for r in ("remote", "work from home", "wfh", "anywhere")):
        return True
    if any(r.lower() in loc for r in reject):
        return False
    if not accept:
        return True
    return any(a.lower() in loc for a in accept)


def _title_ok(title: str, exclude_titles: Iterable[str]) -> bool:
    t = title.lower()
    return not any(x.lower() in t for x in exclude_titles)


# -- Fetching ----------------------------------------------------------------

def fetch_page(client: httpx.Client, keywords: str, where: str, page: int,
               days_old: int | None) -> dict:
    params = {
        "siteKey": "AU-Main",
        "keywords": keywords,
        "where": where,
        "page": page,
        "sortmode": "ListedDate",
        "locale": "en-AU",
    }
    if days_old:
        params["daterange"] = days_old
    resp = client.get(SEEK_SEARCH_URL, params=params)
    resp.raise_for_status()
    return resp.json()


# -- Storage -----------------------------------------------------------------

def store_seek_jobs(conn: sqlite3.Connection, jobs: list[dict], query: str) -> tuple[int, int]:
    now = datetime.now(timezone.utc).isoformat()
    new = existing = 0
    for j in jobs:
        try:
            conn.execute(
                "INSERT INTO jobs (url, title, company, salary, description, location, "
                "site, strategy, discovered_at, application_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (j["url"], j["title"], j["company"], j["salary"], j["description"],
                 j["location"], SITE_LABEL, f"seek:{query}", now, j["url"]),
            )
            new += 1
        except sqlite3.IntegrityError:
            existing += 1
    conn.commit()
    return new, existing


# -- Entry point -------------------------------------------------------------

def run_seek_discovery(cfg: dict | None = None, client: httpx.Client | None = None) -> dict:
    """Search SEEK for every configured query x location and store results."""
    cfg = config.normalize_search_config(cfg if cfg is not None else config.load_search_config())
    seek_cfg = cfg.get("seek") or {}
    if not seek_cfg.get("enabled", False):
        log.info("SEEK discovery disabled (set seek.enabled: true in searches.yaml)")
        return {"new": 0, "existing": 0, "errors": 0, "skipped": True}

    pages = int(seek_cfg.get("pages_per_query", 2))
    days_old = seek_cfg.get("days_old", 3)
    delay = float(seek_cfg.get("delay_seconds", 2.0))
    max_tier = seek_cfg.get("max_tier")
    where_map = {**_DEFAULT_WHERE, **(seek_cfg.get("where_map") or {})}

    accept, reject = config.get_location_filters(cfg)
    exclude_titles = cfg.get("exclude_titles") or []

    queries = [q for q in cfg.get("queries", [])
               if max_tier is None or int(q.get("tier", 1)) <= int(max_tier)]
    locations = cfg.get("locations", [])

    init_db()
    conn = get_connection()
    own_client = client is None
    client = client or httpx.Client(headers=_HEADERS, timeout=20.0, follow_redirects=True)

    totals = {"new": 0, "existing": 0, "errors": 0, "filtered": 0}
    try:
        for q in queries:
            keywords = q["query"]
            for loc in locations:
                label = loc.get("label", "")
                where = where_map.get(label) or loc.get("location", "All Australia")
                for page in range(1, pages + 1):
                    try:
                        payload = fetch_page(client, keywords, where, page, days_old)
                    except Exception as e:  # network / 403 / 429 / bad JSON
                        totals["errors"] += 1
                        log.warning("[seek] %s @ %s p%d failed: %s", keywords, where, page, e)
                        if "403" in str(e) or "429" in str(e):
                            log.error("[seek] blocked or rate-limited - stopping SEEK discovery")
                            return totals
                        break
                    jobs = parse_search_response(payload)
                    if not jobs:
                        break
                    kept = [j for j in jobs
                            if _location_ok(j["location"], accept, reject)
                            and _title_ok(j["title"], exclude_titles)]
                    totals["filtered"] += len(jobs) - len(kept)
                    new, existing = store_seek_jobs(conn, kept, keywords)
                    totals["new"] += new
                    totals["existing"] += existing
                    log.info("[seek] %s @ %s p%d: %d results -> %d new, %d dupes",
                             keywords, where, page, len(jobs), new, existing)
                    time.sleep(delay)
    finally:
        if own_client:
            client.close()

    log.info("[seek] done: %s", totals)
    return totals
