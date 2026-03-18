#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import UTC, datetime
from html import escape
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from string import Template
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT.parent / "analysis.sqlite3"
DEFAULT_OUTPUT_DIR = ROOT / "site"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Polsia Hub static site.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Input SQLite DB")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Static site output directory",
    )
    parser.add_argument("--serve", action="store_true", help="Serve the site after building")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP server host")
    parser.add_argument("--port", type=int, default=8787, help="HTTP server port")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")


def parse_url(raw: str) -> tuple[str, str, str, str]:
    url = raw.strip()
    if not url:
        return "", "", "", ""
    parsed = urlparse(url)
    if not parsed.scheme:
        parsed = urlparse("https://" + url)
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if path == "/":
        path = ""
    elif path.endswith("/"):
        path = path[:-1]
    canonical = f"{scheme}://{host}{path}"
    return canonical, scheme, host, path


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [item.strip() for item in value.split(",")]
    return [item for item in parts if item]


def unique_sorted(items: list[str]) -> list[str]:
    seen: dict[str, None] = {}
    for item in items:
        if item:
            seen.setdefault(item, None)
    return sorted(seen)


def query_company_rows(conn: sqlite3.Connection) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            lower(app_url) AS canonical_url,
            MIN(snapshot_ts) AS first_seen,
            MAX(snapshot_ts) AS last_seen,
            COUNT(*) AS observations,
            COUNT(DISTINCT company_id) AS company_count,
            group_concat(DISTINCT company_id) AS company_ids,
            group_concat(DISTINCT name) AS names,
            group_concat(DISTINCT slug) AS slugs
        FROM company_observations
        WHERE app_url IS NOT NULL
          AND lower(app_url) LIKE '%polsia.app%'
        GROUP BY lower(app_url)
        ORDER BY observations DESC, canonical_url ASC
        """
    ).fetchall()

    records: list[dict[str, object]] = []
    for canonical_url, first_seen, last_seen, observations, company_count, company_ids, names, slugs in rows:
        parsed = urlparse(canonical_url)
        records.append(
            {
                "canonical_url": canonical_url,
                "host": parsed.netloc,
                "path": parsed.path or "",
                "first_seen": first_seen,
                "last_seen": last_seen,
                "observations": int(observations),
                "company_count": int(company_count),
                "company_ids": [int(x) for x in split_csv(company_ids)],
                "names": unique_sorted(split_csv(names)),
                "slugs": unique_sorted(split_csv(slugs)),
            }
        )
    return records


def query_link_rows(conn: sqlite3.Connection) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            lower(url) AS canonical_url,
            MIN(snapshot_ts) AS first_seen,
            MAX(snapshot_ts) AS last_seen,
            COUNT(*) AS observations,
            COUNT(DISTINCT company_label) AS company_label_count,
            group_concat(DISTINCT company_label) AS company_labels,
            group_concat(DISTINCT title) AS titles
        FROM link_observations
        WHERE url IS NOT NULL
          AND lower(url) LIKE '%polsia.app%'
        GROUP BY lower(url)
        ORDER BY observations DESC, canonical_url ASC
        """
    ).fetchall()

    records: list[dict[str, object]] = []
    for canonical_url, first_seen, last_seen, observations, company_label_count, company_labels, titles in rows:
        parsed = urlparse(canonical_url)
        records.append(
            {
                "canonical_url": canonical_url,
                "host": parsed.netloc,
                "path": parsed.path or "",
                "first_seen": first_seen,
                "last_seen": last_seen,
                "observations": int(observations),
                "company_label_count": int(company_label_count),
                "company_labels": unique_sorted(split_csv(company_labels)),
                "titles": unique_sorted(split_csv(titles)),
            }
        )
    return records


def merge_url_rows(company_rows: list[dict[str, object]], link_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}

    for row in company_rows:
        canonical_url = str(row["canonical_url"])
        host = str(row["host"])
        path = str(row["path"])
        merged[canonical_url] = {
            "canonical_url": canonical_url,
            "host": host,
            "path": path,
            "kind": "company",
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "observations": row["observations"],
            "company_ids": row["company_ids"],
            "names": row["names"],
            "slugs": row["slugs"],
            "company_labels": [],
            "titles": [],
        }

    for row in link_rows:
        canonical_url = str(row["canonical_url"])
        host = str(row["host"])
        path = str(row["path"])
        current = merged.get(canonical_url)
        if current is None:
            merged[canonical_url] = {
                "canonical_url": canonical_url,
                "host": host,
                "path": path,
                "kind": "link",
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen"],
                "observations": row["observations"],
                "company_ids": [],
                "names": [],
                "slugs": [],
                "company_labels": row["company_labels"],
                "titles": row["titles"],
            }
            continue

        current["kind"] = "company+link"
        current["first_seen"] = min(str(current["first_seen"]), str(row["first_seen"]))
        current["last_seen"] = max(str(current["last_seen"]), str(row["last_seen"]))
        current["observations"] = int(current["observations"]) + int(row["observations"])
        current["company_labels"] = unique_sorted(list(current["company_labels"]) + list(row["company_labels"]))
        current["titles"] = unique_sorted(list(current["titles"]) + list(row["titles"]))

    return sorted(
        merged.values(),
        key=lambda item: (-int(item["observations"]), str(item["canonical_url"])),
    )


def build_host_index(url_rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    hosts: dict[str, dict[str, object]] = {}
    for row in url_rows:
        host = str(row["host"])
        if not host:
            continue
        entry = hosts.setdefault(
            host,
            {
                "host": host,
                "url_count": 0,
                "path_count": 0,
                "paths": set(),
                "sample_urls": [],
            },
        )
        entry["url_count"] += 1
        path = str(row["path"])
        if path:
            entry["paths"].add(path)
            if len(entry["sample_urls"]) < 6:
                entry["sample_urls"].append(str(row["canonical_url"]))

    for entry in hosts.values():
        entry["path_count"] = len(entry["paths"])
        entry["paths"] = sorted(entry["paths"])
        entry["sample_urls"] = sorted(entry["sample_urls"])
    return hosts


def build_site_rows(company_rows: list[dict[str, object]], host_index: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    sites: list[dict[str, object]] = []
    for row in company_rows:
        canonical_url = str(row["canonical_url"])
        host = str(row["host"])
        host_stats = host_index.get(host, {})
        names = row["names"] or []
        slugs = row["slugs"] or []
        label = ", ".join(names[:2]) if names else host
        root_url = canonical_url
        sites.append(
            {
                "root_url": root_url,
                "host": host,
                "label": label,
                "names": names,
                "slugs": slugs,
                "company_ids": row["company_ids"],
                "company_count": row["company_count"],
                "observations": row["observations"],
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen"],
                "url_count": int(host_stats.get("url_count", 0)),
                "path_count": int(host_stats.get("path_count", 0)),
                "sample_paths": list(host_stats.get("paths", [])[:6]),
                "sample_urls": list(host_stats.get("sample_urls", [])[:6]),
            }
        )

    return sorted(
        sites,
        key=lambda item: (-int(item["observations"]), str(item["root_url"]))
    )


SITE_TEMPLATE = Template(
    """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Polsia Hub</title>
    <meta name="description" content="Observation dashboard for the polsia.app site inventory." />
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600;9..144,700&family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');

      :root {
        color-scheme: dark;
        --bg: #08110f;
        --bg2: #0e1915;
        --panel: rgba(16, 28, 23, 0.84);
        --panel-2: rgba(20, 33, 28, 0.92);
        --line: rgba(197, 228, 210, 0.12);
        --text: #e7f4eb;
        --muted: #96b0a0;
        --accent: #8ee3a2;
        --accent-2: #f2c98e;
        --danger: #f0a3a3;
        --shadow: 0 24px 90px rgba(0, 0, 0, 0.34);
      }

      * { box-sizing: border-box; }
      html { scroll-behavior: smooth; }
      body {
        margin: 0;
        color: var(--text);
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
        background:
          radial-gradient(circle at 14% 8%, rgba(142, 227, 162, 0.10), transparent 24%),
          radial-gradient(circle at 86% 12%, rgba(242, 201, 142, 0.08), transparent 22%),
          linear-gradient(180deg, #07110f 0%, #0c1714 38%, #09110f 100%);
      }

      a { color: inherit; }
      .shell {
        width: min(1400px, calc(100vw - 28px));
        margin: 0 auto;
        padding: 20px 0 48px;
      }

      .hero {
        position: relative;
        overflow: hidden;
        border: 1px solid var(--line);
        border-radius: 28px;
        padding: 28px;
        background:
          linear-gradient(135deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02)),
          linear-gradient(140deg, rgba(142,227,162,0.10), rgba(20,33,28,0.95));
        box-shadow: var(--shadow);
      }

      .hero::after {
        content: "";
        position: absolute;
        inset: auto -80px -120px auto;
        width: 280px;
        height: 280px;
        border-radius: 50%;
        background: rgba(142, 227, 162, 0.08);
        filter: blur(16px);
      }

      .hero-top {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 20px;
      }

      .eyebrow {
        margin: 0 0 10px;
        font-family: "IBM Plex Mono", monospace;
        text-transform: uppercase;
        letter-spacing: 0.18em;
        font-size: 0.74rem;
        color: var(--muted);
      }

      h1, h2, h3 {
        margin: 0;
        font-family: "Fraunces", Georgia, serif;
        line-height: 0.96;
        letter-spacing: -0.03em;
      }

      h1 {
        font-size: clamp(3rem, 7vw, 6.4rem);
        max-width: 10ch;
      }

      .hero-copy {
        margin-top: 18px;
        max-width: 66ch;
        font-size: 1.02rem;
        line-height: 1.65;
        color: #d4e4d9;
      }

      .source-note {
        margin-top: 22px;
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
      }

      .badge {
        border: 1px solid var(--line);
        background: rgba(7, 17, 15, 0.54);
        border-radius: 999px;
        padding: 9px 13px;
        color: var(--muted);
        font-size: 0.9rem;
      }

      .badge strong {
        color: var(--text);
        font-weight: 600;
      }

      .panel-grid {
        margin-top: 18px;
        display: grid;
        grid-template-columns: repeat(12, 1fr);
        gap: 16px;
      }

      .panel {
        border: 1px solid var(--line);
        border-radius: 22px;
        background: var(--panel);
        box-shadow: var(--shadow);
        backdrop-filter: blur(10px);
      }

      .metric {
        grid-column: span 3;
        padding: 18px;
      }

      .metric .kicker {
        margin: 0 0 10px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.12em;
        font-size: 0.72rem;
        font-family: "IBM Plex Mono", monospace;
      }

      .metric .value {
        font-family: "Fraunces", Georgia, serif;
        font-size: clamp(2rem, 3vw, 3.4rem);
      }

      .metric .sub {
        margin-top: 6px;
        color: var(--muted);
        line-height: 1.5;
      }

      .controls {
        margin-top: 18px;
        padding: 18px;
      }

      .control-row {
        display: grid;
        grid-template-columns: 1fr 1fr auto;
        gap: 12px;
      }

      .control-row input {
        width: 100%;
        border: 1px solid rgba(197, 228, 210, 0.14);
        border-radius: 14px;
        background: rgba(5, 10, 9, 0.68);
        color: var(--text);
        padding: 13px 14px;
        font: inherit;
        outline: none;
      }

      .control-row input:focus {
        border-color: rgba(142, 227, 162, 0.6);
        box-shadow: 0 0 0 3px rgba(142, 227, 162, 0.10);
      }

      .control-row button {
        border: 1px solid rgba(197, 228, 210, 0.16);
        border-radius: 14px;
        background: linear-gradient(180deg, rgba(142, 227, 162, 0.18), rgba(142, 227, 162, 0.08));
        color: var(--text);
        padding: 12px 16px;
        font-weight: 600;
        cursor: pointer;
      }

      .section {
        margin-top: 18px;
        padding: 18px;
      }

      .section-head {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 14px;
      }

      .section-head p {
        margin: 0;
        color: var(--muted);
        line-height: 1.5;
      }

      .table-wrap {
        overflow: auto;
        border: 1px solid rgba(197, 228, 210, 0.08);
        border-radius: 18px;
        background: rgba(4, 8, 7, 0.48);
      }

      table {
        width: 100%;
        border-collapse: collapse;
        min-width: 1080px;
      }

      thead th {
        position: sticky;
        top: 0;
        z-index: 1;
        background: rgba(8, 17, 15, 0.98);
        color: var(--muted);
        text-align: left;
        font-size: 0.74rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        font-family: "IBM Plex Mono", monospace;
        padding: 14px 14px;
        border-bottom: 1px solid rgba(197, 228, 210, 0.08);
      }

      tbody td {
        padding: 13px 14px;
        vertical-align: top;
        border-bottom: 1px solid rgba(197, 228, 210, 0.06);
      }

      tbody tr:hover {
        background: rgba(142, 227, 162, 0.04);
      }

      .url-link {
        text-decoration: none;
      }

      .mono {
        font-family: "IBM Plex Mono", monospace;
        font-size: 0.92rem;
      }

      .muted {
        color: var(--muted);
      }

      .chips {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
      }

      .chip {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        border: 1px solid rgba(197, 228, 210, 0.12);
        border-radius: 999px;
        padding: 5px 9px;
        font-family: "IBM Plex Mono", monospace;
        font-size: 0.74rem;
        color: var(--accent);
        background: rgba(142, 227, 162, 0.05);
      }

      .chip.alt {
        color: var(--accent-2);
        background: rgba(242, 201, 142, 0.05);
      }

      .footer {
        margin-top: 18px;
        color: var(--muted);
        font-size: 0.9rem;
        line-height: 1.6;
      }

      .hidden { display: none; }

      @media (max-width: 1100px) {
        .metric { grid-column: span 6; }
        .control-row { grid-template-columns: 1fr; }
        .hero-top { flex-direction: column; }
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <section class="hero panel">
        <div class="hero-top">
          <div>
            <p class="eyebrow">Economy observation dashboard</p>
            <h1>Polsia Hub</h1>
            <p class="hero-copy">
              Live inventory of every `polsia.app` website and URL observed in the derived analysis
              database. This page is built directly from `analysis/analysis.sqlite3` and published as
              a static LAN dashboard.
            </p>
            <div class="source-note">
              <span class="badge"><strong>$site_count</strong> company sites</span>
              <span class="badge"><strong>$url_count</strong> unique URLs</span>
              <span class="badge"><strong>$company_count</strong> company observation rows</span>
              <span class="badge"><strong>$link_count</strong> link observation rows</span>
              <span class="badge">latest snapshot <strong>$latest_snapshot</strong></span>
            </div>
          </div>
          <div>
            <div class="badge" style="max-width: 34ch;">
              Full export: <span class="mono">data.json</span><br />
              Source DB: <span class="mono">$db_path</span><br />
              Generated: <span class="mono">$generated_at</span>
            </div>
          </div>
        </div>
      </section>

      <section class="panel-grid">
        <article class="panel metric">
          <p class="kicker">Sites</p>
          <div class="value" id="metric-sites">0</div>
          <div class="sub">Distinct company app URLs under `polsia.app`.</div>
        </article>
        <article class="panel metric">
          <p class="kicker">URLs</p>
          <div class="value" id="metric-urls">0</div>
          <div class="sub">Combined company and link URLs after dedupe.</div>
        </article>
        <article class="panel metric">
          <p class="kicker">Path Rows</p>
          <div class="value" id="metric-paths">0</div>
          <div class="sub">Non-root URL observations across the host set.</div>
        </article>
        <article class="panel metric">
          <p class="kicker">Hosts</p>
          <div class="value" id="metric-hosts">0</div>
          <div class="sub">Unique `polsia.app` hostnames in the inventory.</div>
        </article>
      </section>

      <section class="panel controls">
        <div class="control-row">
          <input id="site-search" type="search" placeholder="Search sites by name, slug, host, or URL" />
          <input id="url-search" type="search" placeholder="Search URLs, labels, or titles" />
          <button id="reset-filters" type="button">Reset</button>
        </div>
      </section>

      <section class="panel section">
        <div class="section-head">
          <div>
            <h2>Site Inventory</h2>
            <p>One row per company app URL. Sorted by observation volume.</p>
          </div>
          <p id="site-count" class="mono"></p>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Site</th>
                <th>URL</th>
                <th>Names</th>
                <th>Slugs</th>
                <th>Seen</th>
                <th>Evidence</th>
                <th>Host Surface</th>
              </tr>
            </thead>
            <tbody id="sites-body"></tbody>
          </table>
        </div>
      </section>

      <section class="panel section">
        <div class="section-head">
          <div>
            <h2>URL Inventory</h2>
            <p>Combined company and link URLs. Use the URL search field to filter.</p>
          </div>
          <p id="url-count" class="mono"></p>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>URL</th>
                <th>Kind</th>
                <th>Names / Titles</th>
                <th>Seen</th>
                <th>Observations</th>
              </tr>
            </thead>
            <tbody id="urls-body"></tbody>
          </table>
        </div>
      </section>

      <p class="footer">
        The dashboard source is `analysis/polsia_hub/build_site.py`. Rebuild it from the derived
        SQLite corpus, then serve the output directory on the LAN.
      </p>
    </div>

    <script>
      const DATA_URL = 'data.json';
      let DATA = null;
      const siteBody = document.getElementById('sites-body');
      const urlBody = document.getElementById('urls-body');
      const siteSearch = document.getElementById('site-search');
      const urlSearch = document.getElementById('url-search');
      const resetButton = document.getElementById('reset-filters');

      function textOf(values) {
        if (!values || !values.length) return '';
        return values.join(' | ');
      }

      function fmtSeen(firstSeen, lastSeen) {
        return firstSeen + '<br><span class="muted mono">' + lastSeen + '</span>';
      }

      function siteMatches(item, term) {
        if (!term) return true;
        const haystack = [
          item.label,
          item.host,
          item.root_url,
          textOf(item.names),
          textOf(item.slugs),
          textOf(item.sample_urls),
          textOf(item.sample_paths)
        ].join(' ').toLowerCase();
        return haystack.indexOf(term) !== -1;
      }

      function urlMatches(item, term) {
        if (!term) return true;
        const haystack = [
          item.canonical_url,
          item.kind,
          textOf(item.names),
          textOf(item.titles),
          textOf(item.company_labels)
        ].join(' ').toLowerCase();
        return haystack.indexOf(term) !== -1;
      }

      function renderSites(term) {
        const rows = DATA.sites.filter(function(item) { return siteMatches(item, term); });
        document.getElementById('site-count').textContent = rows.length + ' matching sites';
        const limit = rows.length;
        const html = rows.slice(0, limit).map(function(item) {
          const names = textOf(item.names) || item.label;
          const slugs = textOf(item.slugs) || '—';
          const surface = [
            '<span class="chip">' + item.url_count + ' URLs</span>',
            '<span class="chip alt">' + item.path_count + ' paths</span>'
          ].join('');
          const samplePaths = item.sample_paths.length ? '<div class="chips" style="margin-top:8px;">' + item.sample_paths.slice(0, 4).map(function(path) {
            return '<span class="chip alt">' + path + '</span>';
          }).join('') + '</div>' : '<span class="muted">No extra paths</span>';
          return [
            '<tr>',
            '<td><div><a class="url-link" href="' + item.root_url + '" target="_blank" rel="noreferrer"><strong>' + item.label + '</strong></a></div><div class="muted mono">' + item.host + '</div></td>',
            '<td class="mono"><a class="url-link" href="' + item.root_url + '" target="_blank" rel="noreferrer">' + item.root_url + '</a></td>',
            '<td>' + names + '</td>',
            '<td class="mono">' + slugs + '</td>',
            '<td class="mono">' + fmtSeen(item.first_seen, item.last_seen) + '</td>',
            '<td><span class="chip">' + item.observations + '</span><span class="chip alt">' + item.company_count + ' companies</span></td>',
            '<td>' + surface + samplePaths + '</td>',
            '</tr>'
          ].join('');
        }).join('');
        siteBody.innerHTML = html;
      }

      function renderUrls(term) {
        const rows = DATA.urls.filter(function(item) { return urlMatches(item, term); });
        document.getElementById('url-count').textContent = rows.length + ' matching URLs';
        const limit = term ? rows.length : Math.min(rows.length, 300);
        const html = rows.slice(0, limit).map(function(item) {
          const names = textOf(item.names);
          const titles = textOf(item.titles);
          const labels = textOf(item.company_labels);
          const meta = [names, titles, labels].filter(Boolean).join(' | ') || '—';
          return [
            '<tr>',
            '<td class="mono"><a class="url-link" href="' + item.canonical_url + '" target="_blank" rel="noreferrer">' + item.canonical_url + '</a><div class="muted">' + item.host + '</div></td>',
            '<td><span class="chip">' + item.kind + '</span></td>',
            '<td>' + meta + '</td>',
            '<td class="mono">' + fmtSeen(item.first_seen, item.last_seen) + '</td>',
            '<td><span class="chip">' + item.observations + '</span></td>',
            '</tr>'
          ].join('');
        }).join('');
        if (!term && rows.length > limit) {
          urlBody.innerHTML = html + '<tr><td colspan="5" class="muted">Showing top ' + limit + ' rows. Search to filter the full inventory.</td></tr>';
        } else {
          urlBody.innerHTML = html;
        }
      }

      function renderMetrics() {
        document.getElementById('metric-sites').textContent = DATA.sites.length.toLocaleString();
        document.getElementById('metric-urls').textContent = DATA.urls.length.toLocaleString();
        document.getElementById('metric-paths').textContent = DATA.summary.path_count.toLocaleString();
        document.getElementById('metric-hosts').textContent = DATA.summary.host_count.toLocaleString();
      }

      function applyFilters() {
        const siteTerm = siteSearch.value.trim().toLowerCase();
        const urlTerm = urlSearch.value.trim().toLowerCase();
        renderSites(siteTerm);
        renderUrls(urlTerm);
      }

      siteSearch.addEventListener('input', applyFilters);
      urlSearch.addEventListener('input', applyFilters);
      resetButton.addEventListener('click', function() {
        siteSearch.value = '';
        urlSearch.value = '';
        applyFilters();
      });

      fetch(DATA_URL)
        .then(function(resp) { return resp.json(); })
        .then(function(data) {
          DATA = data;
          renderMetrics();
          applyFilters();
        })
        .catch(function(err) {
          siteBody.innerHTML = '<tr><td colspan="7">Failed to load data.json: ' + err + '</td></tr>';
          urlBody.innerHTML = '<tr><td colspan="5">Failed to load data.json: ' + err + '</td></tr>';
        });
    </script>
  </body>
</html>
"""
)


def build_site(db_path: Path, output_dir: Path) -> dict[str, object]:
    conn = sqlite3.connect(str(db_path))
    company_rows = query_company_rows(conn)
    link_rows = query_link_rows(conn)
    snapshots = conn.execute("SELECT MAX(snapshot_ts) FROM company_observations").fetchone()[0]
    company_count = conn.execute("SELECT COUNT(*) FROM company_observations").fetchone()[0]
    link_count = conn.execute("SELECT COUNT(*) FROM link_observations").fetchone()[0]
    conn.close()

    url_rows = merge_url_rows(company_rows, link_rows)
    host_index = build_host_index(url_rows)
    sites = build_site_rows(company_rows, host_index)

    all_paths = set()
    path_observations = 0
    for row in url_rows:
        path = str(row["path"])
        if path:
            all_paths.add(path)
            path_observations += int(row["observations"])

    summary = {
        "host_count": len(host_index),
        "path_count": path_observations,
        "unique_path_count": len(all_paths),
        "company_rows": company_count,
        "link_rows": link_count,
        "site_count": len(sites),
        "url_count": len(url_rows),
    }

    payload = {
        "generated_at": utc_now(),
        "latest_snapshot": snapshots,
        "source_db": str(db_path),
        "summary": summary,
        "sites": sites,
        "urls": url_rows,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "data.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "index.html").write_text(
        SITE_TEMPLATE.substitute(
            site_count=f"{len(sites):,}",
            url_count=f"{len(url_rows):,}",
            company_count=f"{company_count:,}",
            link_count=f"{link_count:,}",
            latest_snapshot=escape(str(snapshots)),
            db_path=escape(str(db_path)),
            generated_at=escape(utc_now()),
        ),
        encoding="utf-8",
    )

    return payload


def serve(output_dir: Path, host: str, port: int) -> None:
    os.chdir(str(output_dir))

    class QuietHandler(SimpleHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer((host, port), QuietHandler)
    print(f"Polsia Hub serving on http://{host}:{port}", flush=True)
    print(f"  directory: {output_dir}", flush=True)
    server.serve_forever()


def main() -> int:
    args = parse_args()
    payload = build_site(args.db, args.output_dir)
    print(
        f"built sites={len(payload['sites'])} urls={len(payload['urls'])} "
        f"hosts={payload['summary']['host_count']} paths={payload['summary']['path_count']}"
    )
    if args.serve:
        serve(args.output_dir, args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
