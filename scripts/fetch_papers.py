#!/usr/bin/env python3
"""
Retirement Research Digest — Paper Fetcher

Aggregates recent economics research relevant to retirement researchers from:
1. OpenAlex API (concept-filtered + source-filtered queries)
2. NBER RSS feed (new working papers)

Outputs docs/data/papers.json for the static frontend.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import feedparser
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAILTO = "colavito@crfb.org"
OPENALEX_BASE = "https://api.openalex.org/works"
NBER_RSS_URL = "https://back.nber.org/rss/new.xml"
RETENTION_DAYS = 180  # 6 months
PAGE_SIZE = 200
MAX_PAGES = 10  # safety cap per query

# OpenAlex concept IDs relevant to retirement research
CONCEPT_IDS = [
    "C2779356",    # Social security
    "C2776325",    # Pension
    "C2776891",    # Retirement
    "C141071460",  # Disability insurance
    "C2780893",    # Defined benefit pension plan
    "C2780586",    # Defined contribution plan
    "C93843015",   # Annuity
    "C2776466",    # Life-cycle hypothesis
    "C2776466",    # Precautionary saving (covered by life-cycle)
    "C71924100",   # Longevity
    "C2779708",    # Medicare
    "C144133560",  # Old age
    "C153180895",  # Social insurance
]
# Deduplicate
CONCEPT_IDS = list(dict.fromkeys(CONCEPT_IDS))

# Level 0/1 concepts that indicate a paper is in economics.
# Used to filter out false positives (e.g., biology papers tagged with "longevity").
# Intentionally narrow: excludes "demography", "sociology", "law" which are too broad.
ECON_DOMAIN_CONCEPTS = {
    "economics", "finance", "actuarial science", "monetary economics",
    "microeconomics", "macroeconomics", "econometrics",
    "public economics", "labour economics", "labor economics",
    "welfare economics", "economic growth", "political economy",
}

# Target journal source IDs in OpenAlex
SOURCE_IDS = {
    "S2809516038": "National Bureau of Economic Research",
    "S23254222":   "American Economic Review",
    "S203860005":  "The Quarterly Journal of Economics",
    "S158011328":  "AEJ: Economic Policy",
    "S199447588":  "Journal of Public Economics",
    "S95323914":   "Journal of Political Economy",
    "S4210172589": "SSRN Electronic Journal",
}

# ---------------------------------------------------------------------------
# Topic Classification
# ---------------------------------------------------------------------------

TOPICS = {
    "public_pensions": {
        "label": "Public Pensions / Social Security",
        "keywords": [
            r"social\s+security", r"oasdi", r"payroll\s+tax", r"trust\s+fund",
            r"claiming\s+age", r"full\s+retirement\s+age", r"fra\b",
            r"social\s+security\s+benefit", r"ssa\b", r"public\s+pension",
            r"state\s+pension", r"old[\s-]age\s+insurance",
            r"social\s+security\s+reform", r"social\s+security\s+wealth",
            r"bend\s+point", r"pia\b", r"aime\b",
        ],
        "exclude": [
            r"network\s+security", r"cyber\s+security", r"food\s+security",
            r"security\s+market", r"security\s+price",
        ],
        "concept_names": ["social security", "social security in the united states"],
    },
    "consumption_savings": {
        "label": "Consumption-Savings / Life Cycle",
        "keywords": [
            r"life[\s-]cycle", r"precautionary\s+sav", r"wealth\s+accumulation",
            r"bequest\s+motive", r"saving\s+rate", r"consumption\s+smooth",
            r"wealth\s+inequality", r"household\s+saving", r"buffer[\s-]stock",
            r"wealth\s+decumulation", r"retirement\s+saving",
            r"retirement\s+wealth", r"consumption\s+in\s+retirement",
        ],
        "exclude": [
            r"energy\s+consumption", r"drug\s+consumption", r"alcohol\s+consumption",
            r"water\s+consumption", r"food\s+consumption", r"meat\s+consumption",
            r"media\s+consumption",
        ],
        "concept_names": ["life-cycle hypothesis", "precautionary saving"],
    },
    "social_insurance": {
        "label": "Social Insurance",
        "keywords": [
            r"disability\s+insurance", r"\bssdi\b", r"unemployment\s+insurance",
            r"\bssi\b", r"supplemental\s+security\s+income", r"safety\s+net",
            r"workers[\'\u2019]?\s*compensation", r"social\s+insurance",
            r"disability\s+benefit", r"disability\s+program",
            r"disability\s+application", r"disability\s+claim",
        ],
        "exclude": [
            r"car\s+insurance", r"auto\s+insurance", r"property\s+insurance",
            r"crop\s+insurance",
        ],
        "concept_names": ["disability insurance", "social insurance", "supplemental security income"],
    },
    "retirement_decisions": {
        "label": "Retirement Decision-Making",
        "keywords": [
            r"retirement\s+timing", r"early\s+retirement", r"claiming\s+decision",
            r"older\s+worker", r"labor\s+force\s+participation.*(?:older|elderly|aged)",
            r"bridge\s+job", r"phased\s+retirement", r"delayed\s+retirement",
            r"retirement\s+age", r"work\s+incentive.*retire",
            r"retire.*work\s+incentive", r"retirement\s+decision",
            r"retirement\s+transition", r"labor\s+supply.*(?:older|elderly|retire)",
        ],
        "exclude": [],
        "concept_names": ["retirement"],
    },
    "longevity_mortality": {
        "label": "Longevity / Mortality",
        "keywords": [
            r"longevity\s+risk", r"life\s+expectancy", r"mortality\s+differential",
            r"actuarial", r"survival\s+curve", r"mortality\s+rate",
            r"mortality\s+improvement", r"longevity\s+trend",
            r"demographic\s+aging", r"population\s+aging",
            r"mortality\s+inequality", r"death\s+rate",
            r"lifespan", r"years\s+of\s+life",
        ],
        "exclude": [
            r"infant\s+mortality", r"child\s+mortality", r"neonatal",
            r"maternal\s+mortality", r"under[\s-]five\s+mortality",
        ],
        "concept_names": ["longevity", "life expectancy"],
    },
    "private_pensions": {
        "label": "Private Pensions (401k, DB/DC)",
        "keywords": [
            r"401\s*\(?\s*k\s*\)?", r"defined\s+benefit", r"defined\s+contribution",
            r"pension\s+fund", r"auto[\s-]enrollment", r"auto[\s-]escalation",
            r"employer[\s-]sponsored\s+retirement", r"private\s+pension",
            r"occupational\s+pension", r"pension\s+plan",
            r"retirement\s+plan", r"403\s*\(?\s*b\s*\)?",
            r"thrift\s+savings", r"ira\b", r"individual\s+retirement",
            r"target[\s-]date\s+fund", r"pension\s+reform",
        ],
        "exclude": [],
        "concept_names": ["pension", "defined benefit pension plan", "defined contribution plan"],
    },
    "insurance_markets": {
        "label": "Insurance Markets",
        "keywords": [
            r"\bannuit", r"long[\s-]term\s+care\s+insurance",
            r"adverse\s+selection.*insur", r"insur.*adverse\s+selection",
            r"medigap", r"medicare\s+supplement",
            r"insurance\s+market", r"health\s+insurance.*(?:older|elderly|retire)",
            r"ltci\b", r"medicare\s+advantage",
        ],
        "exclude": [
            r"auto\s+insurance", r"car\s+insurance", r"property\s+insurance",
            r"crop\s+insurance", r"business\s+insurance",
        ],
        "concept_names": ["annuity"],
    },
}


def classify_paper(title: str, abstract: str, concepts: list[dict]) -> list[str]:
    """Assign topic slugs to a paper based on concepts and keyword matching."""
    text = f"{title} {abstract}".lower()
    matched_topics = []

    for slug, config in TOPICS.items():
        # Check OpenAlex concepts first (score >= 0.3)
        concept_match = any(
            c["name"].lower() in config["concept_names"] and c.get("score", 0) >= 0.3
            for c in concepts
        )

        # Check keywords
        keyword_match = any(re.search(kw, text, re.IGNORECASE) for kw in config["keywords"])

        if concept_match or keyword_match:
            # Check excludes
            excluded = any(re.search(ex, text, re.IGNORECASE) for ex in config["exclude"])
            if not excluded:
                matched_topics.append(slug)

    return matched_topics


# ---------------------------------------------------------------------------
# OpenAlex Helpers
# ---------------------------------------------------------------------------

def reconstruct_abstract(inverted_index: dict | None) -> str:
    """Reconstruct abstract text from OpenAlex's inverted index format."""
    if not inverted_index:
        return ""
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort()
    return " ".join(w for _, w in word_positions)


def parse_openalex_work(work: dict, matched_by: str) -> dict | None:
    """Parse a single OpenAlex work into our paper format."""
    oa_id = work.get("id", "")
    short_id = oa_id.replace("https://openalex.org/", "")

    title = work.get("title") or ""
    if not title.strip():
        return None

    # Authors
    authors = []
    for authorship in work.get("authorships", [])[:10]:  # cap at 10
        author = authorship.get("author", {})
        name = author.get("display_name", "")
        if not name:
            continue
        institutions = authorship.get("institutions", [])
        affiliation = institutions[0].get("display_name", "") if institutions else ""
        authors.append({"name": name, "affiliation": affiliation})

    # Abstract
    abstract = reconstruct_abstract(work.get("abstract_inverted_index"))

    # Publication date
    pub_date = work.get("publication_date", "")

    # Source
    primary_location = work.get("primary_location") or {}
    source_info = primary_location.get("source") or {}
    source_name = source_info.get("display_name", "")
    source_type_raw = work.get("type", "")
    if source_type_raw == "article":
        source_type = "journal"
    elif source_type_raw in ("preprint", "posted-content"):
        source_type = "preprint"
    else:
        source_type = "working_paper"

    # DOI and URL
    doi = work.get("doi") or ""
    doi = doi.replace("https://doi.org/", "") if doi.startswith("https://doi.org/") else doi
    url = f"https://doi.org/{doi}" if doi else work.get("id", "")

    # Concepts
    concepts = []
    for c in work.get("concepts", []):
        concepts.append({
            "name": c.get("display_name", ""),
            "score": round(c.get("score", 0), 3),
        })

    # Domain filter: require at least one economics/social science concept
    # to filter out biology, chemistry, medical papers with incidental matches
    has_econ_concept = any(
        c["name"].lower() in ECON_DOMAIN_CONCEPTS and c["score"] >= 0.2
        for c in concepts
    )
    if not has_econ_concept:
        return None

    # Topic classification
    topics = classify_paper(title, abstract, concepts)
    if not topics:
        return None  # skip papers with no matching topic

    return {
        "id": short_id,
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "publication_date": pub_date,
        "source": source_name,
        "source_type": source_type,
        "doi": doi,
        "url": url,
        "topics": topics,
        "concepts": [c for c in concepts if c["score"] >= 0.3][:5],
        "matched_by": matched_by,
        "added_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


def fetch_openalex_concept_filtered(from_date: str) -> list[dict]:
    """Fetch papers matching retirement-relevant concepts from OpenAlex."""
    concept_filter = "|".join(CONCEPT_IDS)
    papers = []
    cursor = "*"

    for page_num in range(MAX_PAGES):
        params = {
            "filter": f"concepts.id:{concept_filter},from_publication_date:{from_date},type:article|preprint",
            "select": "id,title,authorships,abstract_inverted_index,publication_date,primary_location,doi,type,concepts",
            "per_page": PAGE_SIZE,
            "cursor": cursor,
            "mailto": MAILTO,
        }

        try:
            resp = requests.get(OPENALEX_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"OpenAlex concept query failed (page {page_num}): {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for work in results:
            paper = parse_openalex_work(work, "concept")
            if paper:
                papers.append(paper)

        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

        log.info(f"  Concept query page {page_num + 1}: {len(results)} works, {len(papers)} matched so far")
        time.sleep(0.1)  # polite rate limiting

    log.info(f"Concept-filtered query: {len(papers)} papers matched")
    return papers


def fetch_openalex_source_filtered(from_date: str) -> list[dict]:
    """Fetch recent papers from target journals, then keyword-filter locally."""
    papers = []

    for source_id, source_name in SOURCE_IDS.items():
        cursor = "*"
        source_papers = []
        # Limit SSRN pages since it has 1.5M+ works
        max_pages = 3 if "SSRN" in source_name else MAX_PAGES

        for page_num in range(max_pages):
            params = {
                "filter": f"primary_location.source.id:{source_id},from_publication_date:{from_date}",
                "select": "id,title,authorships,abstract_inverted_index,publication_date,primary_location,doi,type,concepts",
                "per_page": PAGE_SIZE,
                "cursor": cursor,
                "mailto": MAILTO,
            }

            try:
                resp = requests.get(OPENALEX_BASE, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"OpenAlex source query failed ({source_name}, page {page_num}): {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for work in results:
                paper = parse_openalex_work(work, "keyword")
                if paper:
                    source_papers.append(paper)

            cursor = data.get("meta", {}).get("next_cursor")
            if not cursor:
                break

            time.sleep(0.1)

        log.info(f"  Source '{source_name}': {len(source_papers)} papers matched")
        papers.extend(source_papers)

    log.info(f"Source-filtered query: {len(papers)} total papers matched")
    return papers


# ---------------------------------------------------------------------------
# NBER RSS
# ---------------------------------------------------------------------------

def fetch_nber_rss() -> list[dict]:
    """Fetch new NBER working papers from RSS and keyword-filter."""
    papers = []
    try:
        feed = feedparser.parse(NBER_RSS_URL)
    except Exception as e:
        log.error(f"NBER RSS fetch failed: {e}")
        return papers

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for entry in feed.entries:
        raw_title = entry.get("title", "")
        abstract = entry.get("summary", entry.get("description", ""))
        link = entry.get("link", "")

        # NBER RSS embeds authors in title: "Title -- by Author1, Author2"
        authors = []
        title = raw_title
        if " -- " in raw_title:
            parts = raw_title.split(" -- ", 1)
            title = parts[0].strip()
            author_part = parts[1].strip()
            # Remove leading "by "
            author_part = re.sub(r"^by\s+", "", author_part, flags=re.IGNORECASE)
            for name in re.split(r",\s*(?:and\s+)?|\s+and\s+", author_part):
                name = name.strip()
                if name:
                    authors.append({"name": name, "affiliation": "NBER"})

        # Extract NBER ID from link (e.g., https://www.nber.org/papers/w33456)
        nber_match = re.search(r"w(\d+)", link)
        paper_id = f"nber_w{nber_match.group(1)}" if nber_match else f"nber_{hash(title) % 100000}"

        # Published date
        pub_date = today
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub_date = time.strftime("%Y-%m-%d", entry.published_parsed)

        # Topic classification with empty concepts (RSS doesn't have them)
        topics = classify_paper(title, abstract, [])
        if not topics:
            continue

        papers.append({
            "id": paper_id,
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "publication_date": pub_date,
            "source": "NBER Working Papers",
            "source_type": "working_paper",
            "doi": "",
            "url": link,
            "topics": topics,
            "concepts": [],
            "matched_by": "nber_rss",
            "added_date": today,
        })

    log.info(f"NBER RSS: {len(papers)} papers matched")
    return papers


# ---------------------------------------------------------------------------
# Deduplication & Merging
# ---------------------------------------------------------------------------

def normalize_title(title: str) -> str:
    """Normalize title for fuzzy matching."""
    title = title.lower().strip()
    title = re.sub(r"[^\w\s]", "", title)
    title = re.sub(r"\s+", " ", title)
    return title


def deduplicate(papers: list[dict]) -> list[dict]:
    """Remove duplicate papers by ID and fuzzy title matching."""
    seen_ids = {}
    seen_titles = {}
    unique = []

    for paper in papers:
        # Skip if same ID
        if paper["id"] in seen_ids:
            # Prefer concept-matched over keyword/RSS
            existing = seen_ids[paper["id"]]
            if paper["matched_by"] == "concept" and existing["matched_by"] != "concept":
                # Replace with concept-matched version
                unique = [p if p["id"] != paper["id"] else paper for p in unique]
                seen_ids[paper["id"]] = paper
            continue

        # Fuzzy title match
        norm_title = normalize_title(paper["title"])
        if len(norm_title) < 10:
            # Title too short for reliable matching, skip dedup by title
            pass
        elif norm_title in seen_titles:
            existing = seen_titles[norm_title]
            # Prefer the one with more info (abstract, concepts)
            if len(paper.get("abstract", "")) > len(existing.get("abstract", "")):
                unique = [p if normalize_title(p["title"]) != norm_title else paper for p in unique]
                seen_titles[norm_title] = paper
                seen_ids[paper["id"]] = paper
            continue

        seen_ids[paper["id"]] = paper
        seen_titles[norm_title] = paper
        unique.append(paper)

    return unique


def merge_with_existing(new_papers: list[dict], existing_path: Path) -> list[dict]:
    """Merge new papers with existing papers.json, applying retention window."""
    existing_papers = []
    if existing_path.exists():
        try:
            with open(existing_path) as f:
                data = json.load(f)
                existing_papers = data.get("papers", [])
                log.info(f"Loaded {len(existing_papers)} existing papers")
        except (json.JSONDecodeError, KeyError) as e:
            log.warning(f"Could not parse existing papers.json: {e}")

    # Combine: new papers take priority (they have fresh data)
    all_papers = new_papers + existing_papers

    # Deduplicate
    all_papers = deduplicate(all_papers)

    # Apply retention window
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    retained = [p for p in all_papers if p.get("publication_date", "") >= cutoff]
    dropped = len(all_papers) - len(retained)
    if dropped > 0:
        log.info(f"Dropped {dropped} papers older than {cutoff}")

    # Sort by publication date descending
    retained.sort(key=lambda p: p.get("publication_date", ""), reverse=True)

    return retained


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fetch retirement research papers")
    parser.add_argument("--dry-run", action="store_true", help="Print stats but don't write output")
    parser.add_argument("--output", default=None, help="Output path (default: docs/data/papers.json)")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    output_path = Path(args.output) if args.output else repo_root / "docs" / "data" / "papers.json"

    # Date range: 6 months ago
    from_date = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    log.info(f"Fetching papers from {from_date} to today")

    # Phase 1: OpenAlex concept-filtered
    log.info("=== OpenAlex Concept-Filtered Query ===")
    concept_papers = fetch_openalex_concept_filtered(from_date)

    # Phase 2: OpenAlex source-filtered
    log.info("=== OpenAlex Source-Filtered Query ===")
    source_papers = fetch_openalex_source_filtered(from_date)

    # Phase 3: NBER RSS
    log.info("=== NBER RSS Feed ===")
    nber_papers = fetch_nber_rss()

    # Combine all new papers
    all_new = concept_papers + source_papers + nber_papers
    log.info(f"Total new papers before dedup: {len(all_new)}")

    # Merge with existing and deduplicate
    papers = merge_with_existing(all_new, output_path)

    # Build output
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    dates = [p["publication_date"] for p in papers if p["publication_date"]]
    output = {
        "papers": papers,
        "meta": {
            "last_updated": now,
            "paper_count": len(papers),
            "date_range": {
                "from": min(dates) if dates else from_date,
                "to": max(dates) if dates else now[:10],
            },
        },
    }

    # Topic counts
    topic_counts = {}
    for p in papers:
        for t in p["topics"]:
            topic_counts[t] = topic_counts.get(t, 0) + 1
    log.info(f"=== Results ===")
    log.info(f"Total papers: {len(papers)}")
    for slug, count in sorted(topic_counts.items(), key=lambda x: -x[1]):
        label = TOPICS[slug]["label"]
        log.info(f"  {label}: {count}")

    if args.dry_run:
        log.info("Dry run — not writing output")
        return

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    log.info(f"Wrote {output_path} ({output_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
