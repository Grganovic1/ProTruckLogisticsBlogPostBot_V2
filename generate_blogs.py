#!/usr/bin/env python3
"""
Automated blog generator for Pro Truck Logistics.

Pipeline:
1) Collect trucking/logistics topic context from live websites
2) Use OpenAI to generate topic framing, SEO metadata, article body, and cover images
3) Build blog-post HTML from template
4) Update blog-posts/index.json
5) Rebuild rss.xml and sitemap.xml
6) Upload only the newly generated artifacts to hosting via FTP/SFTP
"""

from __future__ import annotations

import base64
import ftplib
import json
import os
import random
import re
import sys
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from io import BytesIO
from pathlib import Path
from urllib.parse import quote, urlparse
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

import html2text
import paramiko
import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from PIL import Image


def env_value(name: str, default: str | None = None) -> str | None:
    raw = os.environ.get(name)
    if raw is None:
        return default

    value = raw.strip()
    return value if value else default


def env_bool(name: str, default: bool = False) -> bool:
    value = env_value(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = env_value(name)
    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        return default


def env_path(name: str, default: Path) -> Path:
    value = env_value(name)
    if value is None:
        return default
    return Path(value).expanduser().resolve()


def log(message: str) -> None:
    print(message, flush=True)


SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_BLOG_DIR = env_path("LOCAL_BLOG_DIR", SCRIPT_DIR / "blog-posts")
LOCAL_BLOG_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR = LOCAL_BLOG_DIR / "images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATE_PATH = env_path("BLOG_TEMPLATE_PATH", SCRIPT_DIR / "blog-post-template.html")
RSS_PATH = env_path("RSS_PATH", SCRIPT_DIR / "rss.xml")
SITEMAP_PATH = env_path("SITEMAP_PATH", SCRIPT_DIR / "sitemap.xml")

# OpenAI configuration
OPENAI_API_KEY = env_value("OPENAI_API_KEY")
OPENAI_BASE_URL = env_value("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_TEXT_MODEL = env_value("OPENAI_TEXT_MODEL", "gpt-5.4-mini")
OPENAI_TOPIC_MODEL = env_value("OPENAI_TOPIC_MODEL", OPENAI_TEXT_MODEL)
OPENAI_IMAGE_MODEL = env_value("OPENAI_IMAGE_MODEL", "gpt-image-1.5")
OPENAI_IMAGE_SIZE = env_value("OPENAI_IMAGE_SIZE", "1024x1024")
OPENAI_IMAGE_QUALITY = env_value("OPENAI_IMAGE_QUALITY", "medium")
REQUIRE_IMAGE_GENERATION = env_bool("REQUIRE_IMAGE_GENERATION", True)

# Site/publishing configuration
SITE_BASE_URL = env_value("SITE_BASE_URL", "https://protrucklogistics.org").rstrip("/")
POSTS_TO_GENERATE = max(1, env_int("POSTS_TO_GENERATE", 2))
SKIP_UPLOAD = env_bool("SKIP_UPLOAD", False)
REQUEST_TIMEOUT_SECONDS = env_int("REQUEST_TIMEOUT_SECONDS", 30)

# Upload configuration
FTP_HOST = env_value("FTP_HOST", "")
FTP_USER = env_value("FTP_USER", "")
FTP_PASS = env_value("FTP_PASS", "")
FTP_BLOG_DIR = env_value("FTP_BLOG_DIR", "/blog-posts/")
FTP_PORT = env_int("FTP_PORT", 21)
FTP_USE_TLS = env_bool("FTP_USE_TLS", False)


def derive_site_root_dir(blog_dir: str) -> str:
    stripped = (blog_dir or "").strip().strip("/")
    if not stripped:
        return "/"

    parts = stripped.split("/")
    if len(parts) <= 1:
        return "/"

    return "/" + "/".join(parts[:-1]) + "/"


FTP_SITE_ROOT_DIR = env_value("FTP_SITE_ROOT_DIR", derive_site_root_dir(FTP_BLOG_DIR))
FTP_IS_SFTP = env_bool("FTP_IS_SFTP", False)
SFTP_PORT = env_int("SFTP_PORT", 22)
SFTP_STRICT_HOST_KEY = env_bool("SFTP_STRICT_HOST_KEY", False)
SFTP_KNOWN_HOSTS = env_value("SFTP_KNOWN_HOSTS", "")

# Social publishing configuration
ENABLE_SOCIAL_AUTOPUBLISH = env_bool("ENABLE_SOCIAL_AUTOPUBLISH", True)
BLUESKY_HANDLE = env_value("BLUESKY_HANDLE", "")
BLUESKY_APP_PASSWORD = env_value("BLUESKY_APP_PASSWORD", "")
BLUESKY_SERVICE = env_value("BLUESKY_SERVICE", "https://bsky.social").rstrip("/")
MASTODON_BASE_URL = env_value("MASTODON_BASE_URL", "").rstrip("/")
MASTODON_ACCESS_TOKEN = env_value("MASTODON_ACCESS_TOKEN", "")
MASTODON_VISIBILITY = env_value("MASTODON_VISIBILITY", "public")


BLOG_CATEGORIES = [
    "Industry Trends",
    "Fleet Management",
    "Regulations",
    "Fuel Management",
    "Safety",
    "Technology Trends",
    "Driver Retention",
    "Supply Chain Management",
    "Economic Outlook",
    "Logistics Insights",
]

AUTHORS = [
    {
        "name": "John Smith",
        "position": "Logistics Specialist",
        "bio": "John has over 15 years of experience in the logistics industry, specializing in supply chain optimization and transportation management.",
        "image": "https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?ixlib=rb-4.0.3",
    },
    {
        "name": "Sarah Johnson",
        "position": "Transportation Analyst",
        "bio": "Sarah is an expert in transportation economics and regulatory compliance with a background in both private sector logistics and government oversight.",
        "image": "https://images.unsplash.com/photo-1494790108377-be9c29b29330?ixlib=rb-4.0.3",
    },
    {
        "name": "Michael Chen",
        "position": "Technology Director",
        "bio": "Michael specializes in logistics technology integration, helping companies leverage AI, IoT, and analytics to optimize supply chains.",
        "image": "https://images.unsplash.com/photo-1560250097-0b93528c311a?ixlib=rb-4.0.3",
    },
]

TRUCKING_SOURCES = [
    {
        "url": "https://www.ttnews.com/articles/logistics",
        "article_selector": "article",
        "title_selector": "h2,h3",
        "summary_selector": "p,div.field--name-field-deckhead",
    },
    {
        "url": "https://www.ccjdigital.com/",
        "article_selector": "article",
        "title_selector": "h2,h3",
        "summary_selector": "p.entry-summary,p",
    },
    {
        "url": "https://www.overdriveonline.com/",
        "article_selector": "article",
        "title_selector": "h2,h3",
        "summary_selector": "p",
    },
    {
        "url": "https://www.fleetowner.com/",
        "article_selector": "article,div.node--type-article",
        "title_selector": "h2,h3",
        "summary_selector": "p,div.field--name-field-subheadline",
    },
]

STATIC_SITEMAP_PAGES = [
    "index.html",
    "about.html",
    "services.html",
    "contact.html",
    "blog.html",
    "careers.html",
    "carriers.html",
    "agents.html",
    "privacy.html",
    "terms.html",
    "track-shipment.html",
]

CURRENT_EVENT_TERMS = {
    "lawsuit",
    "alleges",
    "fraud",
    "scandal",
    "investigation",
    "probe",
    "fine",
    "underpaid",
    "underpayment",
    "crackdown",
    "shutdown",
    "layoff",
    "bankruptcy",
    "violation",
    "settlement",
    "wage",
    "paycheck",
    "compliance",
    "fmcsa",
    "dot",
    "rates",
    "tariff",
    "insurance",
}

AUDIENCE_TERMS = {
    "driver",
    "drivers",
    "dispatcher",
    "dispatchers",
    "fleet",
    "fleets",
    "owner-operator",
    "owner operators",
    "carrier",
    "carriers",
    "broker",
    "brokers",
    "shipper",
    "shippers",
}

STAKE_TERMS = {
    "cost",
    "costs",
    "pay",
    "profit",
    "margin",
    "risk",
    "downtime",
    "empty miles",
    "breakdown",
    "delay",
    "delays",
    "audit",
    "penalty",
    "safety",
    "claim",
    "claims",
    "cash flow",
    "recruiting",
    "retention",
}

TITLE_CURIOSITY_TERMS = {
    "why",
    "what",
    "how",
    "reveals",
    "behind",
    "mistake",
    "mistakes",
    "warning",
    "problem",
    "problems",
    "risk",
    "risks",
    "before",
    "hidden",
}

LEGAL_CAUTION_TERMS = {
    "lawsuit alleges",
    "drivers say",
    "report raises",
    "investigation",
    "what fleets can learn",
    "what drivers should know",
}

CLICKBAIT_BAN_TERMS = {
    "shocking",
    "unbelievable",
    "secret trick",
    "you won't believe",
    "guaranteed",
    "instantly",
}

TITLE_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "how",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "this",
    "to",
    "what",
    "when",
    "why",
    "with",
}

INTERNAL_LINK_CATALOG = [
    {
        "href": "../services.html",
        "label": "Pro Truck Logistics services",
        "keywords": ["dispatch", "dispatchers", "logistics", "shipping", "freight", "operations", "fleet"],
    },
    {
        "href": "../carriers.html",
        "label": "carrier support options",
        "keywords": ["carrier", "carriers", "owner-operator", "owner operators", "drivers", "capacity"],
    },
    {
        "href": "../agents.html",
        "label": "agent network",
        "keywords": ["agent", "broker", "brokers", "freight", "sales", "customers"],
    },
    {
        "href": "../contact.html",
        "label": "contact Pro Truck Logistics",
        "keywords": ["contact", "support", "quote", "help", "team"],
    },
]

openai_client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL) if OPENAI_API_KEY else None


def clean_text(value: str | None, max_len: int | None = None) -> str:
    text = str(value or "").strip()
    text = re.sub(r"^```(?:json|html|xml)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    text = text.replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if max_len and len(text) > max_len:
        return text[:max_len].rstrip() + "..."
    return text


def parse_json_from_response(raw_text: str):
    if not raw_text:
        raise ValueError("Empty model response")

    candidates: list[str] = []
    block = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", raw_text, flags=re.IGNORECASE)
    if block:
        candidates.append(block.group(1).strip())

    array_match = re.search(r"(\[[\s\S]*\])", raw_text)
    if array_match:
        candidates.append(array_match.group(1).strip())

    object_match = re.search(r"(\{[\s\S]*\})", raw_text)
    if object_match:
        candidates.append(object_match.group(1).strip())

    candidates.append(raw_text.strip())

    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    raise ValueError("Could not parse JSON from model response")


def call_text_model(prompt: str, model: str | None = None) -> str:
    if openai_client is None:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    response = openai_client.chat.completions.create(
        model=model or OPENAI_TEXT_MODEL,
        messages=[{"role": "user", "content": prompt}],
    )
    content = response.choices[0].message.content
    return (content or "").strip()


def is_safe_url(value: str, allow_relative: bool = False) -> bool:
    if not value:
        return False

    value = value.strip()
    if allow_relative and (value.startswith("/") or value.startswith("./") or value.startswith("../")):
        return True

    parsed = urlparse(value)
    return parsed.scheme in {"http", "https", "mailto", "tel"}


def normalize_html_content(raw_content: str) -> str:
    if not raw_content:
        return "<p>No content generated.</p>"

    content = raw_content.strip()
    content = re.sub(r"^```(?:html)?\s*", "", content, flags=re.IGNORECASE)
    content = re.sub(r"\s*```$", "", content)

    soup = BeautifulSoup(content, "html.parser")

    if soup.body:
        working_soup = BeautifulSoup("", "html.parser")
        for child in list(soup.body.contents):
            working_soup.append(child)
        soup = working_soup

    for tag in soup.find_all(["script", "style", "iframe", "object", "embed", "link", "meta", "title", "head"]):
        tag.decompose()

    allowed_tags = {
        "p",
        "h2",
        "h3",
        "ul",
        "ol",
        "li",
        "blockquote",
        "strong",
        "em",
        "b",
        "i",
        "a",
        "br",
        "img",
    }

    for tag in soup.find_all(True):
        if tag.name not in allowed_tags:
            tag.unwrap()
            continue

        attrs: dict[str, str] = {}
        if tag.name == "a":
            href = (tag.get("href") or "").strip()
            if is_safe_url(href, allow_relative=True):
                attrs["href"] = href
        elif tag.name == "img":
            src = (tag.get("src") or "").strip()
            if is_safe_url(src, allow_relative=True):
                attrs["src"] = src
            alt = clean_text(tag.get("alt", ""), max_len=180)
            if alt:
                attrs["alt"] = alt

        tag.attrs = attrs

    normalized = str(soup).strip()
    if normalized:
        return normalized

    plain = clean_text(BeautifulSoup(content, "html.parser").get_text(" ", strip=True), max_len=3000)
    if not plain:
        return "<p>No content generated.</p>"

    return f"<p>{escape(plain)}</p>"


def normalize_keywords(raw_keywords: str) -> str:
    raw_keywords = raw_keywords.replace("\n", ",")
    parts = [clean_text(part, max_len=80).strip(",") for part in raw_keywords.split(",")]
    filtered: list[str] = []
    seen: set[str] = set()

    for part in parts:
        if not part:
            continue
        key = part.lower()
        if key in seen:
            continue
        seen.add(key)
        filtered.append(part)
        if len(filtered) == 7:
            break

    if not filtered:
        return "commercial trucking, logistics, fleet management"

    return ", ".join(filtered)


def generate_excerpt(content_html: str) -> str:
    parser = html2text.HTML2Text()
    parser.ignore_links = True
    text_content = parser.handle(content_html)
    text_content = clean_text(text_content)
    text_content = re.sub(r"^#+\s*", "", text_content)
    text_content = re.sub(r"\s+", " ", text_content).strip()

    if not text_content:
        return "Insights for fleet operators and logistics managers."

    if len(text_content) > 220:
        return text_content[:220].rstrip() + "..."

    return text_content


def parse_numeric_post_id(value) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        if value.startswith("bp") and value[2:].isdigit():
            return int(value[2:])
        if value.isdigit():
            return int(value)
    return None


def parse_sort_date(value: str | None) -> datetime:
    if not value:
        return datetime.min

    for fmt in ("%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return datetime.min


def load_existing_index_posts() -> list[dict]:
    index_path = LOCAL_BLOG_DIR / "index.json"
    if not index_path.exists():
        return []

    try:
        with open(index_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            return payload
    except Exception:
        return []

    return []


def get_recent_titles(limit: int = 60) -> list[str]:
    titles: list[str] = []

    for item in load_existing_index_posts():
        if not isinstance(item, dict):
            continue
        title = clean_text(item.get("title", ""), max_len=220).lower()
        if title:
            titles.append(title)
        if len(titles) >= limit:
            return titles

    for post_file in sorted(LOCAL_BLOG_DIR.glob("*.json"), reverse=True):
        if post_file.name == "index.json":
            continue
        try:
            with open(post_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            title = clean_text(payload.get("title", ""), max_len=220).lower()
            if title and title not in titles:
                titles.append(title)
        except Exception:
            continue
        if len(titles) >= limit:
            break

    return titles


def is_title_similar(candidate_title: str, existing_titles: list[str], threshold: float = 0.88) -> bool:
    candidate_norm = clean_text(candidate_title, max_len=220).lower()
    if not candidate_norm:
        return False

    for existing in existing_titles:
        similarity = SequenceMatcher(None, candidate_norm, existing).ratio()
        if similarity >= threshold:
            return True
    return False


def topic_text(topic: dict) -> str:
    return " ".join(
        clean_text(topic.get(field, ""))
        for field in ("title", "summary", "relevance", "audience", "angle", "urgency")
    ).lower()


def topic_matches_keywords(topic: dict, keywords: set[str]) -> int:
    text = topic_text(topic)
    return sum(1 for term in keywords if term in text)


def topic_needs_legal_caution(topic: dict) -> bool:
    legal_sensitivity = clean_text(topic.get("legal_sensitivity", ""), max_len=20).lower()
    if legal_sensitivity in {"medium", "high"}:
        return True
    return topic_matches_keywords(topic, {"lawsuit", "alleges", "fraud", "investigation", "settlement", "underpaid"}) > 0


def score_topic_heat(topic: dict) -> int:
    score = 0
    score += topic_matches_keywords(topic, CURRENT_EVENT_TERMS) * 4
    score += topic_matches_keywords(topic, AUDIENCE_TERMS) * 3
    score += topic_matches_keywords(topic, STAKE_TERMS) * 2

    urgency = clean_text(topic.get("urgency", ""), max_len=120).lower()
    angle = clean_text(topic.get("angle", ""), max_len=120).lower()
    if any(term in urgency for term in ("now", "today", "current", "breaking", "this week", "2026")):
        score += 5
    if any(term in angle for term in ("warning", "mistake", "hidden cost", "scandal", "fraud", "pay", "compliance", "risk")):
        score += 4
    if topic_needs_legal_caution(topic):
        score += 4
    return score


def prioritize_topics(topics: list[dict]) -> list[dict]:
    decorated = []
    for topic in topics:
        working = dict(topic)
        working["heat_score"] = score_topic_heat(working)
        decorated.append(working)
    decorated.sort(key=lambda item: item.get("heat_score", 0), reverse=True)
    return decorated


def generate_title_candidates(topic: dict) -> list[str]:
    legal_caution = "yes" if topic_needs_legal_caution(topic) else "no"
    prompt = f"""
You are writing blog headlines for a trucking and logistics website.

Create 10 headline options for this topic.

Topic:
- Working angle: {topic.get('title', '')}
- Summary: {topic.get('summary', '')}
- Why readers care now: {topic.get('relevance', '')}
- Audience: {topic.get('audience', '')}
- Story angle: {topic.get('angle', '')}
- Urgency: {topic.get('urgency', '')}
- Legal caution required: {legal_caution}

Requirements:
- Headlines must make drivers, dispatchers, owner-operators, brokers, or fleet managers want to click
- Prefer current-event framing when the topic supports it
- Be specific, sharp, and emotionally engaging
- Do not be misleading, vague, or exaggerated
- Maximum 110 characters
- If legal caution is required, use wording like lawsuit alleges, drivers say, report raises, or what fleets can learn
- Avoid hashtags and quotation marks

Return ONLY a valid JSON array of 10 strings.
"""
    response_text = call_text_model(prompt, model=OPENAI_TEXT_MODEL)
    parsed = parse_json_from_response(response_text)

    candidates: list[str] = []
    if isinstance(parsed, list):
        for item in parsed:
            title = clean_text(item, max_len=110)
            if title and title not in candidates:
                candidates.append(title)

    seed_title = clean_text(topic.get("title", ""), max_len=110)
    if seed_title and seed_title not in candidates:
        candidates.append(seed_title)

    return candidates[:12]


def score_title_candidate(title: str, topic: dict, existing_titles: list[str]) -> int:
    normalized = clean_text(title, max_len=110)
    lower = normalized.lower()
    score = 0

    if is_title_similar(normalized, existing_titles, threshold=0.9):
        score -= 50

    length = len(normalized)
    if 60 <= length <= 100:
        score += 8
    elif 48 <= length <= 110:
        score += 4
    else:
        score -= 4

    if any(term in lower for term in TITLE_CURIOSITY_TERMS):
        score += 5
    if any(term in lower for term in AUDIENCE_TERMS):
        score += 5
    if any(term in lower for term in STAKE_TERMS):
        score += 4
    if any(char.isdigit() for char in normalized):
        score += 2
    if ":" in normalized:
        score += 1

    if topic_needs_legal_caution(topic):
        if any(term in lower for term in LEGAL_CAUTION_TERMS):
            score += 8
        else:
            score -= 8

    if any(term in lower for term in CLICKBAIT_BAN_TERMS):
        score -= 12

    if any(term in lower for term in ("insights", "strategies", "trends")) and not any(term in lower for term in CURRENT_EVENT_TERMS):
        score -= 3

    if normalized and normalized[0].islower():
        score -= 5
    if re.search(r"\b(\w+)\s+\1\b", lower):
        score -= 6
    if len(re.findall(r"\b[a-z]{1,3}\b", lower)) > 10:
        score -= 3

    return score


def review_title_shortlist(topic: dict, candidates: list[str]) -> str:
    if len(candidates) <= 1:
        return candidates[0] if candidates else ""

    prompt = f"""
You are choosing the single best trucking blog headline from a shortlist.

Topic summary: {topic.get('summary', '')}
Why readers care now: {topic.get('relevance', '')}
Audience: {topic.get('audience', '')}
Story angle: {topic.get('angle', '')}
Legal caution required: {"yes" if topic_needs_legal_caution(topic) else "no"}

Headline options:
{json.dumps(candidates, ensure_ascii=False, indent=2)}

Choose the one headline that is:
- the most natural and grammatically clean
- the strongest click driver without being misleading
- the best fit for drivers, dispatchers, owner-operators, brokers, or fleet managers
- careful enough if the topic is allegation- or lawsuit-related

Return ONLY a valid JSON object like:
{{"best": "..."}}
"""
    try:
        response_text = call_text_model(prompt, model=OPENAI_TEXT_MODEL)
        parsed = parse_json_from_response(response_text)
        if isinstance(parsed, dict):
            best = clean_text(parsed.get("best", ""), max_len=110)
            if best in candidates:
                return best
    except Exception as exc:
        log(f"Title shortlist review failed, using heuristic ranking instead: {exc}")
    return candidates[0]


def choose_best_title(topic: dict, existing_titles: list[str]) -> tuple[str, list[str]]:
    candidates = generate_title_candidates(topic)
    if not candidates:
        seed_title = clean_text(topic.get("title", ""), max_len=110)
        return seed_title, [seed_title] if seed_title else []

    ranked = sorted(
        candidates,
        key=lambda candidate: score_title_candidate(candidate, topic, existing_titles),
        reverse=True,
    )
    reviewed = review_title_shortlist(topic, ranked[:5])
    if reviewed in ranked:
        return reviewed, ranked
    return ranked[0], ranked


def tokenize_similarity_text(*parts: str) -> set[str]:
    combined = " ".join(clean_text(part) for part in parts).lower()
    tokens = set(re.findall(r"[a-z0-9][a-z0-9'-]+", combined))
    return {token for token in tokens if len(token) > 3 and token not in TITLE_STOPWORDS}


def select_internal_links(topic: dict, category: str) -> list[dict]:
    text = f"{topic_text(topic)} {clean_text(category)}"
    ranked_links: list[tuple[int, dict]] = []
    for link in INTERNAL_LINK_CATALOG:
        score = sum(text.count(keyword) for keyword in link["keywords"])
        if score:
            ranked_links.append((score, link))

    ranked_links.sort(key=lambda item: item[0], reverse=True)
    chosen = [link for _, link in ranked_links[:2]]
    if not chosen:
        chosen = INTERNAL_LINK_CATALOG[:1]
    return chosen


def generate_social_teasers(title: str, topic: dict, post_url: str) -> dict:
    legal_caution = "yes" if topic_needs_legal_caution(topic) else "no"
    prompt = f"""
Create short social teaser copy for a trucking/logistics blog article.

Article title: {title}
Summary: {topic.get('summary', '')}
Why it matters: {topic.get('relevance', '')}
Audience: {topic.get('audience', '')}
Story angle: {topic.get('angle', '')}
Post URL: {post_url}
Legal caution required: {legal_caution}

Return ONLY a valid JSON object in this format:
{{
  "bluesky": ["...", "..."],
  "mastodon": ["...", "..."]
}}

Requirements:
- Each teaser should be 220 characters or less
- Use a strong hook and a clear why-it-matters sentence
- Do not paste the full article
- Do not include the raw URL in the teaser text
- End with a natural read-more style lead-in
- If legal caution is required, use cautious wording
"""
    response_text = call_text_model(prompt, model=OPENAI_TEXT_MODEL)
    parsed = parse_json_from_response(response_text)
    empty = {"bluesky": [], "mastodon": []}
    if not isinstance(parsed, dict):
        return empty

    teasers = {}
    for channel in ("bluesky", "mastodon"):
        value = parsed.get(channel, [])
        cleaned: list[str] = []
        if isinstance(value, list):
            for item in value:
                teaser = clean_text(item, max_len=220)
                if teaser and teaser not in cleaned:
                    cleaned.append(teaser)
        teasers[channel] = cleaned[:2]
    return teasers


def normalize_blog_asset_path(path: str) -> str:
    normalized = clean_text(path, max_len=400)
    if normalized.startswith("blog-posts/"):
        return normalized[len("blog-posts/"):]
    return normalized


def build_public_post_url(post_id: str) -> str:
    return f"{SITE_BASE_URL}/blog-posts/post-{post_id}.html"


def score_related_post(current_post: dict, candidate: dict) -> int:
    score = 0
    if clean_text(candidate.get("category", ""), max_len=80) == clean_text(current_post.get("category", ""), max_len=80):
        score += 8

    current_tokens = tokenize_similarity_text(
        current_post.get("title", ""),
        current_post.get("excerpt", ""),
        current_post.get("category", ""),
    )
    candidate_tokens = tokenize_similarity_text(
        candidate.get("title", ""),
        candidate.get("excerpt", ""),
        candidate.get("category", ""),
    )
    score += len(current_tokens.intersection(candidate_tokens)) * 3

    candidate_date = parse_sort_date(candidate.get("sort_date") or candidate.get("date"))
    if candidate_date != datetime.min:
        score += candidate_date.year - 2020
    return score


def select_related_posts(current_post: dict, all_posts: list[dict], limit: int = 3) -> list[dict]:
    related_candidates: list[tuple[int, datetime, dict]] = []
    current_id = str(current_post.get("id", "")).strip()

    for candidate in all_posts:
        if not isinstance(candidate, dict):
            continue
        candidate_id = str(candidate.get("id", "")).strip()
        if not candidate_id or candidate_id == current_id:
            continue

        candidate_date = parse_sort_date(candidate.get("sort_date") or candidate.get("date"))
        related_candidates.append((score_related_post(current_post, candidate), candidate_date, candidate))

    related_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)

    chosen: list[dict] = []
    seen_ids: set[str] = set()
    for _, _, candidate in related_candidates:
        candidate_id = str(candidate.get("id", "")).strip()
        if candidate_id in seen_ids:
            continue
        seen_ids.add(candidate_id)
        chosen.append(
            {
                "id": candidate_id,
                "title": clean_text(candidate.get("title", ""), max_len=120),
                "category": clean_text(candidate.get("category", ""), max_len=60),
                "excerpt": clean_text(candidate.get("excerpt", ""), max_len=170),
                "href": f"post-{candidate_id}.html",
                "image": normalize_blog_asset_path(candidate.get("image", "")),
            }
        )
        if len(chosen) >= limit:
            break

    return chosen


def sanitize_social_teaser(teaser: str, fallback_title: str) -> str:
    text = clean_text(teaser, max_len=260)
    text = re.sub(r"https?://\S+", "", text).strip()
    text = re.sub(
        r"(read more|learn more|full story|read the full post|details here|see the full post)[:\s-]*$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip(" :-")
    return text or clean_text(fallback_title, max_len=180)


def get_social_teaser(post: dict, channel: str) -> str:
    teasers = post.get("social_teasers", {})
    if not isinstance(teasers, dict):
        return clean_text(post.get("title", ""), max_len=180)
    channel_teasers = teasers.get(channel, [])
    if isinstance(channel_teasers, list) and channel_teasers:
        return sanitize_social_teaser(channel_teasers[0], post["title"])
    return clean_text(post.get("title", ""), max_len=180)


def normalize_bluesky_handle(handle: str) -> str:
    return clean_text(handle).lstrip("@")


def bluesky_enabled() -> bool:
    return bool(BLUESKY_HANDLE and BLUESKY_APP_PASSWORD)


def mastodon_enabled() -> bool:
    return bool(MASTODON_BASE_URL and MASTODON_ACCESS_TOKEN)


def create_bluesky_session() -> dict:
    response = requests.post(
        f"{BLUESKY_SERVICE}/xrpc/com.atproto.server.createSession",
        json={
            "identifier": normalize_bluesky_handle(BLUESKY_HANDLE),
            "password": BLUESKY_APP_PASSWORD,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def prepare_bluesky_thumbnail(image_path: Path) -> tuple[bytes, str] | tuple[None, None]:
    if not image_path.exists():
        return None, None

    image = Image.open(image_path).convert("RGB")
    size_options = [1024, 768, 512]
    quality_options = [86, 80, 72]

    for max_size in size_options:
        working = image.copy()
        working.thumbnail((max_size, max_size))
        for quality in quality_options:
            buffer = BytesIO()
            working.save(buffer, format="JPEG", quality=quality, optimize=True)
            payload = buffer.getvalue()
            if len(payload) <= 900000:
                return payload, "image/jpeg"

    buffer = BytesIO()
    image.thumbnail((512, 512))
    image.save(buffer, format="JPEG", quality=65, optimize=True)
    return buffer.getvalue(), "image/jpeg"


def upload_bluesky_blob(access_token: str, image_bytes: bytes, mime_type: str) -> dict:
    response = requests.post(
        f"{BLUESKY_SERVICE}/xrpc/com.atproto.repo.uploadBlob",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": mime_type,
        },
        data=image_bytes,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()["blob"]


def publish_to_bluesky(post: dict) -> dict:
    session = create_bluesky_session()
    text = get_social_teaser(post, "bluesky")
    post_url = build_public_post_url(post["id"])
    record = {
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "langs": ["en-US"],
        "embed": {
            "$type": "app.bsky.embed.external",
            "external": {
                "uri": post_url,
                "title": clean_text(post["title"], max_len=100),
                "description": clean_text(post["meta"]["description"], max_len=280),
            },
        },
    }

    image_path = IMAGES_DIR / Path(post["image"]).name
    image_bytes, mime_type = prepare_bluesky_thumbnail(image_path)
    if image_bytes and mime_type:
        try:
            record["embed"]["external"]["thumb"] = upload_bluesky_blob(session["accessJwt"], image_bytes, mime_type)
        except Exception as exc:
            log(f"Bluesky thumbnail upload skipped for {post['id']}: {exc}")

    response = requests.post(
        f"{BLUESKY_SERVICE}/xrpc/com.atproto.repo.createRecord",
        headers={"Authorization": f"Bearer {session['accessJwt']}"},
        json={
            "repo": session["did"],
            "collection": "app.bsky.feed.post",
            "record": record,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def publish_to_mastodon(post: dict) -> dict:
    teaser = get_social_teaser(post, "mastodon")
    post_url = build_public_post_url(post["id"])
    status = f"{teaser}\n\n{post_url}".strip()
    if len(status) > 490:
        max_teaser_length = max(80, 490 - len(post_url) - 2)
        trimmed_teaser = clean_text(teaser, max_len=max_teaser_length).rstrip(".")
        status = f"{trimmed_teaser}\n\n{post_url}"

    response = requests.post(
        f"{MASTODON_BASE_URL}/api/v1/statuses",
        headers={
            "Authorization": f"Bearer {MASTODON_ACCESS_TOKEN}",
            "Idempotency-Key": str(uuid.uuid4()),
        },
        data={
            "status": status,
            "visibility": MASTODON_VISIBILITY,
            "language": "en",
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def autopublish_social_posts(posts: list[dict]) -> dict[str, list[str]]:
    results = {"bluesky": [], "mastodon": []}

    if not ENABLE_SOCIAL_AUTOPUBLISH:
        log("Social autopublish disabled by configuration")
        return results

    if not bluesky_enabled() and not mastodon_enabled():
        log("No social credentials configured; skipping social autopublish")
        return results

    for post in posts:
        if bluesky_enabled():
            try:
                response = publish_to_bluesky(post)
                results["bluesky"].append(response.get("uri", "created"))
                log(f"Published {post['id']} to Bluesky")
            except Exception as exc:
                log(f"Bluesky publish failed for {post['id']}: {exc}")

        if mastodon_enabled():
            try:
                response = publish_to_mastodon(post)
                results["mastodon"].append(response.get("url", "created"))
                log(f"Published {post['id']} to Mastodon")
            except Exception as exc:
                log(f"Mastodon publish failed for {post['id']}: {exc}")

    return results


def get_next_post_id() -> str:
    max_bp_id = 0
    max_numeric_id = 0

    for post_file in LOCAL_BLOG_DIR.glob("*.json"):
        if post_file.name == "index.json":
            continue

        stem = post_file.stem
        if stem.startswith("bp") and stem[2:].isdigit():
            max_bp_id = max(max_bp_id, int(stem[2:]))
        else:
            parsed = parse_numeric_post_id(stem)
            if parsed:
                max_numeric_id = max(max_numeric_id, parsed)

        try:
            with open(post_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            payload_id = str(payload.get("id", "")).strip()
            if payload_id.startswith("bp") and payload_id[2:].isdigit():
                max_bp_id = max(max_bp_id, int(payload_id[2:]))
            else:
                parsed_payload_id = parse_numeric_post_id(payload.get("id"))
                if parsed_payload_id:
                    max_numeric_id = max(max_numeric_id, parsed_payload_id)
        except Exception:
            continue

    if max_bp_id:
        return f"bp{max_bp_id + 1}"

    return f"bp{max_numeric_id + 1}"


def fetch_trucking_articles() -> list[dict]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
    }
    articles: list[dict] = []

    for site in TRUCKING_SOURCES:
        try:
            response = requests.get(site["url"], timeout=REQUEST_TIMEOUT_SECONDS, headers=headers)
            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            nodes = soup.select(site["article_selector"])

            for node in nodes[:6]:
                title_node = node.select_one(site["title_selector"])
                summary_node = node.select_one(site["summary_selector"])

                title = clean_text(title_node.get_text(" ", strip=True) if title_node else "", max_len=180)
                summary = clean_text(summary_node.get_text(" ", strip=True) if summary_node else "", max_len=300)

                if not title:
                    continue

                combined = f"{title} {summary}".lower()
                if not any(term in combined for term in [
                    "truck",
                    "trucking",
                    "fleet",
                    "haul",
                    "freight",
                    "driver",
                    "diesel",
                    "transport",
                    "logistics",
                ]):
                    continue

                articles.append({"title": title, "summary": summary})
        except Exception as exc:
            log(f"Skipping source {site['url']} due to fetch error: {exc}")

    deduped: list[dict] = []
    seen_titles: set[str] = set()
    for article in articles:
        key = article["title"].lower()
        if key in seen_titles:
            continue
        seen_titles.add(key)
        deduped.append(article)

    return deduped


def validate_topics(raw_topics) -> list[dict]:
    if not isinstance(raw_topics, list):
        return []

    validated: list[dict] = []
    for item in raw_topics:
        if not isinstance(item, dict):
            continue

        title = clean_text(item.get("title", ""), max_len=180)
        summary = clean_text(item.get("summary", ""), max_len=300)
        relevance = clean_text(item.get("relevance", ""), max_len=300)
        category = clean_text(item.get("category", ""), max_len=60)
        audience = clean_text(item.get("audience", ""), max_len=120)
        angle = clean_text(item.get("angle", ""), max_len=120)
        urgency = clean_text(item.get("urgency", ""), max_len=120)
        legal_sensitivity = clean_text(item.get("legal_sensitivity", ""), max_len=20).lower()

        if not title or not summary:
            continue

        if not relevance:
            relevance = "This topic is relevant to fleet operators and logistics managers due to current market and operational impacts."
        if not audience:
            audience = "drivers, dispatchers, fleet managers, brokers, and owner-operators"
        if not angle:
            angle = "timely industry takeaway"
        if legal_sensitivity not in {"low", "medium", "high"}:
            legal_sensitivity = "medium" if topic_matches_keywords(item, {"lawsuit", "fraud", "investigation", "underpaid"}) else "low"

        topic = {
            "title": title,
            "summary": summary,
            "relevance": relevance,
            "category": category or "",
            "audience": audience,
            "angle": angle,
            "urgency": urgency,
            "legal_sensitivity": legal_sensitivity,
        }
        validated.append(topic)

    return prioritize_topics(validated)


def get_current_logistics_topics() -> list[dict]:
    try:
        log("Fetching live trucking/logistics articles...")
        scraped_articles = fetch_trucking_articles()
        if len(scraped_articles) >= 3:
            payload = json.dumps(scraped_articles[:12], ensure_ascii=False)
            prompt = f"""
You are assisting a commercial trucking company blog editor.

Using the article list below, generate 7 high-interest blog topics for drivers, dispatchers, owner-operators, brokers, and fleet managers.
Prioritize current events, scandals, lawsuits, fraud allegations, pay issues, compliance crackdowns, safety failures, market shocks, and technology problems when they are relevant.
Return ONLY a valid JSON array.

Required JSON format:
[
  {{
    "title": "...",
    "summary": "...",
    "relevance": "...",
    "category": "...",
    "audience": "...",
    "angle": "...",
    "urgency": "...",
    "legal_sensitivity": "low|medium|high"
  }},
  ...
]

Category must be one of these themes: Industry Trends, Fleet Management, Regulations, Fuel Management, Safety, Technology Trends, Driver Retention, Supply Chain Management, Economic Outlook, Logistics Insights.

Requirements:
- Keep topics distinct from each other
- No invented statistics
- No fake named experts
- Focus on topics that real people in trucking would want to click today
- Prefer topics with money, safety, legal, compliance, downtime, pay, or fraud stakes
- If the topic involves allegations or lawsuits, phrase it carefully and flag legal_sensitivity as medium or high
- The summary should explain the hook and why readers care now
- The angle should describe the emotional frame, such as warning, hidden cost, operator protection, accountability, mistake, or current-event fallout

Articles:
{payload}
"""
            response_text = call_text_model(prompt, model=OPENAI_TOPIC_MODEL)
            parsed = parse_json_from_response(response_text)
            topics = validate_topics(parsed)
            if len(topics) >= 3:
                log(f"Generated {len(topics)} topics from live source context")
                return topics
    except Exception as exc:
        log(f"Live-source topic generation failed: {exc}")

    try:
        log("Generating fallback topic ideas directly from OpenAI...")
        today = datetime.now(timezone.utc).strftime("%B %d, %Y")
        prompt = f"""
Today is {today}. Propose 7 high-interest trucking blog topics that people in the industry would actually want to click.

Return ONLY a valid JSON array in this format:
[
  {{
    "title": "...",
    "summary": "...",
    "relevance": "...",
    "category": "...",
    "audience": "...",
    "angle": "...",
    "urgency": "...",
    "legal_sensitivity": "low|medium|high"
  }},
  ...
]

Requirements:
- Focus on commercial trucking, freight hauling, fleet operations, compliance, driver recruiting, safety, fuel, and logistics technology
- Prioritize current events, controversies, lawsuits, fraud concerns, driver pay, dispatch failures, compliance pressure, and market pain
- Keep each summary to 1-2 sentences
- No invented statistics
- No fake named experts
- Avoid duplicate or near-duplicate topics
- Make the topics emotionally engaging but still truthful
- The audience field must name who cares most
- The angle field should describe the best framing, like warning, hidden cost, operator protection, scandal fallout, or current-event breakdown
"""
        response_text = call_text_model(prompt, model=OPENAI_TOPIC_MODEL)
        parsed = parse_json_from_response(response_text)
        topics = validate_topics(parsed)
        if topics:
            log(f"Generated {len(topics)} fallback topics")
            return topics
    except Exception as exc:
        log(f"Fallback topic generation failed: {exc}")

    return [
        {
            "title": "What Drivers Should Watch When Payroll Disputes and Lease Deductions Stop Making Sense",
            "summary": "A practical look at the warning signs behind pay disputes, opaque deductions, and why drivers should document everything early.",
            "relevance": "Driver pay issues spread fast because they hit trust, cash flow, and retention all at once.",
            "category": "Fleet Management",
            "audience": "drivers and owner-operators",
            "angle": "operator protection",
            "urgency": "pay and settlement disputes stay highly clickable whenever trust in carriers is under pressure",
            "legal_sensitivity": "medium",
        },
        {
            "title": "Why Small Dispatch Mistakes Turn Into Expensive Driver Problems",
            "summary": "Miscommunication, bad appointment handling, and weak load planning can quietly create driver anger and lost margin.",
            "relevance": "Dispatch decisions shape detention, empty miles, and whether drivers stick around.",
            "category": "Regulations",
            "audience": "dispatchers and fleet managers",
            "angle": "hidden cost",
            "urgency": "this stays relevant whenever rates are tight and fleets need cleaner execution",
            "legal_sensitivity": "low",
        },
        {
            "title": "The Compliance Problems Fleets Miss Before Auditors and Claims Expose Them",
            "summary": "A sharper look at the paperwork, inspection habits, and data gaps that become expensive only after something goes wrong.",
            "relevance": "Compliance failures stay hot because they hit safety scores, insurance pressure, and customer trust.",
            "category": "Technology Trends",
            "audience": "fleet managers and safety teams",
            "angle": "warning",
            "urgency": "compliance pressure never stays quiet for long in trucking",
            "legal_sensitivity": "low",
        },
    ]


def choose_category_for_topic(topic: dict) -> str:
    category = clean_text(topic.get("category", ""), max_len=60)
    if category:
        return category

    text = f"{topic.get('title', '')} {topic.get('summary', '')}".lower()
    keyword_map = {
        "Regulations": ["regulation", "compliance", "fmcsa", "dot", "mandate"],
        "Fuel Management": ["fuel", "diesel", "efficiency", "mileage"],
        "Safety": ["safety", "accident", "incident", "risk"],
        "Technology Trends": ["technology", "ai", "telematics", "automation", "software"],
        "Driver Retention": ["driver", "retention", "recruit", "wellness"],
        "Fleet Management": ["fleet", "maintenance", "uptime", "dispatch"],
        "Industry Trends": ["market", "trend", "economy", "demand", "outlook"],
        "Supply Chain Management": ["supply chain", "shipper", "warehouse", "inventory"],
        "Economic Outlook": ["rates", "inflation", "economy", "costs", "pricing"],
        "Logistics Insights": ["logistics", "operations", "shipping", "delivery"],
    }

    best_category = "Fleet Management"
    best_score = -1
    for candidate, keywords in keyword_map.items():
        score = sum(text.count(keyword) for keyword in keywords)
        if score > best_score:
            best_score = score
            best_category = candidate

    return best_category


def generate_meta_description(title: str) -> str:
    prompt = f"""
Write one SEO meta description for this trucking/logistics blog title:
"{title}"

Requirements:
- Max 155 characters
- Audience: fleet operators, dispatchers, and logistics managers
- Make it feel current, specific, and worth clicking
- Plain text only
- No hashtags
- No quotation marks
- No markdown
"""
    response = call_text_model(prompt, model=OPENAI_TEXT_MODEL)
    description = clean_text(response, max_len=155).strip("\"'")
    return description or "Trucking and logistics insights for fleet operators and transportation teams."


def generate_keywords(title: str) -> str:
    prompt = f"""
Generate 5-7 SEO keywords for this blog title:
"{title}"

Return only a comma-separated list. No numbering. No hashtags.
"""
    response = call_text_model(prompt, model=OPENAI_TEXT_MODEL)
    return normalize_keywords(response)


def generate_post_content(topic: dict, category: str, post_date_display: str, keywords: str, internal_links: list[dict]) -> str:
    legal_note = "Use careful language such as lawsuit alleges, drivers say, or what fleets can learn." if topic_needs_legal_caution(topic) else "State the issue confidently, but stay accurate."
    internal_link_lines = "\n".join(
        f"- {link['label']}: {link['href']}" for link in internal_links
    )
    prompt = f"""
Write a sharp, highly readable blog post for Pro Truck Logistics.

Title: {topic.get('title', '')}
Context: {topic.get('summary', '')}
Industry relevance: {topic.get('relevance', '')}
Category: {category}
Date: {post_date_display}
Target keywords: {keywords}
Primary audience: {topic.get('audience', '')}
Story angle: {topic.get('angle', '')}
Urgency: {topic.get('urgency', '')}

Requirements:
- Audience: fleet operators, dispatchers, logistics managers, and drivers
- Open with a 2-4 sentence hook that highlights the tension, risk, or money at stake
- Use 3 or 4 body sections with strong H2 subheads
- Include a section that explains what most people miss
- End with a practical what-to-do-next conclusion
- Use practical, actionable guidance and plainspoken language
- Keep paragraphs fairly short and readable
- Include at least one bullet list or numbered list
- If helpful, use a short real-world scenario instead of generic filler
- No fabricated statistics
- No fake named experts
- If data is uncertain, phrase it carefully in general terms
- {legal_note}
- Keep the tone confident, useful, and engaging, not corporate or bland
- Add up to 2 internal links only if they genuinely help the reader
- OUTPUT ONLY AN HTML FRAGMENT
- Do not include <!DOCTYPE>, <html>, <head>, or <body>
- Allowed tags: <p>, <h2>, <h3>, <ul>, <ol>, <li>, <blockquote>, <strong>, <em>, <a>

Relevant internal links:
{internal_link_lines}
"""
    raw_content = call_text_model(prompt, model=OPENAI_TEXT_MODEL)
    return normalize_html_content(raw_content)


def get_cover_image_prompt(topic: dict) -> str:
    prompt = f"""
Create a concise image-generation prompt for a blog post cover image.

Topic title: {topic.get('title', '')}
Summary: {topic.get('summary', '')}

Requirements:
- Commercial trucking or freight logistics context
- Editorial style
- No text overlays
- No logos
- No watermarks
- Under 120 words

Return only the prompt text.
"""
    return clean_text(call_text_model(prompt, model=OPENAI_TEXT_MODEL), max_len=700)


def get_relevant_image(topic: dict) -> tuple[str, str]:
    if openai_client is None:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    if not REQUIRE_IMAGE_GENERATION:
        raise RuntimeError("REQUIRE_IMAGE_GENERATION is false; image generation is required by this workflow")

    prompt = get_cover_image_prompt(topic)
    image_request = {
        "model": OPENAI_IMAGE_MODEL,
        "prompt": prompt,
        "size": OPENAI_IMAGE_SIZE,
        "quality": OPENAI_IMAGE_QUALITY,
    }
    response = openai_client.images.generate(**image_request)

    image_url = ""
    if getattr(response, "data", None):
        image_url = getattr(response.data[0], "url", "") or ""

    if not image_url and getattr(response, "data", None):
        b64_payload = getattr(response.data[0], "b64_json", "") or ""
        if b64_payload:
            local_filename = f"tmp-{random.randint(100000, 999999)}.png"
            local_path = IMAGES_DIR / local_filename
            with open(local_path, "wb") as f:
                f.write(base64.b64decode(b64_payload))
            return str(local_path), "openai-image"

    if not image_url:
        raise RuntimeError("OpenAI image API returned no URL or b64 payload")

    return image_url, "openai-image"


def download_and_save_image(image_url: str, post_id: str) -> str:
    if not image_url:
        raise RuntimeError("Image URL is empty")

    local_filename = f"{post_id}-image.png"
    local_path = IMAGES_DIR / local_filename

    candidate_local = Path(image_url)
    if candidate_local.exists() and candidate_local.is_file():
        image = Image.open(candidate_local).convert("RGB")
        image.save(local_path, format="PNG")
        if candidate_local != local_path:
            candidate_local.unlink(missing_ok=True)
        log(f"Saved image for {post_id} to {local_path}")
        return f"images/{local_filename}"

    response = requests.get(image_url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    image = Image.open(BytesIO(response.content)).convert("RGB")
    image.save(local_path, format="PNG")

    log(f"Saved image for {post_id} to {local_path}")
    return f"images/{local_filename}"


def generate_blog_post(topic: dict, existing_titles: list[str] | None = None) -> dict:
    title, title_candidates = choose_best_title(topic, existing_titles or [])
    title = clean_text(title, max_len=180)
    if not title:
        raise ValueError("Topic missing title")

    author = random.choice(AUTHORS)
    category = choose_category_for_topic(topic)
    post_id = get_next_post_id()
    post_url = f"{SITE_BASE_URL}/blog-posts/post-{post_id}.html"
    working_topic = dict(topic)
    working_topic["title"] = title

    now = datetime.now(timezone.utc)
    post_date_display = now.strftime("%B %d, %Y")
    sort_date = now.strftime("%Y-%m-%d")

    meta_description = generate_meta_description(title)
    keywords = generate_keywords(title)
    internal_links = select_internal_links(working_topic, category)
    content = generate_post_content(working_topic, category, post_date_display, keywords, internal_links)

    image_url, image_source = get_relevant_image(working_topic)
    local_or_remote_image = download_and_save_image(image_url, post_id)

    excerpt = generate_excerpt(content)
    text_only_content = clean_text(BeautifulSoup(content, "html.parser").get_text(" ", strip=True))
    word_count = max(400, len(re.findall(r"\w+", text_only_content)))
    read_minutes = max(4, min(9, round(word_count / 170)))
    read_time = f"{read_minutes} min read"
    social_teasers = generate_social_teasers(title, working_topic, post_url)

    return {
        "id": post_id,
        "title": title,
        "source_title": clean_text(topic.get("title", ""), max_len=180),
        "title_candidates": title_candidates[:5],
        "excerpt": excerpt,
        "date": post_date_display,
        "sort_date": sort_date,
        "category": category,
        "audience": clean_text(topic.get("audience", ""), max_len=120),
        "angle": clean_text(topic.get("angle", ""), max_len=120),
        "urgency": clean_text(topic.get("urgency", ""), max_len=120),
        "legal_sensitivity": clean_text(topic.get("legal_sensitivity", ""), max_len=20).lower(),
        "author": author["name"],
        "author_position": author["position"],
        "author_bio": author["bio"],
        "author_image": author["image"],
        "read_time": read_time,
        "content": content,
        "image": local_or_remote_image,
        "image_source": image_source,
        "internal_links": internal_links,
        "social_teasers": social_teasers,
        "meta": {
            "description": meta_description,
            "keywords": keywords,
        },
    }


def save_blog_post(post: dict) -> Path:
    file_path = LOCAL_BLOG_DIR / f"{post['id']}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(post, f, ensure_ascii=False, indent=2)
    log(f"Saved JSON post: {file_path}")
    return file_path


def update_meta_tag(
    soup: BeautifulSoup,
    *,
    meta_id: str | None = None,
    name: str | None = None,
    property_name: str | None = None,
    content: str = "",
) -> None:
    target = None
    if meta_id:
        target = soup.find("meta", id=meta_id)
    if target is None and name:
        target = soup.find("meta", attrs={"name": name})
    if target is None and property_name:
        target = soup.find("meta", attrs={"property": property_name})

    if target is not None:
        target["content"] = content
        if target.has_attr("id"):
            del target["id"]


def set_text_by_id(soup: BeautifulSoup, element_id: str, value: str) -> None:
    element = soup.find(id=element_id)
    if element is None:
        return
    element.clear()
    element.append(clean_text(value))


def build_absolute_blog_url(path: str) -> str:
    if is_safe_url(path):
        parsed = urlparse(path)
        if parsed.scheme in {"http", "https"}:
            return path

    trimmed = (path or "").lstrip("/")
    if trimmed.startswith("blog-posts/"):
        return f"{SITE_BASE_URL}/{trimmed}"

    return f"{SITE_BASE_URL}/blog-posts/{trimmed}"


def create_blog_post_html(post: dict, related_posts: list[dict] | None = None) -> Path:
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Missing template file: {TEMPLATE_PATH}")

    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    includes_href = env_value("BLOG_INCLUDES_HREF", "../includes.html")
    post_id = post["id"]
    post_url = f"{SITE_BASE_URL}/blog-posts/post-{post_id}.html"
    image_absolute_url = build_absolute_blog_url(post["image"])

    image_for_header = post["image"]
    if image_for_header.startswith("blog-posts/"):
        image_for_header = image_for_header[len("blog-posts/"):]

    if soup.title:
        soup.title.string = f"{clean_text(post['title'])} | Pro Truck Logistics"

    update_meta_tag(soup, meta_id="meta-description", name="description", content=post["meta"]["description"])
    update_meta_tag(soup, meta_id="meta-keywords", name="keywords", content=post["meta"]["keywords"])
    update_meta_tag(
        soup,
        meta_id="og-title",
        property_name="og:title",
        content=f"{post['title']} | Pro Truck Logistics",
    )
    update_meta_tag(
        soup,
        meta_id="og-description",
        property_name="og:description",
        content=post["meta"]["description"],
    )
    update_meta_tag(soup, meta_id="og-image", property_name="og:image", content=image_absolute_url)
    update_meta_tag(soup, property_name="og:url", content=post_url)

    canonical_link = soup.find("link", attrs={"rel": "canonical"})
    if canonical_link is None and soup.head is not None:
        canonical_link = soup.new_tag("link", rel="canonical", href=post_url)
        if soup.title is not None:
            soup.title.insert_after(canonical_link)
        else:
            soup.head.insert(0, canonical_link)
    elif canonical_link is not None:
        canonical_link["href"] = post_url

    for script in soup.find_all("script"):
        script_text = script.string
        if not script_text:
            continue
        updated_script_text = script_text.replace("../blog-includes.html", includes_href)
        if updated_script_text != script_text:
            script.string = updated_script_text

    header = soup.find(id="post-header")
    if header is not None:
        header["style"] = (
            "background-image: linear-gradient(rgba(0, 0, 0, 0.5), rgba(0, 0, 0, 0.5)), "
            f"url('{image_for_header}');"
        )

    set_text_by_id(soup, "post-category", post["category"])
    set_text_by_id(soup, "post-title-header", post["title"])
    set_text_by_id(soup, "post-date", post["date"])
    set_text_by_id(soup, "post-author", post["author"])
    set_text_by_id(soup, "post-read-time", post["read_time"])

    post_content_container = soup.find(id="post-content")
    if post_content_container is not None:
        post_content_container.clear()
        fragment = BeautifulSoup(post["content"], "html.parser")
        for child in list(fragment.contents):
            post_content_container.append(child)

    related_container = soup.find(id="related-posts-container")
    if related_container is not None:
        related_container.clear()
        for related in related_posts or []:
            column = soup.new_tag("div", attrs={"class": "col-md-4 mb-4"})
            card = soup.new_tag("article", attrs={"class": "related-card"})

            link = soup.new_tag("a", href=related["href"], attrs={"class": "text-decoration-none"})
            image = soup.new_tag("div", attrs={"class": "related-image"})
            related_image = normalize_blog_asset_path(related.get("image", ""))
            if related_image:
                image["style"] = f"background-image: url('{related_image}');"
            link.append(image)

            content = soup.new_tag("div", attrs={"class": "related-content"})
            category = soup.new_tag("span", attrs={"class": "related-category"})
            category.string = related.get("category") or "Blog"
            content.append(category)

            title = soup.new_tag("h3", attrs={"class": "related-title"})
            title.string = related.get("title") or "Related post"
            content.append(title)

            excerpt = soup.new_tag("p")
            excerpt.string = related.get("excerpt") or ""
            content.append(excerpt)

            read_more = soup.new_tag("span", attrs={"class": "btn btn-link p-0"})
            read_more.string = "Read more"
            content.append(read_more)

            link.append(content)
            card.append(link)
            column.append(card)
            related_container.append(column)

    author_img = soup.find(id="author-image")
    if author_img is not None:
        author_img["src"] = post["author_image"]
        author_img["alt"] = clean_text(post["author"], max_len=120)

    set_text_by_id(soup, "author-name", post["author"])
    set_text_by_id(soup, "author-position", post["author_position"])
    set_text_by_id(soup, "author-bio", post["author_bio"])

    encoded_post_url = quote(post_url, safe="")
    encoded_title = quote(post["title"], safe="")
    email_subject = quote(post["title"], safe="")
    email_body = quote(f"Check out this article: {post_url}", safe="")

    share_links = {
        "facebook": f"https://www.facebook.com/sharer/sharer.php?u={encoded_post_url}",
        "twitter": f"https://twitter.com/intent/tweet?url={encoded_post_url}&text={encoded_title}",
        "linkedin": f"https://www.linkedin.com/shareArticle?mini=true&url={encoded_post_url}&title={encoded_title}",
        "email": f"mailto:?subject={email_subject}&body={email_body}",
    }

    for channel, href in share_links.items():
        anchor = soup.select_one(f"a.share-button.{channel}")
        if anchor is None:
            continue
        anchor["href"] = href
        if channel != "email":
            anchor["target"] = "_blank"
            anchor["rel"] = "noopener noreferrer"

    schema_script = soup.find("script", id="article-schema")
    if schema_script is not None:
        schema_data = {
            "@context": "https://schema.org",
            "@type": "BlogPosting",
            "headline": post["title"],
            "description": post["meta"]["description"],
            "image": image_absolute_url,
            "author": {
                "@type": "Person",
                "name": post["author"],
                "jobTitle": post["author_position"],
            },
            "publisher": {
                "@type": "Organization",
                "name": "Pro Truck Logistics",
                "logo": {
                    "@type": "ImageObject",
                    "url": f"{SITE_BASE_URL}/ProTruckLogisticsFiles/logo.jpg",
                },
            },
            "datePublished": f"{post['sort_date']}T00:00:00Z",
            "dateModified": f"{post['sort_date']}T00:00:00Z",
            "mainEntityOfPage": {
                "@type": "WebPage",
                "@id": post_url,
            },
            "keywords": post["meta"]["keywords"],
            "articleSection": post["category"],
        }
        schema_script.string = "\n" + json.dumps(schema_data, indent=2) + "\n"

    html_path = LOCAL_BLOG_DIR / f"post-{post_id}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(str(soup))

    log(f"Created HTML page: {html_path}")
    return html_path


def update_blog_index(new_posts: list[dict]) -> Path:
    index_path = LOCAL_BLOG_DIR / "index.json"
    existing_posts = load_existing_index_posts()

    posts_by_id: dict[str, dict] = {}
    for post in existing_posts:
        if isinstance(post, dict) and "id" in post:
            posts_by_id[str(post["id"])] = post

    for post in new_posts:
        image_path = post["image"]
        if image_path.startswith("images/"):
            image_path = f"blog-posts/{image_path}"

        posts_by_id[str(post["id"])] = {
            "id": post["id"],
            "title": post["title"],
            "excerpt": post["excerpt"],
            "date": post["date"],
            "sort_date": post.get("sort_date"),
            "category": post["category"],
            "author": post["author"],
            "read_time": post["read_time"],
            "image": image_path,
        }

    all_posts = list(posts_by_id.values())
    all_posts.sort(
        key=lambda item: (parse_sort_date(item.get("sort_date") or item.get("date")), parse_numeric_post_id(item.get("id")) or 0),
        reverse=True,
    )

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=2)

    log(f"Updated blog index: {index_path}")
    return index_path


def load_existing_sitemap_lastmods(sitemap_path: Path) -> dict[str, str]:
    if not sitemap_path.exists():
        return {}

    try:
        tree = ET.parse(sitemap_path)
    except ET.ParseError:
        return {}

    namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    lastmods: dict[str, str] = {}
    for url_node in tree.findall("sm:url", namespace):
        loc_node = url_node.find("sm:loc", namespace)
        lastmod_node = url_node.find("sm:lastmod", namespace)
        if loc_node is None or lastmod_node is None:
            continue
        loc_text = clean_text(loc_node.text)
        lastmod_text = clean_text(lastmod_node.text)
        if loc_text and lastmod_text:
            lastmods[loc_text] = lastmod_text
    return lastmods


def generate_rss_feed(posts: list[dict]) -> Path:
    items: list[str] = []

    for post in posts[:50]:
        post_id = post.get("id")
        if not post_id:
            continue

        post_url = f"{SITE_BASE_URL}/blog-posts/post-{post_id}.html"
        sort_date = parse_sort_date(post.get("sort_date") or post.get("date"))
        if sort_date == datetime.min:
            sort_date = datetime.now(timezone.utc)
        pub_date = sort_date.strftime("%a, %d %b %Y 00:00:00 +0000")
        title = escape(clean_text(post.get("title", "Untitled Post")))
        description = escape(clean_text(post.get("excerpt", post.get("title", "")), max_len=300))

        items.append(
            "    <item>\n"
            f"      <title>{title}</title>\n"
            f"      <link>{post_url}</link>\n"
            f"      <guid>{post_url}</guid>\n"
            f"      <pubDate>{pub_date}</pubDate>\n"
            f"      <description>{description}</description>\n"
            "    </item>"
        )

    rss_content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<rss version=\"2.0\">\n"
        "  <channel>\n"
        "    <title>Pro Truck Logistics Blog</title>\n"
        f"    <link>{SITE_BASE_URL}/blog.html</link>\n"
        "    <description>Industry news, trends, and expert advice from Pro Truck Logistics.</description>\n"
        "    <language>en-us</language>\n"
        + "\n".join(items)
        + "\n  </channel>\n"
        "</rss>\n"
    )

    RSS_PATH.write_text(rss_content, encoding="utf-8")
    log(f"Updated RSS feed: {RSS_PATH}")
    return RSS_PATH


def generate_sitemap(posts: list[dict]) -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    existing_lastmods = load_existing_sitemap_lastmods(SITEMAP_PATH)
    urls: list[tuple[str, str]] = []

    for page in STATIC_SITEMAP_PAGES:
        loc = f"{SITE_BASE_URL}/{page}"
        lastmod = existing_lastmods.get(loc, today)
        urls.append((loc, lastmod))

    for post in posts:
        post_id = post.get("id")
        if not post_id:
            continue
        loc = f"{SITE_BASE_URL}/blog-posts/post-{post_id}.html"
        lastmod = clean_text(post.get("sort_date") or "", max_len=20)
        if not lastmod:
            parsed = parse_sort_date(post.get("date"))
            lastmod = parsed.strftime("%Y-%m-%d") if parsed != datetime.min else today
        urls.append((loc, lastmod))

    sitemap_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for loc, lastmod in urls:
        sitemap_parts.extend([
            "  <url>",
            f"    <loc>{escape(loc)}</loc>",
            f"    <lastmod>{escape(lastmod)}</lastmod>",
            "  </url>",
        ])
    sitemap_parts.append("</urlset>")
    sitemap_parts.append("")

    SITEMAP_PATH.write_text("\n".join(sitemap_parts), encoding="utf-8")
    log(f"Updated sitemap: {SITEMAP_PATH}")
    return SITEMAP_PATH


def ensure_remote_dir_sftp(sftp: paramiko.SFTPClient, path: str) -> None:
    normalized = (path or "/").strip()
    if not normalized.startswith("/"):
        normalized = "/" + normalized

    if normalized == "/":
        return

    try:
        sftp.stat(normalized)
        return
    except FileNotFoundError:
        pass

    current = ""
    for part in normalized.strip("/").split("/"):
        current += f"/{part}"
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


def upload_files_to_server(blog_files: list[Path], image_files: list[Path], root_files: list[Path]) -> bool:
    if SKIP_UPLOAD:
        log("SKIP_UPLOAD=true, skipping upload phase")
        return True

    if FTP_IS_SFTP:
        return upload_files_via_sftp(blog_files, image_files, root_files)

    return upload_files_via_ftp(blog_files, image_files, root_files)


def upload_files_via_sftp(blog_files: list[Path], image_files: list[Path], root_files: list[Path]) -> bool:
    try:
        ssh = paramiko.SSHClient()
        if SFTP_STRICT_HOST_KEY:
            ssh.load_system_host_keys()
            if SFTP_KNOWN_HOSTS:
                known_hosts_path = Path(SFTP_KNOWN_HOSTS)
                if known_hosts_path.exists():
                    ssh.load_host_keys(str(known_hosts_path))
            ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            log("Warning: SFTP strict host key checking disabled (set SFTP_STRICT_HOST_KEY=true to enforce)")
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        log(f"Connecting to SFTP host {FTP_HOST}:{SFTP_PORT}...")
        ssh.connect(
            hostname=FTP_HOST,
            port=SFTP_PORT,
            username=FTP_USER,
            password=FTP_PASS,
            timeout=20,
        )
        sftp = ssh.open_sftp()

        ensure_remote_dir_sftp(sftp, FTP_BLOG_DIR)
        ensure_remote_dir_sftp(sftp, f"{FTP_BLOG_DIR.rstrip('/')}/images")
        ensure_remote_dir_sftp(sftp, FTP_SITE_ROOT_DIR)

        for file_path in blog_files:
            remote_path = f"{FTP_BLOG_DIR.rstrip('/')}/{file_path.name}"
            sftp.put(str(file_path), remote_path)
            log(f"Uploaded {file_path.name}")

        for image_path in image_files:
            remote_path = f"{FTP_BLOG_DIR.rstrip('/')}/images/{image_path.name}"
            sftp.put(str(image_path), remote_path)
            log(f"Uploaded image {image_path.name}")

        for root_path in root_files:
            remote_path = f"{FTP_SITE_ROOT_DIR.rstrip('/')}/{root_path.name}" if FTP_SITE_ROOT_DIR != "/" else f"/{root_path.name}"
            sftp.put(str(root_path), remote_path)
            log(f"Uploaded root file {root_path.name}")

        sftp.close()
        ssh.close()
        log("SFTP upload completed")
        return True
    except Exception as exc:
        log(f"SFTP upload failed: {exc}")
        return False


def ensure_remote_dir_ftp(ftp: ftplib.FTP, path: str) -> None:
    normalized = (path or "/").strip()
    if not normalized:
        return
    if not normalized.startswith("/"):
        normalized = "/" + normalized

    parts = normalized.strip("/").split("/")
    if parts == [""]:
        return

    current = ""
    for part in parts:
        current += f"/{part}"
        try:
            ftp.cwd(current)
        except ftplib.error_perm:
            ftp.mkd(current)
            ftp.cwd(current)


def upload_files_via_ftp(blog_files: list[Path], image_files: list[Path], root_files: list[Path]) -> bool:
    try:
        ftp_class = ftplib.FTP_TLS if FTP_USE_TLS else ftplib.FTP
        protocol = "FTPS" if FTP_USE_TLS else "FTP"
        log(f"Connecting to {protocol} host {FTP_HOST}:{FTP_PORT}...")
        with ftp_class() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT, timeout=20)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.encoding = "utf-8"
            if FTP_USE_TLS:
                ftp.prot_p()
            ensure_remote_dir_ftp(ftp, FTP_BLOG_DIR)
            ensure_remote_dir_ftp(ftp, f"{FTP_BLOG_DIR.rstrip('/')}/images")
            ensure_remote_dir_ftp(ftp, FTP_SITE_ROOT_DIR)

            for file_path in blog_files:
                ftp.cwd(FTP_BLOG_DIR)
                with open(file_path, "rb") as fp:
                    ftp.storbinary(f"STOR {file_path.name}", fp)
                log(f"Uploaded {file_path.name}")

            if image_files:
                ftp.cwd(f"{FTP_BLOG_DIR.rstrip('/')}/images")
                for image_path in image_files:
                    with open(image_path, "rb") as fp:
                        ftp.storbinary(f"STOR {image_path.name}", fp)
                    log(f"Uploaded image {image_path.name}")

            for root_path in root_files:
                ftp.cwd(FTP_SITE_ROOT_DIR)
                with open(root_path, "rb") as fp:
                    ftp.storbinary(f"STOR {root_path.name}", fp)
                log(f"Uploaded root file {root_path.name}")

        log(f"{protocol} upload completed")
        return True
    except Exception as exc:
        protocol = "FTPS" if FTP_USE_TLS else "FTP"
        log(f"{protocol} upload failed: {exc}")
        return False


def validate_runtime_configuration() -> None:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")

    if REQUIRE_IMAGE_GENERATION and not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY for required image generation")

    if not TEMPLATE_PATH.exists():
        raise RuntimeError(f"Template not found: {TEMPLATE_PATH}")

    if not SKIP_UPLOAD:
        missing = [name for name, value in {
            "FTP_HOST": FTP_HOST,
            "FTP_USER": FTP_USER,
            "FTP_PASS": FTP_PASS,
        }.items() if not value]
        if missing:
            raise RuntimeError(f"Missing upload env vars: {', '.join(missing)}")

    if not SITE_BASE_URL.startswith("https://"):
        raise RuntimeError("SITE_BASE_URL must use https://")

    if BLUESKY_HANDLE and not BLUESKY_APP_PASSWORD:
        log("Warning: BLUESKY_HANDLE is set but BLUESKY_APP_PASSWORD is missing; Bluesky autopost will be skipped")
    if BLUESKY_APP_PASSWORD and not BLUESKY_HANDLE:
        log("Warning: BLUESKY_APP_PASSWORD is set but BLUESKY_HANDLE is missing; Bluesky autopost will be skipped")
    if MASTODON_BASE_URL and not MASTODON_ACCESS_TOKEN:
        log("Warning: MASTODON_BASE_URL is set but MASTODON_ACCESS_TOKEN is missing; Mastodon autopost will be skipped")
    if MASTODON_ACCESS_TOKEN and not MASTODON_BASE_URL:
        log("Warning: MASTODON_ACCESS_TOKEN is set but MASTODON_BASE_URL is missing; Mastodon autopost will be skipped")


def build_upload_manifest(generated_posts: list[dict], index_path: Path, rss_path: Path, sitemap_path: Path) -> tuple[list[Path], list[Path], list[Path]]:
    blog_files: list[Path] = [index_path]
    image_files: list[Path] = []
    root_files: list[Path] = [rss_path, sitemap_path]

    seen_blog_files: set[Path] = {index_path}
    seen_image_files: set[Path] = set()

    for post in generated_posts:
        json_path = LOCAL_BLOG_DIR / f"{post['id']}.json"
        html_path = LOCAL_BLOG_DIR / f"post-{post['id']}.html"
        image_name = Path(post["image"]).name
        image_path = IMAGES_DIR / image_name

        for path in (json_path, html_path):
            if path not in seen_blog_files:
                blog_files.append(path)
                seen_blog_files.add(path)

        if image_path.exists() and image_path not in seen_image_files:
            image_files.append(image_path)
            seen_image_files.add(image_path)

    return blog_files, image_files, root_files


def main() -> None:
    validate_runtime_configuration()

    log("Starting Pro Truck Logistics blog generation")
    log(f"Using OpenAI base URL: {OPENAI_BASE_URL}")
    log(f"Using text model: {OPENAI_TEXT_MODEL}")
    log(f"Using topic model: {OPENAI_TOPIC_MODEL}")
    log(f"Using image model: {OPENAI_IMAGE_MODEL} ({OPENAI_IMAGE_QUALITY}, {OPENAI_IMAGE_SIZE})")
    log(f"Posts to generate this run: {POSTS_TO_GENERATE}")
    log(f"Upload mode: {'SFTP' if FTP_IS_SFTP else 'FTP'}")
    log(f"Social autopublish: {'enabled' if ENABLE_SOCIAL_AUTOPUBLISH else 'disabled'}")

    topics = get_current_logistics_topics()
    if not topics:
        raise RuntimeError("No topics generated")

    random.shuffle(topics)
    recent_titles = get_recent_titles()
    reserved_titles: list[str] = []
    generated_posts: list[dict] = []

    for topic in topics:
        title = clean_text(topic.get("title", ""), max_len=220)
        if not title:
            continue

        compare_titles = recent_titles + reserved_titles
        if is_title_similar(title, compare_titles, threshold=0.93):
            log(f"Skipping near-duplicate topic: {title}")
            continue

        try:
            post = generate_blog_post(topic, compare_titles)
        except Exception as exc:
            log(f"Skipping topic due to generation error: {title} ({exc})")
            continue

        save_blog_post(post)
        generated_posts.append(post)
        reserved_titles.append(clean_text(post["title"], max_len=220).lower())
        log(f"Completed post: {post['title']}")

        if len(generated_posts) >= POSTS_TO_GENERATE:
            break

    if not generated_posts:
        raise RuntimeError("No posts were generated")

    if len(generated_posts) < POSTS_TO_GENERATE:
        log(f"Warning: generated {len(generated_posts)} post(s), below requested count of {POSTS_TO_GENERATE}")

    index_path = update_blog_index(generated_posts)
    updated_posts = load_existing_index_posts()
    for post in generated_posts:
        create_blog_post_html(post, select_related_posts(post, updated_posts))
    rss_path = generate_rss_feed(updated_posts)
    sitemap_path = generate_sitemap(updated_posts)

    blog_files, image_files, root_files = build_upload_manifest(generated_posts, index_path, rss_path, sitemap_path)
    if not upload_files_to_server(blog_files, image_files, root_files):
        raise RuntimeError("Upload failed")

    autopublish_social_posts(generated_posts)

    log("Blog generation pipeline completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"Fatal error: {exc}")
        sys.exit(1)
