#!/usr/bin/env python3
"""
Automated blog generator for Pro Truck Logistics.

Pipeline:
1) Collect trucking/logistics topics from live websites (with AI-assisted fallback)
2) Generate SEO-friendly post metadata + article body
3) Build blog-post HTML from template
4) Update blog-posts/index.json
5) Upload generated files to hosting via SFTP/FTP
"""

import base64
import ftplib
import json
import os
import random
import re
import sys
from datetime import datetime
from difflib import SequenceMatcher
from io import BytesIO
from pathlib import Path
from urllib.parse import quote, urlparse

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


SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_BLOG_DIR = env_path("LOCAL_BLOG_DIR", SCRIPT_DIR / "blog-posts")
LOCAL_BLOG_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR = LOCAL_BLOG_DIR / "images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATE_PATH = env_path("BLOG_TEMPLATE_PATH", SCRIPT_DIR / "blog-post-template.html")

# Provider configuration (ecomagent for text/topic + OpenAI for images)
ECOMAGENT_API_KEY = env_value("ECOMAGENT_API_KEY") or env_value("OPENAI_API_KEY")
ECOMAGENT_BASE_URL = env_value("ECOMAGENT_BASE_URL", "https://api.ecomagent.in/v1").rstrip("/")
ECOMAGENT_MODEL = env_value("ECOMAGENT_MODEL", "claude-opus-4.6")
ECOMAGENT_TOPIC_MODEL = env_value("ECOMAGENT_TOPIC_MODEL", ECOMAGENT_MODEL)

OPENAI_API_KEY = env_value("OPENAI_API_KEY")
OPENAI_BASE_URL = env_value("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_IMAGE_MODEL = env_value("OPENAI_IMAGE_MODEL", "gpt-image-1")
OPENAI_IMAGE_SIZE = env_value("OPENAI_IMAGE_SIZE", "1024x1024")
REQUIRE_IMAGE_GENERATION = env_bool("REQUIRE_IMAGE_GENERATION", True)

# Site/Publishing configuration
SITE_BASE_URL = env_value("SITE_BASE_URL", "https://protrucklogistics.org").rstrip("/")
POSTS_TO_GENERATE = max(1, env_int("POSTS_TO_GENERATE", 1))
SKIP_UPLOAD = env_bool("SKIP_UPLOAD", False)
REQUEST_TIMEOUT_SECONDS = env_int("REQUEST_TIMEOUT_SECONDS", 30)

# Upload configuration
FTP_HOST = env_value("FTP_HOST", "")
FTP_USER = env_value("FTP_USER", "")
FTP_PASS = env_value("FTP_PASS", "")
FTP_BLOG_DIR = env_value("FTP_BLOG_DIR", "/blog-posts/")
FTP_IS_SFTP = env_bool("FTP_IS_SFTP", True)
SFTP_STRICT_HOST_KEY = env_bool("SFTP_STRICT_HOST_KEY", False)
SFTP_KNOWN_HOSTS = env_value("SFTP_KNOWN_HOSTS", "")


BLOG_CATEGORIES = [
    "Industry Trends", "Market Analysis", "Economic Outlook", "Logistics Insights",
    "Supply Chain Management", "Warehousing", "Inventory Management", "Last-Mile Delivery",
    "Cross-Docking", "Intermodal Transportation", "Freight Forwarding", "Order Fulfillment",
    "Driver Tips", "Driver Wellness", "Driver Recruitment", "Driver Retention",
    "Owner-Operator Resources", "Career Development", "Road Life", "Driver Stories",
    "Sustainability", "Green Logistics", "Carbon Reduction", "Alternative Fuels",
    "Environmental Compliance", "Eco-Friendly Practices", "Electric Vehicles", "Renewable Energy",
    "Technology Trends", "AI & Automation", "Telematics", "Blockchain in Logistics",
    "IoT Solutions", "Data Analytics", "Digital Transformation", "Route Optimization",
    "Warehouse Automation", "Transportation Management Systems", "Fleet Tech",
    "Safety", "Regulations", "Compliance Updates", "Risk Management",
    "HOS Regulations", "DOT Compliance", "FMCSA Updates", "Insurance Insights",
    "Security Measures", "Accident Prevention", "Cargo Security",
    "Fleet Management", "Maintenance Tips", "Vehicle Selection", "Asset Utilization",
    "Fleet Efficiency", "Fuel Management", "Preventative Maintenance", "Equipment Upgrades",
    "Business Growth", "Financial Management", "Strategic Planning", "Competitive Advantage",
    "Cost Reduction", "Revenue Optimization", "Customer Experience", "Service Expansion",
    "LTL Shipping", "FTL Transport", "Refrigerated Logistics", "Hazmat Transportation",
    "Heavy Haul", "Expedited Shipping", "Specialized Freight", "Bulk Transport",
    "International Shipping", "Global Supply Chains", "Cross-Border Transport", "Import/Export",
    "Trade Compliance", "Customs Regulations", "Port Operations", "Global Logistics Trends",
    "Customer Service", "Relationship Management", "Shipper Insights", "Client Success Stories",
    "Service Improvements", "Client Retention", "Value-Added Services",
    "Conference Takeaways", "Industry Events", "Trade Shows", "Webinar Recaps",
    "Expert Interviews", "Industry Awards", "Case Studies"
]

AUTHORS = [
    {
        "name": "John Smith",
        "position": "Logistics Specialist",
        "bio": "John has over 15 years of experience in the logistics industry, specializing in supply chain optimization and transportation management.",
        "image": "https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?ixlib=rb-4.0.3"
    },
    {
        "name": "Sarah Johnson",
        "position": "Transportation Analyst",
        "bio": "Sarah is an expert in transportation economics and regulatory compliance with a background in both private sector logistics and government oversight.",
        "image": "https://images.unsplash.com/photo-1494790108377-be9c29b29330?ixlib=rb-4.0.3"
    },
    {
        "name": "Michael Chen",
        "position": "Technology Director",
        "bio": "Michael specializes in logistics technology integration, helping companies leverage AI, IoT, and analytics to optimize supply chains.",
        "image": "https://images.unsplash.com/photo-1560250097-0b93528c311a?ixlib=rb-4.0.3"
    }
]

TRUCKING_SOURCES = [
    {
        "url": "https://www.ttnews.com/articles/logistics",
        "article_selector": "article",
        "title_selector": "h2,h3",
        "summary_selector": "p,div.field--name-field-deckhead"
    },
    {
        "url": "https://www.ccjdigital.com/",
        "article_selector": "article",
        "title_selector": "h2,h3",
        "summary_selector": "p.entry-summary,p"
    },
    {
        "url": "https://www.overdriveonline.com/",
        "article_selector": "article",
        "title_selector": "h2,h3",
        "summary_selector": "p"
    },
    {
        "url": "https://www.fleetowner.com/",
        "article_selector": "article,div.node--type-article",
        "title_selector": "h2,h3",
        "summary_selector": "p,div.field--name-field-subheadline"
    },
]

text_client = OpenAI(api_key=ECOMAGENT_API_KEY, base_url=ECOMAGENT_BASE_URL) if ECOMAGENT_API_KEY else None
image_client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL) if OPENAI_API_KEY else None


def log(message: str) -> None:
    print(message, flush=True)


def clean_text(value: str, max_len: int | None = None) -> str:
    text = (value or "").strip()
    text = re.sub(r"^```(?:json|html)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    text = text.replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if max_len and len(text) > max_len:
        return text[:max_len].rstrip() + "..."
    return text


def parse_json_from_response(raw_text: str):
    if not raw_text:
        raise ValueError("Empty model response")

    candidates = []
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


def call_model(prompt: str, model: str | None = None) -> str:
    if text_client is None:
        raise RuntimeError("ECOMAGENT_API_KEY is not configured")

    response = text_client.chat.completions.create(
        model=model or ECOMAGENT_MODEL,
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
        "p", "h2", "h3", "ul", "ol", "li", "blockquote", "strong", "em", "b", "i", "a", "br", "img"
    }

    for tag in soup.find_all(True):
        if tag.name not in allowed_tags:
            tag.unwrap()
            continue

        attrs = {}
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

    return f"<p>{plain}</p>"


def normalize_keywords(raw_keywords: str) -> str:
    raw_keywords = raw_keywords.replace("\n", ",")
    parts = [clean_text(part, max_len=80).strip(",") for part in raw_keywords.split(",")]
    filtered = []
    seen = set()

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


def get_next_post_id() -> str:
    max_id = 0

    for post_file in LOCAL_BLOG_DIR.glob("*.json"):
        if post_file.name == "index.json":
            continue

        parsed = parse_numeric_post_id(post_file.stem)
        if parsed:
            max_id = max(max_id, parsed)

        try:
            with open(post_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            parsed_payload_id = parse_numeric_post_id(payload.get("id"))
            if parsed_payload_id:
                max_id = max(max_id, parsed_payload_id)
        except Exception:
            continue

    return f"bp{max_id + 1}"


def fetch_trucking_articles() -> list[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    articles = []

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
                    "truck", "trucking", "fleet", "haul", "freight", "driver", "diesel", "transport"
                ]):
                    continue

                articles.append({
                    "title": title,
                    "summary": summary,
                })
        except Exception as exc:
            log(f"Skipping source {site['url']} due to fetch error: {exc}")

    deduped = []
    seen_titles = set()
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

    validated = []
    for item in raw_topics:
        if not isinstance(item, dict):
            continue

        title = clean_text(item.get("title", ""), max_len=180)
        summary = clean_text(item.get("summary", ""), max_len=300)
        relevance = clean_text(item.get("relevance", ""), max_len=300)
        category = clean_text(item.get("category", ""), max_len=60)

        if not title or not summary:
            continue

        if not relevance:
            relevance = "This topic is relevant to fleet operators and logistics managers due to current market and operational impacts."

        topic = {
            "title": title,
            "summary": summary,
            "relevance": relevance,
        }

        if category:
            topic["category"] = category

        validated.append(topic)

    return validated


def get_current_logistics_topics() -> list[dict]:
    # Attempt 1: scrape current trucking websites + synthesize into topic candidates
    try:
        log("Fetching live trucking/logistics articles...")
        scraped_articles = fetch_trucking_articles()
        if len(scraped_articles) >= 3:
            payload = json.dumps(scraped_articles[:12], ensure_ascii=False)
            prompt = f"""
You are assisting a commercial trucking company blog editor.

Using the article list below, generate 5 strong blog topics for fleet operators and logistics managers.
Use the latest context from the provided list. Return ONLY valid JSON array.

Required JSON format:
[
  {{"title": "...", "summary": "...", "relevance": "...", "category": "..."}},
  ...
]

Category must be one of these themes: Industry Trends, Fleet Management, Regulations, Fuel Management, Safety, Technology Trends, Driver Retention.

Articles:
{payload}
"""
            response_text = call_model(prompt, model=ECOMAGENT_TOPIC_MODEL)
            parsed = parse_json_from_response(response_text)
            topics = validate_topics(parsed)
            if len(topics) >= 3:
                log(f"Generated {len(topics)} topics from live source context")
                return topics
    except Exception as exc:
        log(f"Live-source topic generation failed: {exc}")

    # Attempt 2: direct trend generation
    try:
        log("Generating topical ideas directly from model...")
        today = datetime.utcnow().strftime("%B %d, %Y")
        prompt = f"""
Today is {today}. Propose 5 timely blog topics for a semi-truck logistics company.

Return ONLY valid JSON array in this format:
[
  {{"title": "...", "summary": "...", "relevance": "...", "category": "..."}},
  ...
]

Requirements:
- Focus on commercial trucking, freight hauling, fleet operations, and compliance
- Keep each summary to 1-2 sentences
- No invented statistics or fabricated named sources
"""
        response_text = call_model(prompt, model=ECOMAGENT_TOPIC_MODEL)
        parsed = parse_json_from_response(response_text)
        topics = validate_topics(parsed)
        if topics:
            log(f"Generated {len(topics)} model-based trending topics")
            return topics
    except Exception as exc:
        log(f"Fallback trend generation failed: {exc}")

    # Attempt 3: category-based synthetic fallback
    log("Using category-based fallback topics")
    selected_categories = random.sample(BLOG_CATEGORIES, min(5, len(BLOG_CATEGORIES)))
    generated = []

    for category in selected_categories:
        try:
            prompt = f"""
Create one blog topic for a commercial trucking company in category "{category}".
Return ONLY valid JSON object with keys: title, summary, relevance, category.
No markdown.
"""
            response_text = call_model(prompt, model=ECOMAGENT_TOPIC_MODEL)
            parsed = parse_json_from_response(response_text)
            if isinstance(parsed, dict):
                parsed["category"] = clean_text(parsed.get("category") or category, max_len=60)
                validated = validate_topics([parsed])
                if validated:
                    generated.extend(validated)
        except Exception:
            continue

    if generated:
        return generated

    # Hard fallback
    return [
        {
            "title": "Fleet Uptime Strategies for 2026: Reducing Unplanned Downtime",
            "summary": "How fleets can cut roadside breakdowns through preventive maintenance workflows and better service scheduling.",
            "relevance": "Unplanned downtime directly impacts on-time delivery performance and operating margins.",
            "category": "Fleet Management",
        },
        {
            "title": "Preparing for New Compliance Pressure in Long-Haul Operations",
            "summary": "A practical overview of compliance controls fleets should review to stay audit-ready.",
            "relevance": "Compliance penalties and service interruptions create avoidable risk for growing carriers.",
            "category": "Regulations",
        },
        {
            "title": "Fuel Cost Volatility: Route and Dispatch Tactics That Protect Margin",
            "summary": "Operational changes dispatch teams can apply to reduce empty miles and fuel waste.",
            "relevance": "Fuel is one of the largest variable costs in trucking operations.",
            "category": "Fuel Management",
        },
    ]


def get_relevant_image(topic: dict) -> tuple[str, str]:
    if image_client is None:
        raise RuntimeError("OPENAI_API_KEY is not configured for image generation")

    if not REQUIRE_IMAGE_GENERATION:
        raise RuntimeError("REQUIRE_IMAGE_GENERATION is false; strict image mode requires true")

    prompt = call_model(
        f"""
Create a short, visual image prompt for a blog post cover image.
Topic title: {topic.get('title', '')}
Summary: {topic.get('summary', '')}

Return only the prompt text. Keep it under 120 words and focused on commercial trucking.
""",
        model=ECOMAGENT_MODEL,
    )
    prompt = clean_text(prompt, max_len=700)

    response = image_client.images.generate(
        model=OPENAI_IMAGE_MODEL,
        prompt=prompt,
        size=OPENAI_IMAGE_SIZE,
    )

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

    # Support local temporary file path produced from b64 payload
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


def choose_category_for_topic(topic: dict) -> str:
    if topic.get("category"):
        return clean_text(topic["category"], max_len=60)

    text = f"{topic.get('title', '')} {topic.get('summary', '')}".lower()

    keyword_map = {
        "Regulations": ["regulation", "compliance", "fmcsa", "dot", "mandate"],
        "Fuel Management": ["fuel", "diesel", "efficiency", "mileage"],
        "Safety": ["safety", "accident", "incident", "risk"],
        "Technology Trends": ["technology", "ai", "telematics", "automation", "software"],
        "Driver Retention": ["driver", "retention", "recruit", "wellness"],
        "Fleet Management": ["fleet", "maintenance", "uptime", "dispatch"],
        "Industry Trends": ["market", "trend", "economy", "demand", "outlook"],
    }

    best_category = "Fleet Management"
    best_score = -1
    for category, keywords in keyword_map.items():
        score = sum(text.count(keyword) for keyword in keywords)
        if score > best_score:
            best_score = score
            best_category = category

    return best_category


def generate_meta_description(title: str) -> str:
    prompt = f"""
Write one SEO meta description (max 155 chars) for this logistics blog title:
"{title}"

Requirements:
- Audience: fleet operators and logistics managers
- Mention trucking/logistics context naturally
- Return plain text only (no hashtags, no quotes, no markdown)
"""

    response = call_model(prompt, model=ECOMAGENT_MODEL)
    description = clean_text(response, max_len=155)
    return description or "Trucking and logistics insights for fleet operators and transportation teams."


def generate_keywords(title: str) -> str:
    prompt = f"""
Generate 5-7 SEO keywords for this blog title:
"{title}"

Return only comma-separated keywords. No numbering.
"""

    response = call_model(prompt, model=ECOMAGENT_MODEL)
    return normalize_keywords(response)


def generate_post_content(topic: dict, category: str, post_date_display: str, keywords: str) -> str:
    prompt = f"""
Write a detailed blog post for Pro Truck Logistics.

Title: {topic.get('title', '')}
Context: {topic.get('summary', '')}
Industry relevance: {topic.get('relevance', '')}
Category: {category}
Date: {post_date_display}
Target keywords: {keywords}

Requirements:
- Audience: fleet operators, dispatchers, logistics managers, and drivers
- 1 intro + 3-4 body sections + practical conclusion
- Use practical, actionable guidance
- No fabricated statistics and no fake named experts
- If data is uncertain, say so in general terms
- OUTPUT ONLY HTML FRAGMENT (no <!DOCTYPE>, no <html>, no <head>, no <body>)
- Allowed tags: <p>, <h2>, <h3>, <ul>, <ol>, <li>, <blockquote>, <strong>, <em>, <a>
"""

    raw_content = call_model(prompt, model=ECOMAGENT_MODEL)
    return normalize_html_content(raw_content)


def build_absolute_blog_url(path: str) -> str:
    if is_safe_url(path):
        parsed = urlparse(path)
        if parsed.scheme in {"http", "https"}:
            return path

    trimmed = (path or "").lstrip("/")
    if trimmed.startswith("blog-posts/"):
        return f"{SITE_BASE_URL}/{trimmed}"

    return f"{SITE_BASE_URL}/blog-posts/{trimmed}"


def is_topic_too_similar(candidate_title: str) -> bool:
    index_path = LOCAL_BLOG_DIR / "index.json"
    if not index_path.exists():
        return False

    try:
        with open(index_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return False

    if not isinstance(payload, list):
        return False

    candidate_norm = clean_text(candidate_title, max_len=220).lower()
    if not candidate_norm:
        return False

    recent_titles = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        title = clean_text(str(item.get("title", "")), max_len=220).lower()
        if title:
            recent_titles.append(title)
        if len(recent_titles) >= 30:
            break

    for existing in recent_titles:
        similarity = SequenceMatcher(None, candidate_norm, existing).ratio()
        if similarity >= 0.9:
            return True

    return False


def generate_blog_post(topic: dict) -> dict:
    title = clean_text(topic.get("title", ""), max_len=180)
    if not title:
        raise ValueError("Topic missing title")

    if is_topic_too_similar(title):
        raise ValueError(f"Topic too similar to recent posts: {title}")

    log(f"Generating blog post: {title}")

    author = random.choice(AUTHORS)
    category = choose_category_for_topic(topic)
    post_id = get_next_post_id()

    now = datetime.utcnow()
    post_date_display = now.strftime("%B %d, %Y")
    sort_date = now.strftime("%Y-%m-%d")

    meta_description = generate_meta_description(title)
    keywords = generate_keywords(title)
    content = generate_post_content(topic, category, post_date_display, keywords)

    image_url, image_source = get_relevant_image(topic)
    local_or_remote_image = download_and_save_image(image_url, post_id)

    excerpt = generate_excerpt(content)
    read_time = f"{random.randint(7, 10)} min read"

    return {
        "id": post_id,
        "title": title,
        "excerpt": excerpt,
        "date": post_date_display,
        "sort_date": sort_date,
        "category": category,
        "author": author["name"],
        "author_position": author["position"],
        "author_bio": author["bio"],
        "author_image": author["image"],
        "read_time": read_time,
        "content": content,
        "image": local_or_remote_image,
        "image_source": image_source,
        "meta": {
            "description": meta_description,
            "keywords": keywords,
        },
    }


def save_blog_post(post: dict) -> Path:
    post_id = post["id"]
    file_path = LOCAL_BLOG_DIR / f"{post_id}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(post, f, ensure_ascii=False, indent=2)

    log(f"Saved JSON post: {file_path}")
    return file_path


def update_meta_tag(soup: BeautifulSoup, *, meta_id: str | None = None, name: str | None = None,
                    property_name: str | None = None, content: str = "") -> None:
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


def create_blog_post_html(post: dict) -> Path:
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

    # Document title
    if soup.title:
        soup.title.string = f"{clean_text(post['title'])} | Pro Truck Logistics"

    # Meta and OG tags
    update_meta_tag(soup, meta_id="meta-description", name="description", content=post["meta"]["description"])
    update_meta_tag(soup, meta_id="meta-keywords", name="keywords", content=post["meta"]["keywords"])
    update_meta_tag(soup, meta_id="og-title", property_name="og:title", content=f"{post['title']} | Pro Truck Logistics")
    update_meta_tag(soup, meta_id="og-description", property_name="og:description", content=post["meta"]["description"])
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

    # Shared includes fetch path alignment with hosting layout
    for script in soup.find_all("script"):
        script_text = script.string
        if not script_text:
            continue

        updated_script_text = script_text.replace("../blog-includes.html", includes_href)
        if updated_script_text != script_text:
            script.string = updated_script_text

    # Header and core fields
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

    # Content insertion (already sanitized fragment)
    post_content_container = soup.find(id="post-content")
    if post_content_container is not None:
        post_content_container.clear()
        fragment = BeautifulSoup(post["content"], "html.parser")
        for child in list(fragment.contents):
            post_content_container.append(child)

    # Author block
    author_img = soup.find(id="author-image")
    if author_img is not None:
        author_img["src"] = post["author_image"]
        author_img["alt"] = clean_text(post["author"], max_len=120)

    set_text_by_id(soup, "author-name", post["author"])
    set_text_by_id(soup, "author-position", post["author_position"])
    set_text_by_id(soup, "author-bio", post["author_bio"])

    # Share links
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

    # Schema
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


def parse_sort_date(value: str | None) -> datetime:
    if not value:
        return datetime.min

    for fmt in ("%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return datetime.min


def update_blog_index(new_posts: list[dict]) -> Path:
    index_path = LOCAL_BLOG_DIR / "index.json"
    existing_posts = []

    if index_path.exists():
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, list):
                existing_posts = payload
        except Exception:
            existing_posts = []

    posts_by_id = {}
    for post in existing_posts:
        if isinstance(post, dict) and "id" in post:
            posts_by_id[str(post["id"])] = post

    for post in new_posts:
        image_path = post["image"]
        if image_path.startswith("images/"):
            image_path = f"blog-posts/{image_path}"

        index_post = {
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
        posts_by_id[str(post["id"])] = index_post

    all_posts = list(posts_by_id.values())

    def sort_key(item: dict):
        sort_date = parse_sort_date(item.get("sort_date") or item.get("date"))
        numeric_id = parse_numeric_post_id(item.get("id")) or 0
        return sort_date, numeric_id

    all_posts.sort(key=sort_key, reverse=True)

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=2)

    log(f"Updated blog index: {index_path}")
    return index_path


def upload_files_to_server() -> bool:
    if SKIP_UPLOAD:
        log("SKIP_UPLOAD=true, skipping upload phase")
        return True

    if FTP_IS_SFTP:
        return upload_files_via_sftp()

    return upload_files_via_ftp()


def upload_files_via_sftp() -> bool:
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

        log(f"Connecting to SFTP host {FTP_HOST}...")
        ssh.connect(hostname=FTP_HOST, username=FTP_USER, password=FTP_PASS, timeout=20)
        sftp = ssh.open_sftp()

        try:
            sftp.stat(FTP_BLOG_DIR)
        except FileNotFoundError:
            path_parts = FTP_BLOG_DIR.strip("/").split("/")
            current = ""
            for part in path_parts:
                current += f"/{part}"
                try:
                    sftp.stat(current)
                except FileNotFoundError:
                    sftp.mkdir(current)

        remote_images_path = f"{FTP_BLOG_DIR.rstrip('/')}/images"
        try:
            sftp.stat(remote_images_path)
        except FileNotFoundError:
            sftp.mkdir(remote_images_path)

        regular_files = list(LOCAL_BLOG_DIR.glob("*.json")) + list(LOCAL_BLOG_DIR.glob("*.html"))
        image_files = list(IMAGES_DIR.glob("*.*"))

        for file_path in regular_files:
            remote_path = f"{FTP_BLOG_DIR.rstrip('/')}/{file_path.name}"
            sftp.put(str(file_path), remote_path)
            log(f"Uploaded {file_path.name}")

        for image_path in image_files:
            remote_path = f"{remote_images_path}/{image_path.name}"
            sftp.put(str(image_path), remote_path)
            log(f"Uploaded image {image_path.name}")

        sftp.close()
        ssh.close()
        log("SFTP upload completed")
        return True
    except Exception as exc:
        log(f"SFTP upload failed: {exc}")
        return False


def upload_files_via_ftp() -> bool:
    try:
        log(f"Connecting to FTP host {FTP_HOST}...")
        with ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS) as ftp:
            try:
                ftp.cwd(FTP_BLOG_DIR)
            except ftplib.error_perm:
                path_parts = FTP_BLOG_DIR.strip("/").split("/")
                for i in range(len(path_parts)):
                    segment = "/" + "/".join(path_parts[: i + 1])
                    try:
                        ftp.cwd(segment)
                    except ftplib.error_perm:
                        ftp.mkd(segment)
                        ftp.cwd(segment)

            try:
                ftp.cwd("images")
                ftp.cwd("..")
            except ftplib.error_perm:
                ftp.mkd("images")

            regular_files = list(LOCAL_BLOG_DIR.glob("*.json")) + list(LOCAL_BLOG_DIR.glob("*.html"))
            image_files = list(IMAGES_DIR.glob("*.*"))

            for file_path in regular_files:
                with open(file_path, "rb") as fp:
                    ftp.storbinary(f"STOR {file_path.name}", fp)
                log(f"Uploaded {file_path.name}")

            if image_files:
                ftp.cwd("images")
                for image_path in image_files:
                    with open(image_path, "rb") as fp:
                        ftp.storbinary(f"STOR {image_path.name}", fp)
                    log(f"Uploaded image {image_path.name}")

        log("FTP upload completed")
        return True
    except Exception as exc:
        log(f"FTP upload failed: {exc}")
        return False


def validate_runtime_configuration() -> None:
    if not ECOMAGENT_API_KEY:
        raise RuntimeError("Missing ECOMAGENT_API_KEY (or OPENAI_API_KEY fallback)")

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


def main() -> None:
    validate_runtime_configuration()

    log("Starting Pro Truck Logistics blog generation")
    log(f"Using provider base URL: {ECOMAGENT_BASE_URL}")
    log(f"Using model: {ECOMAGENT_MODEL}")

    topics = get_current_logistics_topics()
    if not topics:
        raise RuntimeError("No topics generated")

    selected_topics = random.sample(topics, min(POSTS_TO_GENERATE, len(topics)))
    log(f"Selected {len(selected_topics)} topic(s)")

    generated_posts = []
    for topic in selected_topics:
        try:
            post = generate_blog_post(topic)
        except ValueError as exc:
            log(f"Skipping topic: {exc}")
            continue

        save_blog_post(post)
        create_blog_post_html(post)
        generated_posts.append(post)
        log(f"Completed post: {post['title']}")

    if not generated_posts:
        raise RuntimeError("No posts were generated")

    update_blog_index(generated_posts)

    if not upload_files_to_server():
        raise RuntimeError("Upload failed")

    log("Blog generation pipeline completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"Fatal error: {exc}")
        sys.exit(1)
