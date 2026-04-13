#!/usr/bin/env python3
"""
TikTok Prospect Pipeline - scrape comments, rank with Cohere, enrich profiles.

Usage:  python pipeline.py --video-url "https://www.tiktok.com/@user/video/123"
Env:    APIFY_TOKEN, CO_API_KEY (from .env or shell)
Cost:   ~$0.77 per run on a 500-comment video
Docs:   See README.md for full setup, options, and troubleshooting.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import cohere
from apify_client import ApifyClient
from apify_client.errors import ApifyApiError
from cohere.errors import (
    GatewayTimeoutError,
    InternalServerError,
    ServiceUnavailableError,
    TooManyRequestsError,
)
from cohere.types import (
    JsonObjectResponseFormatV2,
    SystemChatMessageV2,
    UserChatMessageV2,
)
from dotenv import load_dotenv


# --- Configuration ---

COMMENTS_ACTOR = "clockworks/tiktok-comments-scraper"
PROFILE_ACTOR = "clockworks/tiktok-profile-scraper"

# Command A (March 2025) - $2.50/$10 per 1M tokens, 256K context, JSON schema support.
COHERE_MODEL = "command-a-03-2025"

# For the cost report only. Actual billing is server-side.
COHERE_INPUT_PRICE_PER_MTOK = 2.50
COHERE_OUTPUT_PRICE_PER_MTOK = 10.00

# "auto" = infer topic/target from the comments themselves.
DEFAULT_COMMENTS_LIMIT = 1000
DEFAULT_SHORTLIST_SIZE = 15
DEFAULT_TOPIC = "auto"
DEFAULT_TARGET = "auto"


# --- Logging ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")


# --- Data classes ---


@dataclass
class Comment:
    """Normalized from the raw Apify response. See scrape_comments() for mapping."""

    username: str
    text: str
    likes: int
    replies: int
    created_at: str
    video_url: str


@dataclass
class LLMRanking:
    """Cohere's ranking output for one prospect."""

    username: str
    comment_excerpt: str
    why_prospect: str
    priority: str  # "high" | "medium" | "low"


@dataclass
class Prospect:
    """Final row in prospects.csv - ranking + comment + profile joined."""

    username: str
    priority: str
    why_prospect: str
    comment_text: str
    comment_likes: int
    comment_replies: int
    video_url: str
    followers: int
    bio: str
    bio_link: str
    is_seller: bool
    is_verified: bool
    is_private: bool
    profile_url: str


@dataclass
class CostReport:
    """Cumulative cost tracking for a single pipeline run."""

    comments_scraper_usd: float = 0.0
    profile_scraper_usd: float = 0.0
    cohere_usd: float = 0.0
    cohere_input_tokens: int = 0
    cohere_output_tokens: int = 0

    @property
    def total_usd(self) -> float:
        return (
            self.comments_scraper_usd
            + self.profile_scraper_usd
            + self.cohere_usd
        )


# --- Progress spinner (thread-friendly, stderr only) ---


class Spinner:
    """Minimal live-progress spinner for long-running steps."""

    FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

    def __init__(self, label: str):
        self.label = label
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._t0: float = 0.0

    def __enter__(self) -> "Spinner":
        self._t0 = time.monotonic()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        sys.stderr.write("\r" + " " * 80 + "\r")
        sys.stderr.flush()

    def _run(self) -> None:
        i = 0
        while not self._stop.is_set():
            frame = self.FRAMES[i % len(self.FRAMES)]
            elapsed = int(time.monotonic() - self._t0)
            sys.stderr.write(f"\r  {frame}  {self.label}  ({elapsed}s)")
            sys.stderr.flush()
            self._stop.wait(0.1)
            i += 1


# --- Apify helpers ---


def make_apify_client(token: str) -> ApifyClient:
    """ApifyClient with 10 retries and 1s min delay."""
    return ApifyClient(
        token=token,
        max_retries=10,
        min_delay_between_retries_millis=1000,
    )


def run_actor(
    client: ApifyClient,
    actor_name: str,
    run_input: dict[str, Any],
    *,
    label: str,
    timeout_secs: int = 900,
) -> tuple[list[dict], float]:
    """
    Start an Actor, wait for completion, and return (dataset items, cost USD).

    Raises RuntimeError if the Actor does not reach status SUCCEEDED.
    """
    log.info("▶  %s", label)
    t0 = time.monotonic()

    try:
        with Spinner(label):
            run = client.actor(actor_name).call(
                run_input=run_input,
                timeout_secs=timeout_secs,
                wait_secs=None,  # wait as long as the Actor needs
            )
    except ApifyApiError as e:
        log.error(
            "Apify API error while running %s: [%s %s] %s",
            actor_name,
            getattr(e, "status_code", "?"),
            getattr(e, "type", "?"),
            getattr(e, "message", str(e)),
        )
        raise

    if run is None:
        raise RuntimeError(f"{actor_name}: .call() returned None")

    status = run["status"]
    run_id = run["id"]
    cost = float(run.get("usageTotalUsd") or 0.0)
    elapsed = time.monotonic() - t0

    if status != "SUCCEEDED":
        raise RuntimeError(
            f"{actor_name} ended with status {status} "
            f"(run_id={run_id}): "
            f"{run.get('statusMessage', 'no message')}"
        )

    dataset_id = run["defaultDatasetId"]
    items = list(client.dataset(dataset_id).iterate_items())

    log.info(
        "✔  %s finished in %.1fs → %d items, $%.4f",
        label,
        elapsed,
        len(items),
        cost,
    )
    return items, cost


# --- Stage 1 - Scrape comments ---


def scrape_comments(
    client: ApifyClient,
    video_url: str,
    comments_limit: int,
) -> tuple[list[Comment], float]:
    """Call the TikTok Comments Scraper and normalize the results."""
    run_input = {
        "postURLs": [video_url],
        "commentsPerPost": comments_limit,
        "maxRepliesPerComment": 0,  # we only want top-level comments
    }

    raw_items, cost = run_actor(
        client,
        COMMENTS_ACTOR,
        run_input,
        label="Scraping comments",
    )

    comments: list[Comment] = []
    for item in raw_items:
        username = (item.get("uniqueId") or "").strip()
        text = (item.get("text") or "").strip()
        if not username or not text:
            continue
        comments.append(
            Comment(
                username=username,
                text=text,
                likes=int(item.get("diggCount") or 0),
                replies=int(item.get("replyCommentTotal") or 0),
                created_at=str(item.get("createTimeISO") or ""),
                video_url=str(item.get("videoWebUrl") or video_url),
            )
        )

    return comments, cost


def deduplicate_comments(comments: list[Comment]) -> list[Comment]:
    """
    Keep one comment per username - the highest-engagement one.

    Tie-breaking: higher `likes` wins; on a tie the longer `text` wins.
    This is purely client-side; the Apify Actor has no server-side dedup flag.
    """
    best: dict[str, Comment] = {}
    for c in comments:
        key = c.username.lower()
        prev = best.get(key)
        if prev is None:
            best[key] = c
            continue
        if c.likes > prev.likes:
            best[key] = c
        elif c.likes == prev.likes and len(c.text) > len(prev.text):
            best[key] = c
    return list(best.values())


# --- Stage 2 - Cohere ranking ---


_RETRYABLE_COHERE_ERRORS = (
    TooManyRequestsError,
    ServiceUnavailableError,
    GatewayTimeoutError,
    InternalServerError,
)


def _build_prospect_schema() -> dict[str, Any]:
    """
    JSON Schema for the Cohere `response_format` - guarantees well-formed
    structured output with constrained decoding (no parsing retry loop
    needed in practice, but we still defensively handle malformed JSON).
    """
    return {
        "type": "object",
        "properties": {
            "prospects": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "username": {"type": "string"},
                        "comment_excerpt": {"type": "string"},
                        "why_prospect": {"type": "string"},
                        "priority": {
                            "type": "string",
                            "enum": ["high", "medium", "low"],
                        },
                    },
                    "required": [
                        "username",
                        "comment_excerpt",
                        "why_prospect",
                        "priority",
                    ],
                },
            }
        },
        "required": ["prospects"],
    }


def _build_system_prompt(topic: str, target: str, shortlist_size: int) -> str:
    return f"""You are a B2B prospect-qualification analyst helping a sales team find high-intent leads inside a TikTok comment section.

Context:
- Source video topic: {topic}
- Target buyer: {target}

Your task:
From the JSON array of TikTok comments (each has: username, text, likes, replies), identify up to {shortlist_size} commenters who look like real prospects for cold outreach. Rank them by **buying intent** - how close each commenter is to needing and buying a product or service today.

STRICT RANKING RUBRIC - use these exact definitions:

- **high** - The commenter is clearly running a real business RIGHT NOW. Signals: they mention their own store/shop/service/product (e.g., "I have a TikTok shop", "my Shopify store", "my embroidery business", "I'm a realtor"), OR their username explicitly references a business or profession (e.g., "@something_realtor", "@shopname", "@agencyname", "*_biz"), OR they describe an active, specific business pain point they are trying to solve (e.g., "my shop isn't getting views", "shadow banned every post", "can't get PayPal"). These are the prospects you would email first.

- **medium** - The commenter has relevant signals but weaker ones. Signals: they describe trying or planning to start a business (not yet running), OR they ask a substantive how-to question about running/scaling a business, OR their username is business-adjacent but ambiguous. These are warm leads, not hot ones.

- **low** - The commenter has only mild signals: general business curiosity, personal development talk, or weak engagement with the topic. These are cold leads worth a final pass but not immediate outreach.

DO NOT include these - skip them entirely:
- Generic reactions, fan messages, emoji-only comments, and off-topic noise.
- Skeptics, complainers, and people attacking the creator or the topic.
- Scam accounts (usernames pushing Telegram, WhatsApp, crypto, or "DM for money" in the comment).
- People who explicitly say they can't use the product in their country.
- People venting about debt or financial hardship with no business context (these are not buyers).

Tie-breaker between two similar comments: prefer the one with higher likes and more replies (stronger engagement = stronger signal).

Return EXACTLY one JSON object with a single `prospects` field containing up to {shortlist_size} ranked objects. Each object has these fields:
- username: the TikTok handle, WITHOUT the @ prefix. Must appear verbatim in the input array - do not invent usernames.
- comment_excerpt: a short verbatim quote (≤140 chars) from their actual comment
- why_prospect: one sentence explaining the specific signal that made them a prospect
- priority: "high", "medium", or "low" - use the rubric above strictly

Order the array by priority (all highs first, then mediums, then lows). Within a priority, order by strength of signal. Favor quality over quantity - it is better to return 8 strong prospects than {shortlist_size} weak ones."""


def rank_prospects(
    cohere_client: cohere.ClientV2,
    comments: list[Comment],
    topic: str,
    target: str,
    shortlist_size: int,
) -> tuple[list[LLMRanking], int, int]:
    """
    Send the deduped comments to Cohere Command A and return a ranked
    shortlist plus the input/output token counts for cost tracking.
    """
    comment_payload = [
        {
            "username": c.username,
            "text": c.text,
            "likes": c.likes,
            "replies": c.replies,
        }
        for c in comments
    ]

    # Auto-detect from comments if set to "auto".
    resolved_topic = topic.strip()
    if not resolved_topic or resolved_topic.lower() == "auto":
        sample_texts = " | ".join(c.text[:80] for c in comments[:15])
        resolved_topic = (
            "a TikTok video. Here is a sample of the comments to help "
            "you understand the topic and audience: "
            + sample_texts[:500]
        )

    resolved_target = target.strip()
    if not resolved_target or resolved_target.lower() == "auto":
        resolved_target = (
            "anyone who appears to be running, starting, or actively "
            "trying to grow a business - regardless of industry or "
            "niche. Look for signals of real business activity (stores, "
            "services, products, clients) rather than passive interest."
        )

    system_prompt = _build_system_prompt(
        resolved_topic, resolved_target, shortlist_size
    )
    user_message = (
        "Here is the full JSON array of comments. "
        "Return the ranked prospect shortlist now.\n\n"
        + json.dumps(comment_payload, ensure_ascii=False)
    )
    schema = _build_prospect_schema()

    def _call_once() -> tuple[list[LLMRanking], int, int]:
        response = cohere_client.chat(
            model=COHERE_MODEL,
            messages=[
                SystemChatMessageV2(content=system_prompt),
                UserChatMessageV2(content=user_message),
            ],
            response_format=JsonObjectResponseFormatV2(json_schema=schema),
            temperature=0.1,
            max_tokens=4000,
        )

        if response.finish_reason not in ("COMPLETE", "STOP_SEQUENCE"):
            raise RuntimeError(
                f"Cohere call did not complete cleanly: "
                f"finish_reason={response.finish_reason}"
            )

        # response.message.content is a list of content blocks; keep only
        # text blocks (the model may emit thinking blocks too).
        raw_text = ""
        content_blocks = response.message.content or []
        for block in content_blocks:
            block_type = getattr(block, "type", None)
            if block_type == "text":
                raw_text += getattr(block, "text", "") or ""
        if not raw_text:
            raise RuntimeError(
                "Cohere response contained no text content blocks"
            )
        parsed = json.loads(raw_text)

        items = parsed.get("prospects") or []
        rankings: list[LLMRanking] = []
        seen: set[str] = set()
        for item in items:
            username = (item.get("username") or "").lstrip("@").strip()
            if not username or username.lower() in seen:
                continue
            seen.add(username.lower())
            rankings.append(
                LLMRanking(
                    username=username,
                    comment_excerpt=str(item.get("comment_excerpt") or ""),
                    why_prospect=str(item.get("why_prospect") or ""),
                    priority=str(item.get("priority") or "low").lower(),
                )
            )

        usage = getattr(response, "usage", None)
        input_tokens = 0
        output_tokens = 0
        if usage and getattr(usage, "tokens", None):
            input_tokens = int(usage.tokens.input_tokens or 0)
            output_tokens = int(usage.tokens.output_tokens or 0)

        return rankings, input_tokens, output_tokens

    last_error: Exception | None = None
    for attempt in range(5):
        try:
            with Spinner("Ranking prospects with Cohere Command A"):
                return _call_once()
        except _RETRYABLE_COHERE_ERRORS as e:
            last_error = e
            sleep = min(30.0, (2**attempt)) * max(0.5, random.random())
            if isinstance(e, cohere.errors.TooManyRequestsError):
                sleep = max(sleep, 5.0)
            log.warning(
                "Cohere transient error (%s); retrying in %.1fs",
                type(e).__name__,
                sleep,
            )
            time.sleep(sleep)
        except json.JSONDecodeError as e:
            # Schema mode should prevent this, but guard as a safety net.
            last_error = e
            log.warning(
                "Cohere returned malformed JSON; retrying (attempt %d/5)",
                attempt + 1,
            )
            time.sleep(1.0)

    raise RuntimeError(
        f"Cohere ranking failed after 5 attempts"
    ) from last_error


def estimate_cohere_cost(input_tokens: int, output_tokens: int) -> float:
    """Rough USD cost estimate using Command A public pricing."""
    return (
        input_tokens / 1_000_000 * COHERE_INPUT_PRICE_PER_MTOK
        + output_tokens / 1_000_000 * COHERE_OUTPUT_PRICE_PER_MTOK
    )


# --- Stage 3 - Enrich profiles ---


def enrich_profiles(
    client: ApifyClient,
    usernames: list[str],
) -> tuple[dict[str, dict], float]:
    """
    Run the TikTok Profile Scraper against the shortlisted usernames and
    return {lowercase_username: authorMeta_dict}.
    """
    clean_usernames = sorted({u.lstrip("@").strip() for u in usernames if u})
    if not clean_usernames:
        return {}, 0.0

    run_input = {
        "profiles": clean_usernames,
        "resultsPerPage": 1,  # we only need profile meta, not video history
        "profileScrapeSections": ["videos"],
        "profileSorting": "latest",
        "excludePinnedPosts": False,
        "shouldDownloadAvatars": False,
        "shouldDownloadCovers": False,
        "shouldDownloadVideos": False,
    }

    raw_items, cost = run_actor(
        client,
        PROFILE_ACTOR,
        run_input,
        label="Enriching profiles",
    )

    profiles_by_username: dict[str, dict] = {}
    for item in raw_items:
        author = item.get("authorMeta") or {}
        name = (author.get("name") or "").strip().lower()
        if not name:
            continue
        # One row per video - keep only the first row we see per username
        profiles_by_username.setdefault(name, author)

    return profiles_by_username, cost


# --- Stage 4 - Join ---


def _coerce_bio_link(bio_link: Any) -> str:
    """TikTok's bioLink is sometimes a dict {'link': '...'}, sometimes a str."""
    if isinstance(bio_link, dict):
        return str(bio_link.get("link") or "")
    if isinstance(bio_link, str):
        return bio_link
    return ""


def join_prospects(
    rankings: list[LLMRanking],
    comments_by_username: dict[str, Comment],
    profiles_by_username: dict[str, dict],
) -> list[Prospect]:
    """
    Combine the LLM ranking, the original comment, and the enriched profile
    into one unified Prospect per row. Ranking order is preserved.
    """
    priority_rank = {"high": 0, "medium": 1, "low": 2}
    rankings_sorted = sorted(
        rankings, key=lambda r: priority_rank.get(r.priority, 99)
    )

    prospects: list[Prospect] = []
    for r in rankings_sorted:
        key = r.username.lower()
        comment = comments_by_username.get(key)
        profile = profiles_by_username.get(key) or {}

        prospects.append(
            Prospect(
                username=r.username,
                priority=r.priority,
                why_prospect=r.why_prospect,
                comment_text=(comment.text if comment else r.comment_excerpt),
                comment_likes=(comment.likes if comment else 0),
                comment_replies=(comment.replies if comment else 0),
                video_url=(comment.video_url if comment else ""),
                followers=int(profile.get("fans") or 0),
                bio=str(profile.get("signature") or ""),
                bio_link=_coerce_bio_link(profile.get("bioLink")),
                is_seller=bool(profile.get("ttSeller") or False),
                is_verified=bool(profile.get("verified") or False),
                is_private=bool(profile.get("privateAccount") or False),
                profile_url=str(
                    profile.get("profileUrl")
                    or f"https://www.tiktok.com/@{r.username}"
                ),
            )
        )

    return prospects


# --- Stage 5 - Write output ---


def write_prospects_csv(prospects: list[Prospect], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "priority",
        "username",
        "followers",
        "is_seller",
        "is_verified",
        "is_private",
        "bio",
        "bio_link",
        "profile_url",
        "comment_text",
        "comment_likes",
        "comment_replies",
        "why_prospect",
        "video_url",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for p in prospects:
            writer.writerow(
                {
                    "priority": p.priority,
                    "username": p.username,
                    "followers": p.followers,
                    "is_seller": p.is_seller,
                    "is_verified": p.is_verified,
                    "is_private": p.is_private,
                    "bio": p.bio,
                    "bio_link": p.bio_link,
                    "profile_url": p.profile_url,
                    "comment_text": p.comment_text,
                    "comment_likes": p.comment_likes,
                    "comment_replies": p.comment_replies,
                    "why_prospect": p.why_prospect,
                    "video_url": p.video_url,
                }
            )


def write_debug_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)


# --- Summary printer ---


def print_summary(
    prospects: list[Prospect],
    cost: CostReport,
    video_url: str,
    total_elapsed_sec: float,
    output_path: Path,
) -> None:
    header = "═" * 72
    sub = "─" * 72
    print()
    print(header)
    print("  TIKTOK PROSPECT PIPELINE - RESULTS")
    print(header)
    print(f"  Source video : {video_url}")
    print(f"  Prospects    : {len(prospects)}")
    print(f"  Output file  : {output_path}")
    print(f"  Total runtime: {total_elapsed_sec:.1f}s")
    print(sub)

    top = [p for p in prospects if p.priority == "high"][:3]
    if top:
        print("  TOP 3 HIGH-PRIORITY PROSPECTS")
        print(sub)
        for i, p in enumerate(top, 1):
            seller_badge = " ⭐ TikTok Seller" if p.is_seller else ""
            verified_badge = " ✓ verified" if p.is_verified else ""
            bio_link_label = p.bio_link or "(no bio link)"
            print(
                f"  {i}. @{p.username}  "
                f"- {p.followers:,} followers{seller_badge}{verified_badge}"
            )
            print(f"     bio link    : {bio_link_label}")
            print(
                f'     comment     : "{_truncate(p.comment_text, 120)}"'
            )
            print(f"     why prospect: {_truncate(p.why_prospect, 100)}")
            print()

    # Cost breakdown
    print(sub)
    print("  COST BREAKDOWN")
    print(sub)
    print(
        f"  Comments Scraper : ${cost.comments_scraper_usd:.4f}"
    )
    print(
        f"  Cohere Command A : ${cost.cohere_usd:.4f}   "
        f"({cost.cohere_input_tokens:,} in + "
        f"{cost.cohere_output_tokens:,} out tokens)"
    )
    print(
        f"  Profile Scraper  : ${cost.profile_scraper_usd:.4f}"
    )
    print(sub)
    print(f"  TOTAL            : ${cost.total_usd:.4f}")
    print(header)
    print()


def _truncate(s: str, limit: int) -> str:
    s = s.replace("\n", " ").strip()
    if len(s) <= limit:
        return s
    return s[: limit - 1] + "…"


# --- Entry point ---


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "TikTok prospect pipeline - scrape comments, filter with Cohere, "
            "enrich with Profile Scraper, and write a unified prospects.csv."
        ),
    )
    parser.add_argument(
        "--video-url",
        required=True,
        help="Full TikTok video URL to scrape comments from",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=(
            "Free-text description of the source video's topic "
            "(used in the LLM prompt)"
        ),
    )
    parser.add_argument(
        "--target",
        default=DEFAULT_TARGET,
        help=(
            "Description of the ideal prospect for your business "
            "(used in the LLM prompt)"
        ),
    )
    parser.add_argument(
        "--shortlist-size",
        type=int,
        default=DEFAULT_SHORTLIST_SIZE,
        help="Max number of prospects to return",
    )
    parser.add_argument(
        "--comments-limit",
        type=int,
        default=DEFAULT_COMMENTS_LIMIT,
        help="Max comments to scrape from the source video",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory to write prospects.csv and debug files into",
    )
    parser.add_argument(
        "--save-raw",
        action="store_true",
        help="Also save raw comment/profile JSON for debugging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()

    apify_token = os.environ.get("APIFY_TOKEN")
    cohere_key = os.environ.get("CO_API_KEY")
    missing: list[str] = []
    if not apify_token:
        missing.append("APIFY_TOKEN")
    if not cohere_key:
        missing.append("CO_API_KEY")
    if missing:
        log.error(
            "Missing environment variables: %s. "
            "Copy .env.example to .env and fill in your keys.",
            ", ".join(missing),
        )
        return 2
    # Both variables are guaranteed non-None past this point.
    assert apify_token is not None
    assert cohere_key is not None

    apify_client = make_apify_client(apify_token)
    cohere_client = cohere.ClientV2(
        api_key=cohere_key,
        timeout=180.0,
        client_name="tiktok-prospect-pipeline/1.0",
    )

    cost = CostReport()
    pipeline_t0 = time.monotonic()

    try:
        # ----- Stage 1: scrape comments ---------------------------------
        comments, comments_cost = scrape_comments(
            apify_client,
            args.video_url,
            args.comments_limit,
        )
        cost.comments_scraper_usd = comments_cost

        if not comments:
            log.error("No comments returned. Check the video URL.")
            return 1

        raw_count = len(comments)
        unique_comments = deduplicate_comments(comments)
        log.info(
            "Deduplicated %d raw comments → %d unique commenters",
            raw_count,
            len(unique_comments),
        )

        if args.save_raw:
            write_debug_json(
                [c.__dict__ for c in unique_comments],
                args.output_dir / "raw_comments.json",
            )

        # ----- Stage 2: rank with Cohere --------------------------------
        rankings, in_tokens, out_tokens = rank_prospects(
            cohere_client,
            unique_comments,
            args.topic,
            args.target,
            args.shortlist_size,
        )
        cost.cohere_input_tokens = in_tokens
        cost.cohere_output_tokens = out_tokens
        cost.cohere_usd = estimate_cohere_cost(in_tokens, out_tokens)
        log.info(
            "✔  Cohere returned %d ranked prospects "
            "(%d in + %d out tokens, ~$%.4f)",
            len(rankings),
            in_tokens,
            out_tokens,
            cost.cohere_usd,
        )

        if not rankings:
            log.error("Cohere returned zero prospects - nothing to enrich")
            return 1

        # ----- Stage 3: enrich profiles ---------------------------------
        shortlist = [r.username for r in rankings]
        profiles, profile_cost = enrich_profiles(apify_client, shortlist)
        cost.profile_scraper_usd = profile_cost

        if args.save_raw:
            write_debug_json(
                profiles,
                args.output_dir / "raw_profiles.json",
            )

        # ----- Stage 4: join --------------------------------------------
        comments_by_username = {
            c.username.lower(): c for c in unique_comments
        }
        prospects = join_prospects(
            rankings, comments_by_username, profiles
        )

        # ----- Stage 5: write output ------------------------------------
        output_path = args.output_dir / "prospects.csv"
        write_prospects_csv(prospects, output_path)
        log.info("✔  Wrote %d prospects to %s", len(prospects), output_path)

    except KeyboardInterrupt:
        log.warning("Interrupted by user")
        return 130
    except Exception as e:  # noqa: BLE001 - top-level catch for CLI UX
        log.exception("Pipeline failed: %s", e)
        return 1

    total_elapsed = time.monotonic() - pipeline_t0
    print_summary(prospects, cost, args.video_url, total_elapsed, output_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
