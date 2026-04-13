# TikTok Prospect Pipeline

End-to-end Python automation of the TikTok lead-generation workflow: scrape comments, filter with an LLM, enrich with profile data, and export a unified prospect list - all in one command.

```
┌─────────────────┐   ┌────────────────┐   ┌─────────────────┐   ┌──────────────┐
│  TikTok video   │ → │ Comments       │ → │ Cohere          │ → │ Profile      │ → prospects.csv
│  URL            │   │ Scraper (Apify)│   │ Command A       │   │ Scraper      │
└─────────────────┘   └────────────────┘   └─────────────────┘   └──────────────┘
                      508 comments          Top 15 ranked        15 enriched
                      $0.635 cost           shortlist            profiles
                                            ~$0.07 cost          $0.06 cost
```

**Total cost per run:** ~$0.77 on a 500-comment video.
**Total runtime:** ~4 minutes.
**Code surface:** one file, ~1,000 lines, three dependencies.

---

## Requirements

- **Python 3.10+**
- An **Apify account** - free, $5/month in platform credits (more than enough for this workflow).
- A **Cohere API key** - their free trial tier gives you 1,000 API calls/month without a credit card.

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/triposat/tiktok-prospect-pipeline.git
cd tiktok-prospect-pipeline
```

### 2. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows PowerShell

pip install -r requirements.txt
```

Only three packages are installed:

| Package | Version | Purpose |
|---|---|---|
| `apify-client` | `>=2.5.0,<3.0.0` | Calls the TikTok Comments & Profile Scraper Actors |
| `cohere` | `>=6.1.0,<7.0.0` | Calls Cohere Command A for prospect ranking |
| `python-dotenv` | `>=1.0.0,<2.0.0` | Loads API keys from `.env` |

No pandas, no heavyweight frameworks. Python's built-in `csv` and `json` modules handle the rest.

### 3. Configure your API keys

```bash
cp .env.example .env
```

Open `.env` and paste in your keys:

```
APIFY_TOKEN=apify_api_your_real_token_here
CO_API_KEY=your_cohere_key_here
```

- **Apify token:** https://console.apify.com/account/integrations
- **Cohere key:** https://dashboard.cohere.com/api-keys - create a trial key (no credit card required)

The `.env` file is gitignored. Never commit real secrets.

---

## Usage

The simplest run - point it at a TikTok video URL and let it go:

```bash
python pipeline.py --video-url "https://www.tiktok.com/@garyvee/video/7626061508174236941"
```

### Full option list

```bash
python pipeline.py \
  --video-url "https://www.tiktok.com/@someone/video/123..." \
  --topic "a TikTok video about real estate lead generation" \
  --target "realtors and real estate teams who would pay for CRM and marketing tools" \
  --shortlist-size 15 \
  --comments-limit 1000 \
  --output-dir ./output \
  --save-raw
```

| Flag | Default | What it does |
|---|---|---|
| `--video-url` | *(required)* | The TikTok video to scrape comments from |
| `--topic` | `auto` (inferred from comments) | Passed to the LLM so it knows what the video is about. Set to `auto` to let the pipeline detect the niche from the first 15 comments. |
| `--target` | `auto` (inferred from comments) | Describes your ideal prospect, passed to the LLM. Set to `auto` and the pipeline uses a universal "anyone running a business" rubric. |
| `--shortlist-size` | 15 | Maximum prospects the LLM should return |
| `--comments-limit` | 1000 | Max comments to scrape from the video |
| `--output-dir` | `./output` | Directory for `prospects.csv` and debug files |
| `--save-raw` | off | Also save `raw_comments.json` and `raw_profiles.json` for debugging |

### Output

`./output/prospects.csv` - one row per enriched prospect, with these columns:

| Column | Description |
|---|---|
| `priority` | `high` / `medium` / `low` (from the LLM ranking) |
| `username` | TikTok handle, no `@` |
| `followers` | Follower count |
| `is_seller` | `True` if TikTok flagged them as an active seller (`ttSeller`) |
| `is_verified` | `True` if verified on TikTok |
| `is_private` | `True` if the profile is private (filter these out for outreach) |
| `bio` | Bio text from their profile |
| `bio_link` | The URL in their bio (website, Linktree, etc.) |
| `profile_url` | Direct link to their TikTok profile |
| `comment_text` | The exact comment that got them onto the shortlist |
| `comment_likes` | Likes on that comment |
| `comment_replies` | Replies on that comment |
| `why_prospect` | One-sentence reasoning from the LLM |
| `video_url` | The source video they commented on |

### What you see in the terminal

The pipeline prints live progress during each stage (scrape → rank → enrich → join → write) and ends with a human-readable summary. Example output from a real run (profiles shown are publicly available TikTok data, used for demonstration):

```
═══════════════════════════════════════════════════════════════════
  TIKTOK PROSPECT PIPELINE - RESULTS
═══════════════════════════════════════════════════════════════════
  Source video : https://www.tiktok.com/@garyvee/video/7626061508174236941
  Prospects    : 15
  Output file  : output/prospects.csv
  Total runtime: 244.8s
  ─────────────────────────────────────────────────────────────────
  TOP 3 HIGH-PRIORITY PROSPECTS
  ─────────────────────────────────────────────────────────────────
  1. @qualityprints_embroidery  - 17,500 followers ⭐ TikTok Seller
     bio link    : https://035f3d-2.myshopify.com/
     comment     : "I have TikTok shop and that's bogus I don't get any views"
     why prospect: Active TikTok seller with a live Shopify store…

  2. @zuristarrfinds  - 15,000 followers
     bio link    : https://twitch.tv/ZuriStarr
     comment     : "I make $50/hr with my masters degree and will retire…"
     why prospect: Active seller, real engagement, email in bio…

  ─────────────────────────────────────────────────────────────────
  COST BREAKDOWN
  ─────────────────────────────────────────────────────────────────
  Comments Scraper : $0.6350
  Cohere Command A : $0.0672  (22,450 in + 1,203 out tokens)
  Profile Scraper  : $0.0600
  ─────────────────────────────────────────────────────────────────
  TOTAL            : $0.7622
═══════════════════════════════════════════════════════════════════
```

---

## How it works

### Stage 1 - Scrape comments (`scrape_comments`)

Calls the `clockworks/tiktok-comments-scraper` Actor with:

```python
{
    "postURLs": [video_url],
    "commentsPerPost": 1000,
    "maxRepliesPerComment": 0,
}
```

Uses `apify_client.ApifyClient.actor(...).call()` which blocks until the run finishes. Typically takes 1-3 minutes for 500 comments. Results are loaded via `iterate_items()` into a list - fine for the typical volume (under 1,000 comments).

### Stage 2 - Deduplicate (`deduplicate_comments`)

The same username can comment multiple times on a video. The script keeps one row per user - the one with the highest `diggCount` (likes). Ties break on longer comment text. All client-side; the Actor has no server-side dedup flag.

### Stage 3 - Rank with Cohere (`rank_prospects`)

Sends the deduped comments as a JSON array to **Cohere Command A** (`command-a-03-2025`) with:

- A system prompt describing the topic, target buyer, and ranking criteria.
- A `response_format` with a strict JSON schema - Cohere's constrained decoding guarantees the output is valid JSON matching the schema every time.
- Temperature `0.1` for consistent rankings.

Exponential backoff on transient errors (`TooManyRequestsError`, `ServiceUnavailableError`, `GatewayTimeoutError`, `InternalServerError`).

### Stage 4 - Enrich profiles (`enrich_profiles`)

Takes the shortlisted usernames the LLM returned and calls `clockworks/tiktok-profile-scraper`:

```python
{
    "profiles": ["username1", "username2", ...],
    "resultsPerPage": 1,   # minimize cost - we only need authorMeta
    "profileScrapeSections": ["videos"],
    "profileSorting": "latest",
}
```

Returns full profile metadata (`authorMeta.{name,fans,signature,bioLink,verified,ttSeller,privateAccount,profileUrl}`) for each user.

### Stage 5 - Join (`join_prospects`)

Matches each ranked prospect back to its original comment and its enriched profile, using lowercase username as the join key. Produces one `Prospect` dataclass per row.

### Stage 6 - Write output

Writes `prospects.csv` with stable column order. Prints the summary with the top 3 high-priority prospects and the full cost breakdown.

---

## Customization

### Use a different LLM

To swap Cohere for OpenAI, Anthropic, or another provider, replace the `rank_prospects` function. The signature is:

```python
def rank_prospects(
    client,                  # your LLM client
    comments: list[Comment],
    topic: str,
    target: str,
    shortlist_size: int,
) -> tuple[list[LLMRanking], int, int]:
    ...
```

Return `(rankings, input_tokens, output_tokens)`. Any modern LLM with structured JSON output works - OpenAI's `response_format`, Anthropic's `response_format` or tool use, Google's Gemini structured outputs. The rest of the pipeline is LLM-agnostic.

### Change the prompt

The system prompt lives in `_build_system_prompt()`. Rewrite the ranking signals, the "skip these" list, or the output schema to match your business.

### Scrape multiple videos at once

The Comments Scraper accepts an array of URLs. Change the `postURLs` list in `scrape_comments()` to pass multiple videos. Costs scale linearly.

### Schedule it

Wrap the script in a cron job, GitHub Actions workflow, or Apify Schedule:

```cron
0 9 * * MON  cd /path/to/tiktok-prospect-pipeline && .venv/bin/python pipeline.py --video-url "https://..." >> run.log 2>&1
```

---

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `Missing environment variables: APIFY_TOKEN` | Missing `.env` | Run `cp .env.example .env` and fill in your keys |
| `Actor ... ended with status FAILED` | TikTok blocked the request / invalid URL | Check the video URL is public and not age-restricted |
| `Cohere ranking failed after 5 attempts` | Trial key rate limit (20 RPM / 1,000 calls/month) | Wait a minute, or upgrade to a paid Cohere key |
| `No comments returned` | Video has no comments / URL malformed | Verify the video URL in a browser first |
| Most prospects have `followers=0` | `authorMeta.fans` missing - account may be private | Check `is_private=True` rows and filter them out |
| LLM returns fewer than `shortlist-size` prospects | Pool was genuinely small (a lot of noise) | Lower the threshold - any real prospect is still valuable |

---

## Costs in detail

Real numbers from a typical run:

```
Stage              Cost       Notes
─────────────────────────────────────────────────────────────────
Comments Scraper   $0.6350    508 comments × $1.25 / 1,000
Cohere Command A   $0.0672    ~22K input + ~1.2K output tokens
Profile Scraper    $0.0600    15 profiles × $4.00 / 1,000
─────────────────────────────────────────────────────────────────
Total              $0.7622    per complete run
```

The Apify free tier ($5/month) covers ~6 full runs before billing kicks in. The Cohere free tier (1,000 calls/month) covers 1,000 runs - the Apify side is the constraint, not Cohere.

---

## What this script does NOT do

- **It does not extract email addresses.** Use the Apify [Mass TikTok Email Scraper](https://apify.com/scraper-mind/tiktok-email-scraper) for that (paid, $5/month rental).
- **It does not send outreach.** It produces the prospect list; you write the message.
- **It does not auto-qualify private accounts.** The `is_private=True` flag tells you which ones to skip.
- **It is not a legal review.** Scraping TikTok data has real compliance implications (TikTok ToS, CAN-SPAM, GDPR). Read the "Legal and compliance considerations" section of the blog post before any outreach.

---

## License

MIT. Fork it, adapt it, use it however you want.
