# Pro Truck Logistics Blog Generator

Automated blog generation pipeline for **protrucklogistics.org**.

The workflow runs on GitHub Actions, generates trucking/logistics blog posts with ecomagent (text) plus OpenAI image generation, builds static HTML pages, updates `blog-posts/index.json`, and uploads files to your hosting account.

## How It Works

1. GitHub Actions runs on schedule (or manually via `workflow_dispatch`).
2. `generate_blogs.py` fetches topic context from trucking sources.
3. The script uses ecomagent chat completions to generate:
   - topic framing
   - SEO description/keywords
   - article body HTML fragment
4. The script generates a required cover image using OpenAI image API.
5. Generated artifacts are written to `blog-posts/`:
   - `<post-id>.json`
   - `post-<post-id>.html`
   - `images/<post-id>-image.png`
   - `index.json`
6. Files are uploaded to your server via SFTP/FTP.

## Repository Structure

```text
ProTruckLogisticsBlogPostBot/
├── .github/workflows/
│   └── blog-generator.yml
├── blog-posts/
├── blog-post-template.html
├── generate_blogs.py
└── README.md
```

## Required GitHub Secrets

Set these in **Repository → Settings → Secrets and variables → Actions**.

### Required

- `ECOMAGENT_API_KEY`
- `OPENAI_API_KEY`
- `FTP_HOST`
- `FTP_USER`
- `FTP_PASS`

### Optional (recommended as Variables)

- `ECOMAGENT_BASE_URL` (default: `https://api.ecomagent.in/v1`)
- `ECOMAGENT_MODEL` (default: `claude-opus-4.6`)
- `ECOMAGENT_TOPIC_MODEL` (defaults to `ECOMAGENT_MODEL`)
- `OPENAI_BASE_URL` (default: `https://api.openai.com/v1`)
- `OPENAI_IMAGE_MODEL` (default: `gpt-image-1`)
- `OPENAI_IMAGE_SIZE` (default: `1024x1024`)
- `REQUIRE_IMAGE_GENERATION` (default: `true`)
- `POSTS_TO_GENERATE` (default `1`)
- `SITE_BASE_URL` (default `https://protrucklogistics.org`)
- `BLOG_TEMPLATE_PATH` (default `./blog-post-template.html`)
- `BLOG_INCLUDES_HREF` (default `../includes.html`)
- `SKIP_UPLOAD` (`true` for dry-runs)
- `FTP_IS_SFTP` (`true` recommended)
- `FTP_BLOG_DIR` (default `/blog-posts/`)
- `SFTP_STRICT_HOST_KEY` (`true` to enforce known-host checking)
- `SFTP_KNOWN_HOSTS` (path on runner, if strict host key checking is enabled)

## Important Behavior Notes

- **Upload-only workflow:** the Action does not commit generated files back to the repo.
- **Strict image requirement:** if OpenAI image generation fails and `REQUIRE_IMAGE_GENERATION=true`, the workflow fails.
- **Canonical domain:** generated metadata/schema/share links are based on `https://protrucklogistics.org`.

## Local Run (optional)

```bash
python -m pip install --upgrade pip
pip install openai requests beautifulsoup4 html2text pillow paramiko

export ECOMAGENT_API_KEY="your_ecomagent_key"
export OPENAI_API_KEY="your_openai_key"
export SKIP_UPLOAD=true
python generate_blogs.py
```

## Troubleshooting

### Workflow fails before generation

- Confirm `ECOMAGENT_API_KEY` is present and valid.
- Confirm `OPENAI_API_KEY` is present when image generation is required.
- Confirm model/base URL vars are valid.

### Upload fails

- Verify `FTP_HOST`, `FTP_USER`, `FTP_PASS` secrets.
- Verify `FTP_BLOG_DIR` path exists or can be created.
- Prefer `FTP_IS_SFTP=true`.

### No new post visible on site

- Verify `blog-posts/index.json` was uploaded.
- Verify corresponding `post-<id>.html` and image files were uploaded.
- Check browser cache and console errors.

## License

This project is intended for Pro Truck Logistics internal use.