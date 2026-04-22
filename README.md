# Pro Truck Logistics Blog Generator

Automated blog generation pipeline for **protrucklogistics.org**.

The workflow runs on GitHub Actions, discovers trucking topics from live industry sources, uses OpenAI for topic framing, metadata, article writing, and image generation, rebuilds the blog index and SEO feeds, uploads artifacts to hosting over FTP, and commits generated content back to the repository so the repo stays in sync with production.

## Current defaults

- Text model: `gpt-5.4-mini`
- Topic model: `gpt-5.4-mini`
- Image model: `gpt-image-1.5`
- Image quality: `medium`
- Posts per run: `3`
- Upload mode: FTP (`FTP_IS_SFTP=false`)

## How it works

1. GitHub Actions runs on schedule or manually.
2. `generate_blogs.py` scrapes current trucking/logistics sources for topic context.
3. OpenAI generates topic framing, SEO metadata, article HTML, and a cover image prompt.
4. OpenAI image generation creates a cover image for each post.
5. Generated artifacts are written locally:
   - `blog-posts/<post-id>.json`
   - `blog-posts/post-<post-id>.html`
   - `blog-posts/images/<post-id>-image.png`
   - `blog-posts/index.json`
   - `rss.xml`
   - `sitemap.xml`
6. Files are uploaded to the hosting account.
7. Generated content is committed back to the repository.

## Required GitHub secrets

Set these in **Repository → Settings → Secrets and variables → Actions**.

- `OPENAI_API_KEY`
- `FTP_HOST`
- `FTP_USER`
- `FTP_PASS`

## Optional GitHub variables

- `OPENAI_BASE_URL` default: `https://api.openai.com/v1`
- `OPENAI_TEXT_MODEL` default: `gpt-5.4-mini`
- `OPENAI_TOPIC_MODEL` default: `gpt-5.4-mini`
- `OPENAI_IMAGE_MODEL` default: `gpt-image-1.5`
- `OPENAI_IMAGE_SIZE` default: `1024x1024`
- `OPENAI_IMAGE_QUALITY` default: `medium`
- `REQUIRE_IMAGE_GENERATION` default: `true`
- `POSTS_TO_GENERATE` default: `3`
- `REQUEST_TIMEOUT_SECONDS` default: `30`
- `SITE_BASE_URL` default: `https://protrucklogistics.org`
- `BLOG_TEMPLATE_PATH` default: `./blog-post-template.html`
- `BLOG_INCLUDES_HREF` default: `../includes.html`
- `SKIP_UPLOAD` default: `false`
- `FTP_IS_SFTP` default: `false`
- `FTP_BLOG_DIR` default: `/blog-posts/`
- `FTP_SITE_ROOT_DIR` default: parent of `FTP_BLOG_DIR`
- `SFTP_STRICT_HOST_KEY` optional for SFTP-only setups
- `SFTP_KNOWN_HOSTS` optional for SFTP-only setups

## Important behavior notes

- The repository is the source of truth. Generated files are committed back after each run.
- The workflow uploads only the newly generated post artifacts plus `index.json`, `rss.xml`, and `sitemap.xml`.
- If image generation fails and `REQUIRE_IMAGE_GENERATION=true`, the workflow fails.
- Topic discovery still uses live-source scraping so posts are grounded in current industry coverage without paying for OpenAI web-search tool calls.

## Local run

```bash
python -m pip install --upgrade pip
pip install openai requests beautifulsoup4 html2text pillow paramiko

export OPENAI_API_KEY="your_openai_key"
export SKIP_UPLOAD=true
python generate_blogs.py
```

## Troubleshooting

### Workflow fails before generation

- Confirm `OPENAI_API_KEY` is present and valid.
- Confirm model variables are set to valid OpenAI model IDs if you overrode the defaults.
- Confirm the template path exists.

### Upload fails

- Verify `FTP_HOST`, `FTP_USER`, and `FTP_PASS`.
- Verify `FTP_BLOG_DIR` is correct for your hosting layout.
- Confirm `FTP_IS_SFTP=false` if you want plain FTP.

### New posts do not appear on site

- Verify the workflow uploaded `blog-posts/index.json`.
- Verify it uploaded the new `post-<id>.html` and matching image.
- Verify `rss.xml` and `sitemap.xml` were uploaded to the site root.
- Check the browser console and clear cache.

## License

This project is intended for Pro Truck Logistics internal use.
