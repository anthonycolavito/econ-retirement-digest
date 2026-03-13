# Retirement Research Digest

A public web app that aggregates and displays recent economics research relevant to retirement researchers. Scans working papers and journal articles from OpenAlex and NBER, filters for relevance via concept tagging and keywords, and displays them in a searchable dashboard.

**Live site**: [anthonycolavito.github.io/econ-retirement-digest](https://anthonycolavito.github.io/econ-retirement-digest)

## How It Works

- **Data sources**: [OpenAlex API](https://openalex.org/) (concept-filtered + journal-filtered queries) and [NBER RSS](https://www.nber.org/) feed
- **Classification**: Papers are tagged into 7 topic categories (Public Pensions, Consumption-Savings, Social Insurance, Retirement Decisions, Longevity/Mortality, Private Pensions, Insurance Markets)
- **Updates**: GitHub Actions runs daily at 6 AM ET to fetch new papers
- **Frontend**: Static single-page app hosted on GitHub Pages — no server needed

## Local Development

```bash
# Fetch papers
pip install -r scripts/requirements.txt
python scripts/fetch_papers.py

# Dry run (no output file)
python scripts/fetch_papers.py --dry-run

# Serve locally
cd docs && python -m http.server 8000
```

## Project Structure

```
scripts/fetch_papers.py     # Paper fetcher (OpenAlex + NBER RSS)
docs/                       # GitHub Pages root
  index.html                # Single-page app
  css/style.css             # Styles
  js/app.js                 # Filter + render logic
  data/papers.json          # Generated paper data
.github/workflows/          # Daily update automation
```
