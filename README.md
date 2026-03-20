# Car Price Tracker

Local Python + HTML tracker for vehicle market pricing trends using MarketCheck.

## Features

- Dynamic `vehicle year -> make -> model -> trim` selectors from the active provider.
- Monthly/yearly price trend toggle.
- Multiple comparison rows.
- Depreciation comparison chart.
- Local caching to reduce API calls and avoid rate-limit issues.

## Run

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000).

## Provider

The app now uses MarketCheck as the primary provider.

- Selectors come from MarketCheck taxonomy endpoints.
- Trend charts are built from MarketCheck listing history plus current market stats.
- The chart data is market/listing-oriented, not a public nationwide confirmed-sale ledger.

The app loads credentials from environment variables and from a local `.env` file if present.

Required:

```powershell
$env:MARKETCHECK_API_KEY="your-key"
```

Optional:

```powershell
$env:MARKETCHECK_API_SECRET="your-secret"
$env:MARKETCHECK_BASE_URL="https://api.marketcheck.com"
```

## Notes

- MarketCheck rate limits are real, so the app caches active listing searches, VIN history samples, and market stats in `cache/`.
- If a trim is too sparse, the app may show only a few monthly points. That is a source coverage issue, not a UI issue.
