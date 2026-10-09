# Nifty 50 Predictive Analytics Terminal (live)

Academic project, GIM PGDM-BIFS. A single-screen market terminal for the Nifty 50:

- **Live market data:** Upstox API, read-only Analytics Token stored as a server environment variable (never sent to the browser).
- **End-of-day data:** NSE archives (delivery %, participant open interest).
- **News:** Google News RSS with live sentiment scores. The research pipeline scores 6.9 lakh headlines with FinBERT.
- **Prediction panels:** "model in training" until the model is validated.

**Render settings**
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Environment variables: `UPSTOX_ANALYTICS_TOKEN` and `ACCESS_CODE`

Not investment advice. Market data is shown for academic demonstration only and is not for redistribution.
