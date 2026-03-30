import pandas as pd
import yfinance as yf
import requests
import io
import time
import logging
import os
import smtplib
from datetime import datetime
from curl_cffi import requests as curl_requests
from email.message import EmailMessage
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Global Session for Browser Impersonation ───────────────────────────────────
# Using curl_cffi to mimic a real Chrome browser fingerprint.
SESSION = curl_requests.Session(impersonate="chrome")

# ── Config ─────────────────────────────────────────────────────────────────────
JPX_URL = (
    "https://www.jpx.co.jp/english/markets/statistics-equities/misc/"
    "tvdivq0000001vg2-att/data_e.xls"
)

START_DATE = "2024-01-01"

# LIMIT: Set to a small number (e.g., 20) for testing; None for full market (~4400)
LIMIT = None 

# ── Step 1: fetch the JPX master list ─────────────────────────────────────────
def get_jpx_tickers() -> list[tuple[str, str, str]]:
    """Return [(yf_ticker, name, sector), …] for all TSE-listed equities."""
    log.info("Downloading JPX stock list …")
    try:
        resp = requests.get(JPX_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))

        code_col   = next((c for c in df.columns if "Local Code"      in str(c)), "Code")
        name_col   = next((c for c in df.columns if "Name (English)"  in str(c)), "Name")
        sector_col = next((c for c in df.columns if "33 Sector(name)" in str(c)), "Sector")

        tickers = df[code_col].astype(str).str.strip()
        names   = df[name_col].tolist()
        sectors = df[sector_col].tolist()

        yf_tickers = [f"{t}.T" if len(t) >= 4 else t for t in tickers]
        return list(zip(yf_tickers, names, sectors))
    except Exception as exc:
        log.error("Failed to fetch JPX list: %s", exc)
        return []

# ── Step 2: Sequential Analysis ───────────────────────────────────────────────
def analyze_market(ticker_info: list[tuple[str, str, str]]):
    """
    Processes each stock sequentially to calculate returns since 2024-01-01.
    """
    hits = []
    total = len(ticker_info)
    
    log.info("Starting sequential analysis of %d stocks...", total)
    
    for i, (ticker, name, sector) in enumerate(ticker_info, 1):
        log.info("Checking %d/%d: %s (%s)", i, total, ticker, name)
        attempts = 0
        max_attempts = 3
        
        while attempts < max_attempts:
            try:
                t_obj = yf.Ticker(ticker, session=SESSION)
                hist = t_obj.history(start=START_DATE, interval="1d")
                
                if hist.empty:
                    attempts += 1
                    if attempts < max_attempts:
                        wait = attempts * 60
                        log.warning("Empty data/Limit for %s. Waiting %ds (Attempt %d/%d)...", ticker, wait, attempts, max_attempts)
                        time.sleep(wait)
                        continue
                    else:
                        break

                if len(hist) < 2:
                    log.debug("Insufficient data for %s", ticker)
                    break
                
                price_start = float(hist["Close"].iloc[0])
                price_end = float(hist["Close"].iloc[-1])
                
                if price_start > 0:
                    return_pct = ((price_end - price_start) / price_start) * 100
                    
                    # Calculate CAGR
                    # hist.index[0] is the date of the first price in 2024
                    # hist.index[-1] is the date of the latest price
                    days = (hist.index[-1] - hist.index[0]).days
                    years = days / 365.25
                    if years > 0:
                        cagr = (pow(price_end / price_start, 1 / years) - 1) * 100
                    else:
                        cagr = return_pct # Fallback if same day
                        
                    hits.append({
                        "Ticker": ticker, 
                        "Name": name, 
                        "Sector": sector,
                        "First Trading Date": hist.index[0].strftime('%Y-%m-%d'),
                        "Start Price (2024)": round(price_start, 2),
                        "Latest Price": round(price_end, 2),
                        "Return %": round(return_pct, 2),
                        "CAGR %": round(cagr, 2)
                    })
                
                break # Success!
                
            except Exception as e:
                if "Rate Limit" in str(e) or "429" in str(e):
                    attempts += 1
                    wait = attempts * 60
                    log.warning("Rate limit hit for %s. Waiting %ds (Attempt %d/%d)...", ticker, wait, attempts, max_attempts)
                    time.sleep(wait)
                else:
                    log.error("Error processing %s: %s", ticker, e)
                    break
    
    return hits

def send_email(file_path, total_hits):
    """Sends the result CSV via email."""
    sender = os.environ.get("EMAIL_SENDER")
    password = os.environ.get("EMAIL_PASSWORD")
    receiver = os.environ.get("EMAIL_RECEIVER")
    
    if not all([sender, password, receiver]):
        log.warning("Email credentials missing. Skipping email.")
        return

    msg = EmailMessage()
    msg['Subject'] = f"Daily Japan Stock Report: Top 10 Returns per Sector"
    msg['From'] = sender
    msg['To'] = receiver
    msg.set_content(f"Found {total_hits} stocks in total for the top 10 performers in each sector since {START_DATE}.\n\nPlease find the attached CSV for details.")

    with open(file_path, 'rb') as f:
        file_data = f.read()
        msg.add_attachment(file_data, maintype='application', subtype='csv', filename=file_path)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)
    log.info("Email sent successfully to %s", receiver)

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    t0 = time.time()
    
    ticker_info = get_jpx_tickers()
    if not ticker_info:
        log.error("No tickers retrieved – exiting.")
    else:
        if LIMIT:
            log.info("LIMIT=%d – testing first %d stocks only.", LIMIT, LIMIT)
            ticker_info = ticker_info[:LIMIT]
        
        results = analyze_market(ticker_info)
        
        if results:
            df_out = pd.DataFrame(results)
            
            # Filter top 10 in each sector
            df_top10 = (
                df_out.sort_values(["Sector", "Return %"], ascending=[True, False])
                .groupby("Sector")
                .head(10)
            )
            
            fname = f"highest_returns_since_2024_{datetime.now().strftime('%Y%m%d')}.csv"
            df_top10.to_csv(fname, index=False, encoding="utf-8-sig")
            
            # Send Email
            send_email(fname, len(df_top10))
            log.info("Results saved and emailed. Total matches: %d", len(df_top10))
        else:
            log.info("No data collected.")

    log.info("Total execution time: %.1f seconds", time.time() - t0)
