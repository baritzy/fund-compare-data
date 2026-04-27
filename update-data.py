"""
Fund Compare — Daily Data Update Script (GitHub Actions version)
Runs via GitHub Actions cron at 03:00 UTC (06:00 Israel time)
Fetches: TASE Maya (Israeli funds) + Yahoo Finance (US ETFs) + Bizportal (3yr/5yr returns)
"""
import subprocess
import json
import os
import random
import re
import sys
import time
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# All paths relative to repo root (where this script lives)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, 'update-log.txt')

def log(msg):
    line = f"[{date.today()}] {msg}"
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

def run_cmd(cmd, timeout=120):
    """Run a shell command"""
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, shell=True)

def update_us_etfs():
    """Fetch US ETF data from Yahoo Finance"""
    import yfinance as yf

    ETFS = {
        'VOO': 'S&P 500', 'SPY': 'S&P 500', 'IVV': 'S&P 500', 'SPLG': 'S&P 500',
        'QQQ': 'NASDAQ 100', 'QQQM': 'NASDAQ 100',
        'DIA': 'Dow Jones Industrial Average',
        'VTI': 'Total US Stock Market', 'ITOT': 'Total US Stock Market', 'SPTM': 'Total US Stock Market',
        'RSP': 'S&P 500 Equal Weight', 'NOBL': 'S&P 500 Dividend Aristocrats',
        'SCHD': 'Dow Jones US Dividend 100', 'VIG': 'Dividend Appreciation',
        'SPYV': 'S&P 500 Value', 'SPYG': 'S&P 500 Growth', 'SPLV': 'S&P 500 Low Volatility',
        'IWM': 'Russell 2000', 'IWF': 'Russell 1000 Growth', 'IWD': 'Russell 1000 Value',
        'IWB': 'Russell 1000', 'VTWO': 'Russell 2000',
        'IJH': 'S&P MidCap 400', 'VO': 'CRSP US Mid Cap', 'MDY': 'S&P MidCap 400',
        'IJR': 'S&P SmallCap 600', 'VB': 'CRSP US Small Cap',
        'XLK': 'Technology Select Sector', 'VGT': 'Information Technology',
        'SMH': 'Semiconductor', 'SOXX': 'PHLX Semiconductor', 'ARKK': 'ARK Innovation',
        'XLV': 'Health Care Select Sector', 'VHT': 'Health Care', 'IBB': 'NASDAQ Biotechnology',
        'XLF': 'Financial Select Sector', 'VFH': 'Financials', 'KBE': 'S&P Banks',
        'XLE': 'Energy Select Sector', 'VDE': 'Energy',
        'VNQ': 'Real Estate', 'XLRE': 'Real Estate Select Sector', 'IYR': 'US Real Estate',
        'XLY': 'Consumer Discretionary', 'XLP': 'Consumer Staples',
        'XLI': 'Industrial Select Sector', 'VIS': 'Industrials',
        'XLU': 'Utilities Select Sector', 'XLC': 'Communication Services', 'XLB': 'Materials Select Sector',
        'VEA': 'FTSE Developed Markets', 'EFA': 'MSCI EAFE', 'IEFA': 'MSCI EAFE',
        'VGK': 'FTSE Europe', 'EWJ': 'MSCI Japan', 'EWG': 'MSCI Germany', 'EWU': 'MSCI United Kingdom',
        'VWO': 'FTSE Emerging Markets', 'EEM': 'MSCI Emerging Markets', 'IEMG': 'MSCI Emerging Markets',
        'VXUS': 'FTSE All-World ex-US', 'VT': 'FTSE Global All Cap', 'ACWI': 'MSCI ACWI',
        'MCHI': 'MSCI China', 'FXI': 'FTSE China 50', 'KWEB': 'China Internet', 'INDA': 'MSCI India',
        'BND': 'US Aggregate Bond', 'AGG': 'US Aggregate Bond',
        'TLT': 'Treasury 20+ Year', 'IEF': 'Treasury 7-10 Year', 'SHY': 'Treasury 1-3 Year',
        'GOVT': 'US Treasury Bond', 'TIP': 'TIPS Bond',
        'VGSH': 'Short-Term Treasury', 'VGIT': 'Intermediate-Term Treasury', 'VGLT': 'Long-Term Treasury',
        'LQD': 'Investment Grade Corporate', 'HYG': 'High Yield Corporate', 'JNK': 'High Yield Corporate',
        'VCIT': 'Intermediate-Term Corporate', 'VCSH': 'Short-Term Corporate', 'BNDX': 'International Bond',
        'GLD': 'Gold', 'IAU': 'Gold', 'SLV': 'Silver', 'USO': 'Crude Oil', 'DBC': 'Commodity Index',
        'IBIT': 'Bitcoin', 'BITO': 'Bitcoin Futures', 'ETHA': 'Ethereum',
        'ITA': 'US Aerospace & Defense',
        'ICLN': 'Global Clean Energy', 'TAN': 'Solar Energy', 'QCLN': 'Clean Edge Green Energy',
        'CIBR': 'Cybersecurity', 'HACK': 'Cybersecurity',
        'BOTZ': 'Robotics & AI', 'ROBO': 'Robotics & Automation',
        'SKYY': 'Cloud Computing', 'WCLD': 'Cloud Computing',
    }

    # Step 1: Fetch info for all tickers (with retry for rate limiting)
    results = []
    tickers_list = list(ETFS.keys())
    failed_tickers = []
    for ticker, index_name in ETFS.items():
        try:
            info = yf.Ticker(ticker).info
            if not info.get('longName'):
                failed_tickers.append((ticker, index_name))
                continue
            raw_er = info.get('netExpenseRatio') or 0
            mf = round(raw_er, 4)
            raw_3y = info.get('threeYearAverageReturn') or 0
            y3 = round(raw_3y * 100, 2) if abs(raw_3y) < 5 else round(raw_3y, 2)
            raw_5y = info.get('fiveYearAverageReturn') or 0
            y5 = round(raw_5y * 100, 2) if abs(raw_5y) < 5 else round(raw_5y, 2)
            # If Yahoo returns 0 for young ETFs, treat as no data
            if y3 == 0.0:
                y3 = None
            if y5 == 0.0:
                y5 = None
            y3c = round(((1 + y3/100)**3 - 1) * 100, 2) if y3 is not None else None
            y5c = round(((1 + y5/100)**5 - 1) * 100, 2) if y5 is not None else None
            results.append({
                'id': ticker, 'n': info.get('longName', ticker), 'tk': ticker,
                'mf': mf, 'yy': 0, 'l12': 0,  # will be filled by batch download in Step 3
                'y3': y3, 'y5': y5, 'y3c': y3c, 'y5c': y5c,
                'av': round((info.get('totalAssets') or 0) / 1e6, 0),
                'idx': index_name, 'mgr': info.get('fundFamily', ''),
                'cls': info.get('category', ''), 'src': 'yahoo'
            })
        except Exception as e:
            failed_tickers.append((ticker, index_name))
            log(f"  WARN: {ticker}: {e}")

    # Retry failed tickers once after a short delay
    if failed_tickers:
        log(f"  Retrying {len(failed_tickers)} failed tickers after 10s...")
        time.sleep(10)
        fetched_ids = {r['tk'] for r in results}
        for ticker, index_name in failed_tickers:
            if ticker in fetched_ids:
                continue
            try:
                info = yf.Ticker(ticker).info
                if not info.get('longName'):
                    continue
                raw_er = info.get('netExpenseRatio') or 0
                mf = round(raw_er, 4)
                raw_3y = info.get('threeYearAverageReturn') or 0
                y3 = round(raw_3y * 100, 2) if abs(raw_3y) < 5 else round(raw_3y, 2)
                raw_5y = info.get('fiveYearAverageReturn') or 0
                y5 = round(raw_5y * 100, 2) if abs(raw_5y) < 5 else round(raw_5y, 2)
                if y3 == 0.0:
                    y3 = None
                if y5 == 0.0:
                    y5 = None
                y3c = round(((1 + y3/100)**3 - 1) * 100, 2) if y3 is not None else None
                y5c = round(((1 + y5/100)**5 - 1) * 100, 2) if y5 is not None else None
                results.append({
                    'id': ticker, 'n': info.get('longName', ticker), 'tk': ticker,
                    'mf': mf, 'yy': 0, 'l12': 0,
                    'y3': y3, 'y5': y5, 'y3c': y3c, 'y5c': y5c,
                    'av': round((info.get('totalAssets') or 0) / 1e6, 0),
                    'idx': index_name, 'mgr': info.get('fundFamily', ''),
                    'cls': info.get('category', ''), 'src': 'yahoo'
                })
                log(f"  Retry OK: {ticker}")
            except Exception as e:
                log(f"  Retry FAIL: {ticker}: {e}")

    # Step 2: Fix YTD from price data (more reliable than ytdReturn field)
    try:
        year_start = date(date.today().year, 1, 1)
        ytd_data = yf.download(tickers_list, start=str(year_start), end=str(date.today()), progress=False)
        close = ytd_data['Close']
        for fund in results:
            tk = fund['tk']
            try:
                if tk in close.columns:
                    col = close[tk].dropna()
                    if len(col) >= 2:
                        fund['yy'] = round((float(col.iloc[-1]) / float(col.iloc[0]) - 1) * 100, 2)
            except:
                pass
    except Exception as e:
        log(f"  WARN: YTD batch fetch failed: {e}")

    # Step 3: Compute trailing 12-month return from price data
    try:
        l12_start = date.today() - timedelta(days=395)  # ~13 months to ensure coverage
        l12_data = yf.download(tickers_list, start=str(l12_start), end=str(date.today()), progress=False)
        l12_close = l12_data['Close']
        for fund in results:
            tk = fund['tk']
            try:
                if tk in l12_close.columns:
                    col = l12_close[tk].dropna()
                    if len(col) >= 2:
                        # Find the price closest to 12 months ago
                        target_date = date.today() - timedelta(days=365)
                        idx = col.index.get_indexer([str(target_date)], method='nearest')[0]
                        price_12m_ago = float(col.iloc[idx])
                        price_latest = float(col.iloc[-1])
                        if price_12m_ago > 0:
                            fund['l12'] = round((price_latest / price_12m_ago - 1) * 100, 2)
            except:
                pass
    except Exception as e:
        log(f"  WARN: 12-month batch fetch failed: {e}")

    if len(results) < 50:
        log(f"US ETFs: only {len(results)}, skipping")
        return False
    path = os.path.join(BASE_DIR, 'us-etfs.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({'updated': str(date.today()), 'source': 'Yahoo Finance', 'funds': results}, f, ensure_ascii=False, separators=(',', ':'))
    log(f"US ETFs: {len(results)} funds saved (with y3c/y5c)")
    return True

def update_tase_tracking():
    """Fetch Israeli tracking funds from TASE Maya via Playwright headless browser"""
    out_path = os.path.join(BASE_DIR, 'tracking-funds.json').replace('\\', '/')

    script = f"""
const {{ chromium }} = require('playwright');
(async () => {{
    const browser = await chromium.launch({{ headless: true }});
    const page = await browser.newPage();
    await page.goto('https://maya.tase.co.il/he/funds/mutual-funds', {{ waitUntil: 'networkidle', timeout: 60000 }});
    await page.waitForTimeout(8000);
    let allFunds = [];
    let pn = 1, more = true;
    while (more && pn <= 100) {{
        const resp = await page.evaluate(async (p) => {{
            const r = await fetch('/api/v1/funds/mutual', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify({{ pageSize: 20, pageNumber: p }}) }});
            if (!r.ok) return {{ error: r.status }};
            return await r.json();
        }}, pn);
        if (resp.error) {{ console.error('API error:', resp.error); break; }}
        allFunds = allFunds.concat(resp);
        if (resp.length < 20) more = false;
        pn++;
    }}
    const tracking = allFunds.filter(f => f.features && f.features.some(ft => ft.title && (ft.title.includes('Tracking fund') || ft.title.includes('קרן מחקה')) && ft.enabled));
    const slim = tracking.map(f => ({{ id: f.fundId, n: f.name, mf: f.managementFee, tf: f.trusteeFee, yy: f.yearYield, l12: f.last12MonthYield, sd: f.standardDeviation, av: f.assetValue, idx: f.underlyingAssetsText, mgr: f.managerName, cls: f.classification ? f.classification.major : null }}));
    if (slim.length < 100) {{ console.log('saved 0 tracking funds (only ' + slim.length + ', below threshold 100)'); await browser.close(); return; }}
    require('fs').writeFileSync('{out_path}', JSON.stringify({{ updated: new Date().toISOString().split('T')[0], source: 'TASE Maya', funds: slim }}), 'utf-8');
    console.log('saved ' + slim.length + ' tracking funds');
    await browser.close();
}})();
"""
    script_path = os.path.join(BASE_DIR, '_tase_fetch.js')
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(script)
    try:
        result = run_cmd(f'node "{script_path}"', timeout=180)
        if result.returncode == 0 and 'saved' in result.stdout and 'saved 0' not in result.stdout:
            log(f"TASE: {result.stdout.strip()}")
            return True
        else:
            log(f"TASE: failed — {(result.stderr or result.stdout)[:200]}")
            return False
    except Exception as e:
        log(f"TASE: error — {e}")
        return False
    finally:
        # Clean up temp JS file
        if os.path.exists(script_path):
            os.remove(script_path)

def enrich_from_bizportal():
    """Scrape Bizportal for per-fund 3yr/5yr cumulative returns, then calculate annualized"""
    import requests
    from bs4 import BeautifulSoup

    path = os.path.join(BASE_DIR, 'tracking-funds.json')
    if not os.path.exists(path):
        log("Bizportal: no tracking-funds.json to enrich")
        return False

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'}

    def parse_pct(s):
        if not s or s == '--':
            return None
        s = s.replace('%', '').replace(',', '').strip()
        try:
            return float(s)
        except:
            return None

    def cum_to_annual(cum_pct, years):
        if cum_pct is None:
            return None
        total = 1 + cum_pct / 100
        if total <= 0:
            return None
        return round((total ** (1/years) - 1) * 100, 2)

    def scrape_fund(fund_id):
        url = f'https://www.bizportal.co.il/mutualfunds/quote/performance/{fund_id}'
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                return fund_id, None, None, None, None
            soup = BeautifulSoup(resp.text, 'html.parser')
            tables = soup.find_all('table')
            if not tables:
                return fund_id, None, None, None, None
            result = {}
            for row in tables[0].find_all('tr'):
                cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                if len(cells) >= 2:
                    result[cells[0]] = cells[1]
            y3c = parse_pct(result.get('3 שנים'))
            y5c = parse_pct(result.get('5 שנים'))
            y3 = cum_to_annual(y3c, 3)
            y5 = cum_to_annual(y5c, 5)
            return fund_id, y3, y5, y3c, y5c
        except:
            return fund_id, None, None, None, None

    fund_ids = [f['id'] for f in data['funds'] if f.get('id')]
    log(f"Bizportal: scraping {len(fund_ids)} funds...")

    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_fund, fid): fid for fid in fund_ids}
        for future in as_completed(futures):
            fid, y3, y5, y3c, y5c = future.result()
            results[fid] = (y3, y5, y3c, y5c)

    updated = 0
    for f in data['funds']:
        fid = f.get('id')
        if fid and fid in results:
            y3, y5, y3c, y5c = results[fid]
            if y3 is not None:
                f['y3'] = y3
                f['y3c'] = y3c
            if y5 is not None:
                f['y5'] = y5
                f['y5c'] = y5c
                updated += 1

    with open(path, 'w', encoding='utf-8') as f2:
        json.dump(data, f2, ensure_ascii=False, separators=(',', ':'))
    log(f"Bizportal: enriched {updated} funds with y3/y5/y3c/y5c")
    return True


def enrich_from_maya_detail():
    """Fetch Maya per-fund detail endpoint to enrich each fund with variableFee (vf),
    saleLoad (sl), addedValueFee (avf), and refresh managementFee (mf) / trusteeFee (tf)
    from the detail record. Fail-soft: keep prior values when a fetch returns None."""
    import requests

    path = os.path.join(BASE_DIR, 'tracking-funds.json')
    if not os.path.exists(path):
        log("Maya detail: no tracking-funds.json to enrich")
        return False

    # Load existing data BEFORE enriching so we can preserve prior vf/sl/avf
    # if today fetch returns None (fail-soft against Maya 403/rate-limit).
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0',
        'Referer': 'https://maya.tase.co.il/funds/main',
        'Accept': 'application/json',
    }

    # Track consecutive 403/429 responses to detect rate-limit / IP block
    rate_limit_state = {'consecutive_blocks': 0, 'aborted': False, 'lock': __import__('threading').Lock()}
    RATE_LIMIT_ABORT_THRESHOLD = 50

    def fetch_detail(fund_id):
        # Abort early if rate-limit threshold tripped
        if rate_limit_state['aborted']:
            return fund_id, None, 'aborted'
        # Jitter to avoid hammering Maya
        time.sleep(random.uniform(0.05, 0.20))
        url = f'https://maya.tase.co.il/api/v1/funds/mutual/{fund_id}'
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code in (403, 429):
                with rate_limit_state['lock']:
                    rate_limit_state['consecutive_blocks'] += 1
                    if rate_limit_state['consecutive_blocks'] >= RATE_LIMIT_ABORT_THRESHOLD:
                        rate_limit_state['aborted'] = True
                return fund_id, None, 'blocked'
            if resp.status_code != 200:
                return fund_id, None, 'http-error'
            # Reset consecutive-block counter on success
            with rate_limit_state['lock']:
                rate_limit_state['consecutive_blocks'] = 0
            return fund_id, resp.json(), 'ok'
        except Exception:
            return fund_id, None, 'exception'

    # Optional sample limit for local testing
    limit_env = os.environ.get('LIMIT')
    fund_ids = [f['id'] for f in data['funds'] if f.get('id')]
    if limit_env:
        try:
            n = int(limit_env)
            fund_ids = fund_ids[:n]
            log(f"Maya detail: LIMIT={n} (sample mode)")
        except ValueError:
            pass
    total = len(fund_ids)
    log(f"Maya detail: fetching {total} funds...")

    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_detail, fid): fid for fid in fund_ids}
        for future in as_completed(futures):
            fid, detail, status = future.result()
            if detail is not None:
                results[fid] = detail

    if rate_limit_state['aborted']:
        kept_count = sum(1 for f in data['funds'] if f.get('id') in set(fund_ids) and f.get('vf') is not None)
        log(f"Maya rate-limit detected — aborting enrichment, kept {kept_count} existing values")

    # Counters for new vs kept-from-previous vs still-missing per field set (vf primary)
    new_count = 0
    kept_count = 0
    missing_count = 0
    enriched_sl = 0
    enriched_avf = 0
    refreshed_mf = 0
    refreshed_tf = 0
    fund_id_set = set(fund_ids)
    for f in data['funds']:
        fid = f.get('id')
        if not fid or fid not in fund_id_set:
            continue
        d = results.get(fid)
        had_vf = f.get('vf') is not None
        # variableFee -> vf  (fail-soft)
        new_vf_value = None
        if d is not None:
            vf_raw = d.get('variableFee')
            if vf_raw is not None:
                try:
                    new_vf_value = round(float(vf_raw), 4)
                except (TypeError, ValueError):
                    new_vf_value = None
        if new_vf_value is not None:
            f['vf'] = new_vf_value
            new_count += 1
        elif had_vf:
            kept_count += 1
        else:
            missing_count += 1

        # saleLoad -> sl (fail-soft)
        new_sl_value = None
        if d is not None:
            sl_raw = d.get('saleLoad')
            if sl_raw is not None:
                try:
                    new_sl_value = round(float(sl_raw), 4)
                except (TypeError, ValueError):
                    new_sl_value = None
        if new_sl_value is not None:
            f['sl'] = new_sl_value
            enriched_sl += 1
        # else: keep existing f['sl'] if present (fail-soft, no-op)

        # addedValueFee -> avf (fail-soft, NEW field per Patch 2)
        new_avf_value = None
        if d is not None:
            avf_raw = d.get('addedValueFee')
            if avf_raw is not None:
                try:
                    new_avf_value = round(float(avf_raw), 4)
                except (TypeError, ValueError):
                    new_avf_value = None
        if new_avf_value is not None:
            f['avf'] = new_avf_value
            enriched_avf += 1
        # else: keep existing f['avf'] if present (fail-soft, no-op)

        # Refresh managementFee/trusteeFee from detail (overwrite when present)
        if d is not None:
            mf = d.get('managementFee')
            if mf is not None:
                try:
                    f['mf'] = round(float(mf), 4)
                    refreshed_mf += 1
                except (TypeError, ValueError):
                    pass
            tf = d.get('trusteeFee')
            if tf is not None:
                try:
                    f['tf'] = round(float(tf), 4)
                    refreshed_tf += 1
                except (TypeError, ValueError):
                    pass

    with open(path, 'w', encoding='utf-8') as f2:
        json.dump(data, f2, ensure_ascii=False, separators=(',', ':'))
    log(f"Maya detail: {new_count} new + {kept_count} kept-from-previous + "
        f"{missing_count} still-missing (out of {total})")
    log(f"Maya detail: sl={enriched_sl}, avf={enriched_avf}, "
        f"mf-refresh={refreshed_mf}, tf-refresh={refreshed_tf}")
    return True


def enrich_inception_dates():
    """Fetch Bizportal generalview page per fund and parse the inception date
    (תאריך הקמה) into ISO YYYY-MM-DD, stored as 'inc'."""
    import requests

    path = os.path.join(BASE_DIR, 'tracking-funds.json')
    if not os.path.exists(path):
        log("Bizportal generalview: no tracking-funds.json to enrich")
        return False

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'}
    inception_re = re.compile(
        r'<dt>\s*תאריך הקמה\s*</dt>\s*<dd>\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\s*</dd>'
    )

    def fetch_inception(fund_id):
        # Per-host delay to avoid hammering Bizportal (shared with enrich_from_bizportal)
        time.sleep(random.uniform(0.10, 0.20))
        url = f'https://www.bizportal.co.il/mutualfunds/quote/generalview/{fund_id}'
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                return fund_id, None
            m = inception_re.search(resp.text)
            if not m:
                return fund_id, None
            try:
                d = datetime.strptime(m.group(1), '%d/%m/%Y').date()
                return fund_id, d.isoformat()
            except ValueError:
                return fund_id, None
        except Exception:
            return fund_id, None

    # Optional sample limit for local testing
    limit_env = os.environ.get('LIMIT')
    fund_ids = [f['id'] for f in data['funds'] if f.get('id')]
    if limit_env:
        try:
            n = int(limit_env)
            fund_ids = fund_ids[:n]
            log(f"Bizportal generalview: LIMIT={n} (sample mode)")
        except ValueError:
            pass
    total = len(fund_ids)
    log(f"Bizportal generalview: fetching inception for {total} funds...")

    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_inception, fid): fid for fid in fund_ids}
        for future in as_completed(futures):
            fid, inc = future.result()
            if inc:
                results[fid] = inc

    enriched = 0
    for f in data['funds']:
        fid = f.get('id')
        if fid and fid in results:
            f['inc'] = results[fid]
            enriched += 1

    with open(path, 'w', encoding='utf-8') as f2:
        json.dump(data, f2, ensure_ascii=False, separators=(',', ':'))
    log(f"Bizportal generalview: enriched {enriched}/{total} with inception")
    return True



if __name__ == '__main__':
    log("=== Starting daily update ===")
    us_ok = update_us_etfs()
    tase_ok = update_tase_tracking()
    if tase_ok:
        enrich_from_bizportal()
        enrich_from_maya_detail()
        enrich_inception_dates()
    if not us_ok and not tase_ok:
        log("No data updated")
        sys.exit(1)
    log("=== Done ===")
