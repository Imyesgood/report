"""
data_parser.py  —  엑셀 파싱 → data.json 생성
"""

import openpyxl
import json
import os
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

# ============================================================
# 인덱스 설정 — 헤더/열 이름 바뀌면 여기만 수정
# ============================================================
INDEX_CONFIG = [
    # ── LEFT: 국내채권 ───────────────────────────────────────
    {"label":"통안 2Y",      "section":"left",  "type":"rate",
     "sheet":"국내채권", "header":"통안 2Y",           "value_col":"민평3사 수익률(산출일) 당일"},
    {"label":"국고 3Y",      "section":"left",  "type":"rate",
     "sheet":"국내채권", "header":"국고 3Y",            "value_col":"민평3사 수익률(산출일) 당일"},
    {"label":"국고 5Y",      "section":"left",  "type":"rate",
     "sheet":"국내채권", "header":"국고 5Y",            "value_col":"민평3사 수익률(산출일) 당일"},
    {"label":"국고 10Y",     "section":"left",  "type":"rate",
     "sheet":"국내채권", "header":"국고 10Y",           "value_col":"민평3사 수익률(산출일) 당일"},
    {"label":"국채3년선물",  "section":"left",  "type":"equity",
     "sheet":"국내채권", "header":"3년국채 연결",        "value_col":"현재가"},

    # ── LEFT: 환율 ──────────────────────────────────────────
    {"label":"USD/KRW",      "section":"left",  "type":"fx",
     "sheet":"환율", "header":"서울외환(기업용) USDKRW 스팟 (~15:30)", "value_col":"현재가"},
    {"label":"NDF",          "section":"left",  "type":"fx",
     "sheet":"환율", "header":"NDF 뉴욕 NDF 뉴욕",     "value_col":"NDF_MID_Close"},
    {"label":"Dollar Index", "section":"left",  "type":"fx",
     "sheet":"환율", "header":"달러인덱스 DOLLARS",     "value_col":"KR_MID_Close"},
    {"label":"USD/JPY",      "section":"left",  "type":"fx",
     "sheet":"환율", "header":"서울외환 이종통화 USDJPY","value_col":"Close"},
    {"label":"EUR/USD",      "section":"left",  "type":"fx",
     "sheet":"환율", "header":"서울외환 이종통화 EURUSD","value_col":"Close"},
    {"label":"JPY/KRW",      "section":"left",  "type":"fx",
     "sheet":"환율", "header":"서울외환 이종통화 JPYKRW","value_col":"Close"},
    {"label":"USD/CNY",      "section":"left",  "type":"fx",
     "sheet":"환율", "header":"중국:USDCNY:뉴욕종가",   "value_col":"현재가"},
    {"label":"GBP/USD",      "section":"left",  "type":"fx",
     "sheet":"환율", "header":"영국:GBPUSD:뉴욕종가",   "value_col":"현재가"},

    # ── RIGHT: 주가 ─────────────────────────────────────────
    {"label":"KOSPI",        "section":"right", "type":"equity",
     "sheet":"주가지수", "header":"KOSPI",              "value_col":"현재가"},
    {"label":"NIKKEI",       "section":"right", "type":"equity",
     "sheet":"주가지수", "header":"니케이 225",          "value_col":"현재가"},
    {"label":"중국상해종합",  "section":"right", "type":"equity",
     "sheet":"지수",    "header":"중국:상하이종합지수",   "value_col":"현재가"},
    {"label":"DOW",          "section":"right", "type":"equity",
     "sheet":"주가지수", "header":"다우",                "value_col":"현재가"},
    {"label":"S&P500",       "section":"right", "type":"equity",
     "sheet":"주가지수", "header":"S&P 500",             "value_col":"현재가"},
    {"label":"NASDAQ",       "section":"right", "type":"equity",
     "sheet":"주가지수", "header":"나스닥",              "value_col":"현재가"},

    # ── RIGHT: 해외채권 ─────────────────────────────────────
    {"label":"T-Note (2yr)", "section":"right", "type":"rate",
     "sheet":"해외채권", "header":"2년 T-NOTE",          "value_col":"현재가"},
    {"label":"T-Note (10yr)","section":"right", "type":"rate",
     "sheet":"해외채권", "header":"10년 T-NOTE",         "value_col":"현재가"},
    {"label":"T-Bill (30yr)","section":"right", "type":"rate",
     "sheet":"해외채권", "header":"30년 T-BOND",         "value_col":"현재가"},

    # ── LEFT: 원자재·기타 ────────────────────────────────────
    {"label":"WTI",          "section":"left",  "type":"commodity",
     "sheet":"원자재",  "header":"WTI 현물",             "value_col":"현재가"},
    {"label":"GOLD",         "section":"left",  "type":"commodity",
     "sheet":"__pending__",  "header":"",                "value_col":""},
    {"label":"SOFR",         "section":"left",  "type":"rate",
     "sheet":"외환",    "header":"미국:SOFR:90일평균",   "value_col":"현재가"},
    {"label":"TED spread",   "section":"left",  "type":"spread",
     "sheet":"지수",    "header":"미국:TED스프레드",     "value_col":"현재가"},

# ============================================================
# 차트용 시계열 설정 (1년치 데이터 → chart_series)
# 독일 10년채 헤더명은 인포맥스 시트 확인 후 수정하세요
# ============================================================
CHART_CONFIG = [
    {"label": "Korea 10Y",   "color": "#14422e", "dash": "solid",
     "sheet": "국내채권", "header": "국고 10Y",    "value_col": "민평3사 수익률(산출일) 당일"},
    {"label": "US 10Y",      "color": "#1d4ed8", "dash": "dashed",
     "sheet": "해외채권", "header": "10년 T-NOTE", "value_col": "현재가"},
    {"label": "Germany 10Y", "color": "#b45309", "dash": "dotted",
     "sheet": "해외채권", "header": "독일 10년 분트", "value_col": "현재가"},
]

]


# ============================================================
# YTM 기준: 해당 연도 첫 거래일 탐색
# ============================================================
def first_trading_day_of_year(series: dict, year: int):
    """series에서 해당 연도 첫 거래일(가장 이른 날짜) 반환"""
    candidates = [d for d in series if d.year == year]
    return min(candidates) if candidates else None


# ============================================================
# 핵심 수정: 헤더 컬럼에서 '전방' 탐색으로 일자·값 컬럼 찾기
# ============================================================
def find_columns(ws, header_text, value_col_text):
    """
    row1에서 header_text 첫 번째 등장 컬럼(header_col) 찾기
    → row2에서 header_col 위치부터 '전방' 탐색으로 '일자' 컬럼 찾기
    → 그 이후에서 value_col_text 찾기
    반환: (date_col, val_col) 1-based, 없으면 (None, None)
    """
    # 1) row1에서 헤더 탐색 (첫 번째 등장)
    header_col = None
    for cell in ws[1]:
        if cell.value is not None and str(cell.value).strip() == str(header_text).strip():
            header_col = cell.column
            break
    if header_col is None:
        return None, None

    # 2) row2에서 header_col 기준 ±2 이내에서 '일자' 찾기 (앞뒤 2칸)
    date_col = None
    for offset in [0, 1, -1, 2, -2]:
        col = header_col + offset
        if col < 1:
            continue
        cell_val = ws.cell(row=2, column=col).value
        if cell_val == '일자':
            date_col = col
            break

    if date_col is None:
        return None, None

    # 3) date_col 이후 최대 15칸에서 value_col 찾기
    val_col = None
    for col in range(date_col + 1, date_col + 16):
        cell_val = ws.cell(row=2, column=col).value
        if cell_val is not None and str(cell_val).strip() == str(value_col_text).strip():
            val_col = col
            break

    return date_col, val_col


def read_series(ws, date_col, val_col):
    """date_col, val_col 기준 {date: float} dict 반환. 5행 연속 None이면 중단."""
    series = {}
    null_streak = 0
    for row in ws.iter_rows(min_row=3, values_only=True):
        raw_date = row[date_col - 1]
        raw_val  = row[val_col - 1]
        if raw_date is None:
            null_streak += 1
            if null_streak >= 5:
                break
            continue
        null_streak = 0
        if isinstance(raw_date, datetime):
            d = raw_date.date()
        elif isinstance(raw_date, date):
            d = raw_date
        else:
            continue
        if raw_val is not None:
            try:
                series[d] = float(raw_val)
            except (TypeError, ValueError):
                pass
    return series


# ============================================================
# 날짜 탐색
# ============================================================
def nearest_on_or_before(series: dict, target: date):
    candidates = [d for d in series if d <= target]
    if not candidates:
        return None, None
    best = max(candidates)
    return best, series[best]


# ============================================================
# GOLD yfinance (SSL 우회 포함)
# ============================================================
def fetch_gold_series(base_date: date):
    """Yahoo Finance CSV API 직접 호출 (SSL 검증 비활성화 — 회사 네트워크 우회)"""
    try:
        import requests, csv
        from io import StringIO

        start_ts = int(datetime(base_date.year, 1, 1).timestamp()) - 86400 * 5
        end_ts   = int(datetime(base_date.year, base_date.month, base_date.day).timestamp()) + 86400 * 2
        url = (f"https://query1.finance.yahoo.com/v7/finance/download/GC%3DF"
               f"?period1={start_ts}&period2={end_ts}&interval=1d&events=history")
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, verify=False, timeout=10)
        resp.raise_for_status()
        series = {}
        reader = csv.DictReader(StringIO(resp.text))
        for row in reader:
            try:
                d = date.fromisoformat(row["Date"])
                c = row.get("Close") or row.get("Adj Close")
                if c and c != "null":
                    series[d] = float(c)
            except Exception:
                continue
        if series:
            print(f"  [           GOLD] Yahoo CSV OK ({len(series)}일치)")
        else:
            print("[WARN] GOLD Yahoo CSV 빈 응답")
        return series
    except Exception as e:
        print(f"[WARN] GOLD fetch 실패: {e}")
        return {}


# ============================================================
# 변화량 계산
# ============================================================
def calc_change(t0_val, ref_val, index_type):
    if t0_val is None or ref_val is None:
        return None, None
    change = t0_val - ref_val
    if index_type in ("rate", "spread"):
        change_pct = round(change * 100, 2)   # bp
    else:
        change_pct = round(change / ref_val * 100, 3) if ref_val else None
    return round(change, 6), change_pct


# ============================================================
# 메인
# ============================================================
def generate_data(excel_path: str, output_path: str = None):
    if output_path is None:
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

    today        = date.today()
    base_date    = today - timedelta(days=1)   # 초기값 (나중에 실제 T0로 보정)
    one_m_target = base_date - relativedelta(months=1)

    print(f"[INFO] 초기 기준일: {base_date}")
    print(f"[INFO] 엑셀 경로: {excel_path}")

    wb = openpyxl.load_workbook(excel_path, data_only=True, read_only=True)

    gold_series = {}
    results = []
    actual_t0_date = None   # 첫 번째 유효 지표의 실제 T0 날짜

    for cfg in INDEX_CONFIG:
        label   = cfg["label"]
        sheet   = cfg["sheet"]
        itype   = cfg["type"]
        section = cfg["section"]

        if sheet == "__yfinance__":
            series = gold_series
        elif sheet == "__pending__":
            results.append({
                "label":   label,
                "section": section,
                "type":    itype,
                "holiday": False,
                "pending": True,
                "T0":  {"date": None, "value": None},
                "1D":  {"date": None, "value": None, "change": None},
                "1M":  {"date": None, "value": None, "change": None},
                "YTM": {"date": None, "value": None, "change": None},
            })
            print(f"  [{label:>15}] 준비중")
            continue
        else:
            if sheet not in wb.sheetnames:
                print(f"[WARN] 시트 없음: {sheet} ({label})")
                results.append(_make_empty(cfg))
                continue

            ws = wb[sheet]
            date_col, val_col = find_columns(ws, cfg["header"], cfg["value_col"])

            if date_col is None or val_col is None:
                print(f"[WARN] 컬럼 탐지 실패: {label} (header='{cfg['header']}', value_col='{cfg['value_col']}')")
                results.append(_make_empty(cfg))
                continue

            series = read_series(ws, date_col, val_col)

        t0_date, t0_val = nearest_on_or_before(series, base_date)
        holiday = (t0_val is None)

        if t0_date is not None:
            d1_series = {d: v for d, v in series.items() if d < t0_date}
            d1_date, d1_val = nearest_on_or_before(d1_series, t0_date - timedelta(days=1))
        else:
            d1_date, d1_val = None, None

        m1_date, m1_val = nearest_on_or_before(series, one_m_target)

        # YTM: 해당 연도 첫 거래일 (데이터가 없으면 Jan 2 fallback)
        ytm_year  = base_date.year
        ytm_first = first_trading_day_of_year(series, ytm_year)
        ytm_target_date = ytm_first if ytm_first else date(ytm_year, 1, 2)
        ytm_date, ytm_val = nearest_on_or_before(series, ytm_target_date)

        _, d1_chg  = calc_change(t0_val, d1_val,  itype)
        _, m1_chg  = calc_change(t0_val, m1_val,  itype)
        _, ytm_chg = calc_change(t0_val, ytm_val, itype)

        # 첫 번째 유효 T0 날짜를 actual_t0_date로 확정
        if actual_t0_date is None and t0_date is not None and not holiday:
            actual_t0_date = t0_date

        results.append({
            "label":   label,
            "section": section,
            "type":    itype,
            "holiday": holiday,
            "T0":  {"date": str(t0_date)  if t0_date  else None, "value": t0_val},
            "1D":  {"date": str(d1_date)  if d1_date  else None, "value": d1_val,  "change": d1_chg},
            "1M":  {"date": str(m1_date)  if m1_date  else None, "value": m1_val,  "change": m1_chg},
            "YTM": {"date": str(ytm_date) if ytm_date else None, "value": ytm_val, "change": ytm_chg},
        })

        status = "휴장" if holiday else f"T0={t0_val}"
        print(f"  [{label:>15}] {status}")

    wb.close()

    # base_date를 실제 첫 번째 유효 T0 날짜로 보정
    if actual_t0_date is not None:
        base_date = actual_t0_date
        print(f"[INFO] 실제 기준일(T0) 확정: {base_date}")

    # ── 차트용 시계열 수집 (1년치)
    chart_cutoff = base_date - timedelta(days=366)
    chart_series = []
    for ccfg in CHART_CONFIG:
        sheet, hdr, vcol = ccfg["sheet"], ccfg["header"], ccfg["value_col"]
        series_data = {"label": ccfg["label"], "color": ccfg["color"],
                       "dash": ccfg["dash"], "dates": [], "values": []}
        try:
            wb2 = openpyxl.load_workbook(excel_path, data_only=True, read_only=True)
            if sheet in wb2.sheetnames:
                ws2 = wb2[sheet]
                dc, vc = find_columns(ws2, hdr, vcol)
                if dc and vc:
                    raw = read_series(ws2, dc, vc)
                    pairs = sorted([(d, v) for d, v in raw.items()
                                    if chart_cutoff <= d <= base_date])
                    series_data["dates"]  = [str(d) for d, _ in pairs]
                    series_data["values"] = [v for _, v in pairs]
                    print(f"  [CHART {ccfg['label']:>12}] {len(pairs)}일치")
                else:
                    print(f"  [CHART {ccfg['label']:>12}] 컬럼 탐지 실패 — header='{hdr}'")
            else:
                print(f"  [CHART {ccfg['label']:>12}] 시트 없음: {sheet}")
            wb2.close()
        except Exception as e:
            print(f"  [CHART {ccfg['label']:>12}] 오류: {e}")
        chart_series.append(series_data)

    output = {
        "generated_at": datetime.now().strftime("%Y-%m-%d"),
        "base_date":    str(base_date),
        "today":        str(today),
        "indices":      results,
        "chart_series": chart_series,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    print(f"[OK] data.json 저장 완료 → {output_path}")
    return output


def _make_empty(cfg):
    return {
        "label":   cfg["label"],
        "section": cfg["section"],
        "type":    cfg["type"],
        "holiday": True,
        "T0":  {"date": None, "value": None},
        "1D":  {"date": None, "value": None, "change": None},
        "1M":  {"date": None, "value": None, "change": None},
        "YTM": {"date": None, "value": None, "change": None},
    }


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "데이터.xlsx"
    generate_data(path)
