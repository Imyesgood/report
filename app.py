import json, os, sqlite3, threading, webbrowser, traceback
from datetime import datetime
from flask import Flask, jsonify, request, send_file, Response

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
SETTINGS_PATH = os.path.join(BASE_DIR, "settings.json")
DATA_PATH     = os.path.join(BASE_DIR, "data.json")

# HTML 파일 자동 탐색 (어떤 이름이든 찾아냄)
def find_html():
    for name in ["report_gpt_fixed.html", "report.html"]:
        p = os.path.join(BASE_DIR, name)
        if os.path.exists(p):
            return p
    # 없으면 .html 파일 중 가장 최근 것
    htmls = [f for f in os.listdir(BASE_DIR) if f.endswith(".html")]
    if htmls:
        return os.path.join(BASE_DIR, sorted(htmls)[-1])
    return None

def load_settings():
    if not os.path.exists(SETTINGS_PATH):
        default = {
            "excel_path": "C:/Users/YourName/Desktop/데이터.xlsx",
            "db_path": os.path.join(BASE_DIR, "comments.db"),
            "port": 5000
        }
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)
        print(f"[SETUP] settings.json 생성됨. excel_path 수정 필요:\n  {SETTINGS_PATH}")
    with open(SETTINGS_PATH, encoding="utf-8") as f:
        return json.load(f)

def get_db():
    conn = sqlite3.connect(SETTINGS["db_path"])
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    # 새 단일-payload 스키마
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comments_v2 (
            date       TEXT PRIMARY KEY,
            payload    TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT
        )
    """)
    conn.commit()

    # 구버전 테이블(comments) 있으면 마이그레이션
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    if "comments" in tables and "comments_v2" in tables:
        old_rows = conn.execute("SELECT * FROM comments").fetchall()
        migrated = 0
        for row in old_rows:
            try:
                d = row["date"]
                # 이미 v2에 있으면 스킵
                exists = conn.execute("SELECT 1 FROM comments_v2 WHERE date=?", (d,)).fetchone()
                if exists:
                    continue
                payload = {}
                for key in ("indicators","events","equity","bond","commodity","currency","schedule_table","payload"):
                    try:
                        val = row[key]
                        if val is None:
                            continue
                        if key in ("indicators","schedule_table","payload"):
                            payload.update({"_raw_"+key: val})
                        else:
                            payload[key] = val
                    except IndexError:
                        pass
                # indicators/schedule_table JSON 파싱
                for jkey in ("indicators","schedule_table"):
                    raw = payload.pop("_raw_"+jkey, None)
                    if raw:
                        try:
                            payload[jkey] = json.loads(raw)
                        except:
                            payload[jkey] = [] if jkey=="indicators" else {}
                raw_p = payload.pop("_raw_payload", None)
                if raw_p:
                    try:
                        payload.update(json.loads(raw_p))
                    except:
                        pass
                updated_at = None
                try:
                    updated_at = row["updated_at"]
                except:
                    pass
                conn.execute(
                    "INSERT OR IGNORE INTO comments_v2 (date,payload,updated_at) VALUES (?,?,?)",
                    (d, json.dumps(payload, ensure_ascii=False), updated_at)
                )
                migrated += 1
            except Exception as e:
                print(f"[WARN] 마이그레이션 실패 {row[0]}: {e}")
        if migrated:
            conn.commit()
            print(f"[INFO] 구버전 DB 마이그레이션 완료: {migrated}건")
    conn.close()

EMPTY_CMT = {
    "indicators": [],
    "events": "", "equity": "", "bond": "", "commodity": "", "currency": "",
    "events_align": {"h":"left","v":"top"},
    "summary_align": {
        "bond":{"h":"left","v":"top"}, "equity":{"h":"left","v":"top"},
        "currency":{"h":"left","v":"top"}, "commodity":{"h":"left","v":"top"}
    },
    "schedule_table": {}
}

app = Flask(__name__, static_folder=BASE_DIR)

@app.errorhandler(Exception)
def handle_exception(e):
    traceback.print_exc()
    return jsonify({"status":"error","message":str(e)}), 500

@app.route("/")
def index():
    html = find_html()
    if not html:
        return "report.html 파일을 찾을 수 없습니다.", 404
    return send_file(html)

@app.route("/api/data")
def api_data():
    if not os.path.exists(DATA_PATH):
        return jsonify({"error": "data.json 없음. 엑셀 경로 확인 후 재파싱하세요."}), 404
    with open(DATA_PATH, encoding="utf-8") as f:
        return jsonify(json.load(f))

@app.route("/api/comments", methods=["GET"])
def get_comments():
    d = request.args.get("date", "")
    conn = get_db()
    row = conn.execute("SELECT payload FROM comments_v2 WHERE date=?", (d,)).fetchone()
    conn.close()
    if row:
        try:
            data = json.loads(row["payload"] or "{}")
            # 빠진 키 보완
            result = dict(EMPTY_CMT)
            result.update(data)
            return jsonify(result)
        except Exception as e:
            print(f"[WARN] payload 파싱 실패: {e}")
    return jsonify(dict(EMPTY_CMT))

@app.route("/api/comments", methods=["POST"])
def save_comments():
    raw = request.get_data()
    try:
        data = json.loads(raw)
    except Exception as e:
        return jsonify({"status":"error","message":f"JSON 파싱 실패: {e}"}), 400

    d = data.get("date","")
    if not d:
        return jsonify({"status":"error","message":"date 필드 없음"}), 400

    payload = {k:v for k,v in data.items() if k != "date"}
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO comments_v2 (date,payload,updated_at) VALUES (?,?,?)",
        (d, json.dumps(payload, ensure_ascii=False), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn.commit()
    conn.close()
    return jsonify({"status":"ok","date":d})

@app.route("/api/dates")
def list_dates():
    conn = get_db()
    rows = conn.execute(
        "SELECT date, updated_at FROM comments_v2 ORDER BY date DESC LIMIT 30"
    ).fetchall()
    conn.close()
    return jsonify([{"date":r["date"],"updated_at":r["updated_at"]} for r in rows])

@app.route("/api/refresh", methods=["POST"])
def refresh_data():
    try:
        import importlib, data_parser
        importlib.reload(data_parser)
        body = {}
        try: body = request.get_json(force=True) or {}
        except: pass
        print(f"[REFRESH] 받은 payload: {body}")
        data_parser.generate_data(
            SETTINGS["excel_path"], DATA_PATH,
            override_date = body.get("t0") or body.get("date") or request.args.get("t0") or request.args.get("date"),
            d1_override = body.get("t_minus1") or body.get("d1_date") or request.args.get("t_minus1") or request.args.get("d1_date"),
            ytm_override = body.get("ytm_start") or body.get("ytm_date") or request.args.get("ytm_start") or request.args.get("ytm_date"),
            generated_at_override = body.get("generated_at") or request.args.get("generated_at"),
        )
        return jsonify({"status":"ok"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status":"error","message":str(e)}), 500

if __name__ == "__main__":
    SETTINGS = load_settings()
    excel_path = SETTINGS.get("excel_path","")
    if not os.path.exists(excel_path):
        print(f"[WARN] 엑셀 없음: {excel_path}")
        print("       settings.json에서 excel_path를 실제 경로로 수정하세요.")
    else:
        print("[INFO] 데이터 파싱 중...")
        try:
            from data_parser import generate_data
            generate_data(excel_path, DATA_PATH)
        except Exception as e:
            traceback.print_exc()
            print(f"[ERROR] 파싱 실패: {e}")
    init_db()
    html = find_html()
    print(f"[INFO] HTML: {html}")
    port = SETTINGS.get("port", 5000)
    url  = f"http://localhost:{port}"
    print(f"\n[START] {url}  (종료: Ctrl+C)\n")
    threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    app.run(host="127.0.0.1", port=port, debug=False)
else:
    SETTINGS = load_settings()
