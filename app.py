from flask import Flask, render_template, request, jsonify, session, redirect, send_file
from functools import wraps
import os
import json
import uuid
import hashlib
import tempfile
import shutil
import io
import re
import sqlite3
from datetime import datetime, timedelta
import pandas as pd

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'performance-pulse-secret')

# 本地开发默认开启模板热更新（改 HTML/CSS/JS 无需重启）
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.jinja_env.auto_reload = True

_BASE = os.path.dirname(os.path.abspath(__file__))
# 优先用环境变量；否则尝试在项目目录建 data/，失败则用 /tmp（Railway 只读文件系统）
DATA_DIR = os.environ.get('DATA_DIR') or os.path.join(_BASE, 'data')
DB_PATH = os.path.join(DATA_DIR, 'performance_pulse.db')

EXCEL_DEFAULT = {
    "date_col": "B",
    "people_cols": ["C", "D"],
    "header_row": 1,
    "people_names": []
}


def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    global DATA_DIR, DB_PATH
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        # 测试可写
        _test = os.path.join(DATA_DIR, '.write_test')
        open(_test, 'w').close()
        os.remove(_test)
    except OSError:
        # 文件系统只读（如 Railway），改用 /tmp
        DATA_DIR = '/tmp/pp_data'
        DB_PATH   = os.path.join(DATA_DIR, 'performance_pulse.db')
        os.makedirs(DATA_DIR, exist_ok=True)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'admin'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS people (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS performance_records (
                id TEXT PRIMARY KEY,
                person_id INTEGER NOT NULL,
                client TEXT,
                task_desc TEXT NOT NULL,
                quantity REAL NOT NULL,
                unit TEXT,
                date TEXT NOT NULL,
                month TEXT NOT NULL,
                year TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE CASCADE
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS excel_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                date_col TEXT NOT NULL,
                people_cols TEXT NOT NULL,
                header_row INTEGER NOT NULL,
                people_names TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                chart_js_url TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS custom_filters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                keyword TEXT NOT NULL,
                kpi REAL NOT NULL DEFAULT 1,
                enabled INTEGER NOT NULL DEFAULT 1
            )
        """)
        cur.execute("""
            INSERT OR IGNORE INTO app_config (id, chart_js_url)
            VALUES (1, 'https://cdn.staticfile.net/Chart.js/3.9.1/chart.min.js')
        """)
        # 兼容旧库：补齐 kpi 字段
        try:
            cur.execute("SELECT kpi FROM custom_filters LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE custom_filters ADD COLUMN kpi REAL NOT NULL DEFAULT 1")
        # 兼容旧库：补齐 extra_json 字段（存储主题/标题/小数位等扩展配置）
        try:
            cur.execute("SELECT extra_json FROM app_config LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE app_config ADD COLUMN extra_json TEXT NOT NULL DEFAULT '{}'")
        cur.execute("""
            INSERT OR IGNORE INTO excel_config (id, date_col, people_cols, header_row, people_names)
            VALUES (1, ?, ?, ?, ?)
        """, (EXCEL_DEFAULT["date_col"], json.dumps(EXCEL_DEFAULT["people_cols"]), EXCEL_DEFAULT["header_row"], json.dumps(EXCEL_DEFAULT["people_names"])))
        cur.execute("""
            INSERT OR IGNORE INTO app_config (id, chart_js_url)
            VALUES (1, ?)
        """, ("https://cdn.staticfile.net/Chart.js/3.9.1/chart.min.js",))
        conn.commit()

        row = cur.execute("SELECT date_col, people_cols FROM excel_config WHERE id=1").fetchone()
        if row:
            try:
                people_cols = json.loads(row["people_cols"])
            except Exception:
                people_cols = []
            if row["date_col"] == "A" and people_cols == ["C"]:
                cur.execute("UPDATE excel_config SET date_col=?, people_cols=? WHERE id=1",
                            (EXCEL_DEFAULT["date_col"], json.dumps(EXCEL_DEFAULT["people_cols"])))
                conn.commit()

        cur.execute("SELECT COUNT(*) AS cnt FROM users")
        if cur.fetchone()["cnt"] == 0:
            cur.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, 'admin')",
                        ("admin", hash_pw("admin123")))
        conn.commit()


def load_excel_config():
    with get_db() as conn:
        row = conn.execute("SELECT * FROM excel_config WHERE id=1").fetchone()
    if not row:
        return dict(EXCEL_DEFAULT)
    return {
        "date_col": row["date_col"],
        "people_cols": json.loads(row["people_cols"]),
        "header_row": int(row["header_row"]),
        "people_names": json.loads(row["people_names"])
    }


def load_app_config():
    with get_db() as conn:
        row = conn.execute("SELECT * FROM app_config WHERE id=1").fetchone()
    if not row:
        return {"chart_js_url": "https://cdn.staticfile.net/Chart.js/3.9.1/chart.min.js"}
    extra = {}
    try:
        extra = json.loads(row["extra_json"] or "{}")
    except Exception:
        pass
    return {"chart_js_url": row["chart_js_url"], **extra}


def load_config_payload():
    with get_db() as conn:
        people_rows = conn.execute("SELECT id, name, enabled FROM people ORDER BY name").fetchall()
        filter_rows = conn.execute("SELECT id, name, keyword, kpi, enabled FROM custom_filters ORDER BY id").fetchall()

    people = [r["name"] for r in people_rows]
    people_enabled = [r["name"] for r in people_rows if r["enabled"]]

    custom_filters = [
        {
            "id": r["id"],
            "name": r["name"],
            "keyword": r["keyword"],
            "kpi": r["kpi"],
            "enabled": bool(r["enabled"])
        }
        for r in filter_rows
    ]

    return {
        "people": people,
        "people_enabled": people_enabled,
        "excel": load_excel_config(),
        "custom_filters": custom_filters,
        "app": load_app_config()
    }


def save_config_payload(payload):
    """
    Patch 模式：只更新 payload 中实际传入的 key，其余字段读旧值保留。
    支持的顶层 key：people / people_enabled / custom_filters / excel / app
    """
    import traceback
    keys = list(payload.keys())
    if 'custom_filters' in payload:
        app.logger.warning(f"[CONFIG] save_config_payload: custom_filters被写入, count={len(payload.get('custom_filters') or [])}, 调用栈:\n{''.join(traceback.format_stack())}")
    else:
        app.logger.info(f"[CONFIG] save_config_payload: keys={keys}")
    with get_db() as conn:
        cur = conn.cursor()

        # ── 人员 ─────────────────────────────────────────────
        if "people" in payload:
            people  = payload["people"]
            enabled = set(payload.get("people_enabled", people))
            cur.execute("DELETE FROM people")
            for name in people:
                cur.execute("INSERT INTO people (name, enabled) VALUES (?, ?)",
                            (name, 1 if name in enabled else 0))
        elif "people_enabled" in payload:
            enabled = set(payload["people_enabled"])
            for row in cur.execute("SELECT id, name FROM people").fetchall():
                cur.execute("UPDATE people SET enabled=? WHERE id=?",
                            (1 if row["name"] in enabled else 0, row["id"]))

        # ── 自定义筛选项 ──────────────────────────────────────
        if "custom_filters" in payload:
            items_to_save = [
                item for item in (payload["custom_filters"] or [])
                if (item or {}).get("name", "").strip() and (item or {}).get("keyword", "").strip()
            ]
            # 安全保护：传入空列表时不执行清空（防止前端未加载完就保存）
            if not items_to_save and not payload["custom_filters"]:
                pass  # 空列表：跳过，不清空数据库
            else:
                cur.execute("DELETE FROM custom_filters")
            for item in items_to_save:
                name    = (item or {}).get("name", "").strip()
                keyword = (item or {}).get("keyword", "").strip()
                if not name or not keyword:
                    continue
                enabled_flag = 1 if (item or {}).get("enabled", True) else 0
                kpi = float((item or {}).get("kpi", 1) or 1)
                cur.execute("INSERT INTO custom_filters (name, keyword, kpi, enabled) VALUES (?, ?, ?, ?)",
                            (name, keyword, kpi, enabled_flag))

        # ── Excel 配置 ────────────────────────────────────────
        if "excel" in payload:
            excel = payload["excel"]
            cur.execute(
                "UPDATE excel_config SET date_col=?, people_cols=?, header_row=?, people_names=? WHERE id=1",
                (excel.get("date_col", "B"),
                 json.dumps(excel.get("people_cols", ["C", "D"])),
                 int(excel.get("header_row", 1)),
                 json.dumps(excel.get("people_names", []))))

        # ── app 配置 ──────────────────────────────────────────
        if "app" in payload:
            app_cfg = payload["app"] or {}
            old_extra = {}
            try:
                row = conn.execute("SELECT extra_json FROM app_config WHERE id=1").fetchone()
                if row and row["extra_json"]:
                    old_extra = json.loads(row["extra_json"])
            except Exception:
                pass
            extra_keys = ["module_order", "hidden_modules", "stats_order", "hidden_stats", "footer_items", "favicon_url", "date_format", "default_theme", "titles", "decimal_places", "target_value", "target_config"]
            new_extra  = {k: app_cfg[k] for k in extra_keys if k in app_cfg}
            merged     = {**old_extra, **new_extra}
            chart_js_url = app_cfg.get("chart_js_url", "")
            if chart_js_url:
                cur.execute("UPDATE app_config SET chart_js_url=?, extra_json=? WHERE id=1",
                            (chart_js_url, json.dumps(merged, ensure_ascii=False)))
            else:
                cur.execute("UPDATE app_config SET extra_json=? WHERE id=1",
                            (json.dumps(merged, ensure_ascii=False),))

        conn.commit()


def _excel_col_to_index(col):
    col = (col or '').strip().upper()
    if not col:
        return None
    idx = 0
    for ch in col:
        if not ch.isalpha():
            continue
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1 if idx > 0 else None


def _split_task_text(text):
    text = (text or '').strip()
    if not text:
        return []
    parts = re.split(r'[\n;；]+', text)
    return [p.strip() for p in parts if p.strip()]


def _parse_task_entry(text):
    text = (text or '').strip()
    if not text:
        return None
    m = re.search(r'(.*?)(?:\*|×|x|X)?\s*(\d+(?:\.\d+)?)\s*(张|个|次|套|小时)?$', text)
    if m:
        desc = m.group(1).strip()
        qty = float(m.group(2))
        unit = m.group(3) or ''
    else:
        desc = text
        qty = 1
        unit = ''
    client = ''
    if '-' in desc:
        client = desc.split('-', 1)[0].strip()
    return {
        'task_desc': desc,
        'quantity': qty,
        'unit': unit,
        'client': client
    }


def _check_format_issues(raw_cell, chunks, date_str, person, row_num):
    """检查单元格文本的书写格式问题，返回问题列表"""
    issues = []
    text = str(raw_cell).strip()

    # 1. 使用了逗号分隔（应使用换行或分号）
    if re.search(r'[\uff0c,]', text) and not re.search(r'[;\n；]', text):
        # 排除纯描述中自然出现的逗号（如"A,B项目"），只在有多个任务嫌疑时报
        comma_parts = re.split(r'[\uff0c,]', text)
        if len(comma_parts) >= 2 and any(
            re.search(r'\d', p) or len(p.strip()) > 2 for p in comma_parts[1:]
        ):
            issues.append('使用逗号分隔任务（应用换行或分号 ; ）')

    # 2. 全角连字符作客户分隔（应使用半角 -）
    if re.search(r'[\uff0d\u2014\u2013]', text):
        issues.append('客户与任务之间使用了全角连字符（应使用半角 -）')

    # 3. 乘号使用小写英文 x 或 X（应使用 × 或 *）
    for chunk in chunks:
        if re.search(r'(?:^|[^a-zA-Z])[xX](?=\d)|(?<=\d)[xX](?=[^a-zA-Z]|$)', chunk):
            issues.append(f'数量乘号使用了英文字母 x/X（应使用 × 或 *）：「{chunk}」')
            break

    # 4. 任务描述为空（只写了数字）
    for chunk in chunks:
        parsed = _parse_task_entry(chunk)
        if parsed and not parsed['task_desc'].strip():
            issues.append(f'任务描述为空，仅有数量：「{chunk}」')
            break

    # 5. 单位不在标准列表
    std_units = {'张', '个', '次', '套', '小时', ''}
    for chunk in chunks:
        m = re.search(r'\d+(?:\.\d+)?\s*([^\d\s×*xX\n;；,，]+)$', chunk.strip())
        if m:
            unit = m.group(1).strip()
            if unit and unit not in std_units and len(unit) <= 3:
                issues.append(f'非标准单位「{unit}」（标准：张/个/次/套/小时）')
                break

    # 6. 分隔符中英混用（同时出现 ; 和 ；）
    if ';' in text and '；' in text:
        issues.append('分隔符中英混用（同时使用了 ; 和 ；）')

    if not issues:
        return []
    return [{
        'row': row_num,
        'person': person,
        'date': date_str,
        'text': text[:80] + ('…' if len(text) > 80 else ''),
        'issues': issues
    }]


def _split_keywords(keyword_text: str):
    parts = re.split(r'[，,；;]+', str(keyword_text or '').strip())
    return [p.strip().lower() for p in parts if p and p.strip()]


def _match_keywords(text: str, keyword_text: str) -> bool:
    hay = str(text or '').lower()
    kws = _split_keywords(keyword_text)
    if not kws:
        return False
    return any(k in hay for k in kws)


def _build_period_compare(records):
    def _sum_in_range(start_dt, end_dt):
        s = 0.0
        for r in records:
            ds = r.get('date')
            if not ds:
                continue
            try:
                d = datetime.strptime(ds, '%Y-%m-%d').date()
            except Exception:
                continue
            if start_dt <= d <= end_dt:
                s += float(r.get('kpi_value') or 0)
        return round(s, 4)

    if not records:
        return {
            'week': {'current': 0, 'previous': 0, 'change_pct': None, 'text': '暂无数据'},
            'month': {'current': 0, 'previous': 0, 'change_pct': None},
            'yoy_month': {'current': 0, 'previous': 0, 'change_pct': None}
        }

    date_list = []
    for r in records:
        ds = r.get('date')
        if not ds:
            continue
        try:
            date_list.append(datetime.strptime(ds, '%Y-%m-%d').date())
        except Exception:
            pass
    if not date_list:
        return {
            'week': {'current': 0, 'previous': 0, 'change_pct': None, 'text': '暂无数据'},
            'month': {'current': 0, 'previous': 0, 'change_pct': None},
            'yoy_month': {'current': 0, 'previous': 0, 'change_pct': None}
        }

    end_date = max(date_list)

    week_start = end_date - timedelta(days=6)
    prev_week_end = week_start - timedelta(days=1)
    prev_week_start = prev_week_end - timedelta(days=6)
    wk_cur = _sum_in_range(week_start, end_date)
    wk_prev = _sum_in_range(prev_week_start, prev_week_end)
    wk_pct = ((wk_cur - wk_prev) / wk_prev) if wk_prev else None
    if wk_pct is None:
        wk_text = '本周较上周：暂无对比基数'
    else:
        sign = '+' if wk_pct >= 0 else ''
        wk_text = f"本周较上周 {sign}{wk_pct * 100:.1f}%"

    month_start = end_date.replace(day=1)
    prev_month_end = month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)
    mo_cur = _sum_in_range(month_start, end_date)
    mo_prev = _sum_in_range(prev_month_start, prev_month_end)
    mo_pct = ((mo_cur - mo_prev) / mo_prev) if mo_prev else None

    last_year_same = end_date.replace(year=end_date.year - 1)
    ly_month_start = last_year_same.replace(day=1)
    if end_date.month == 12:
        ly_month_end = ly_month_start.replace(year=ly_month_start.year + 1, month=1) - timedelta(days=1)
    else:
        ly_month_end = ly_month_start.replace(month=ly_month_start.month + 1) - timedelta(days=1)
    yoy_cur = _sum_in_range(month_start, end_date)
    yoy_prev = _sum_in_range(ly_month_start, ly_month_end)
    yoy_pct = ((yoy_cur - yoy_prev) / yoy_prev) if yoy_prev else None

    return {
        'week': {'current': wk_cur, 'previous': wk_prev, 'change_pct': wk_pct, 'text': wk_text},
        'month': {'current': mo_cur, 'previous': mo_prev, 'change_pct': mo_pct},
        'yoy_month': {'current': yoy_cur, 'previous': yoy_prev, 'change_pct': yoy_pct}
    }


def _parse_perf_excel(file_path, cfg):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.xls':
        df = pd.read_excel(file_path, header=None, engine='xlrd')
    else:
        df = pd.read_excel(file_path, header=None, engine='openpyxl')
    header_row = int(cfg.get('excel', {}).get('header_row', 1)) - 1
    date_col = cfg.get('excel', {}).get('date_col', 'A')
    people_cols = cfg.get('excel', {}).get('people_cols', ['C'])
    people_names = cfg.get('excel', {}).get('people_names', [])
    date_idx = _excel_col_to_index(date_col)
    people_idx = [_excel_col_to_index(c) for c in people_cols]

    records = []
    format_warnings = []

    for row_idx in range(header_row + 1, len(df)):
        row = df.iloc[row_idx]
        raw_date = row.iloc[date_idx] if date_idx is not None and date_idx < len(row) else None
        if pd.isna(raw_date):
            continue
        try:
            dt = pd.to_datetime(raw_date)
        except Exception:
            continue
        date_str = dt.strftime('%Y-%m-%d')
        month_str = dt.strftime('%Y-%m')
        year_str = dt.strftime('%Y')

        for idx_pos, col_idx in enumerate(people_idx):
            if col_idx is None or col_idx >= len(row):
                continue
            cell = row.iloc[col_idx]
            if pd.isna(cell) or not str(cell).strip():
                continue
            if idx_pos < len(people_names) and people_names[idx_pos]:
                person = people_names[idx_pos]
            else:
                header_val = ''
                if header_row >= 0 and header_row < len(df):
                    header_val = str(df.iloc[header_row, col_idx])
                person = header_val.strip() if header_val else f'人员{idx_pos + 1}'

            chunks = _split_task_text(str(cell))

            # 格式检查
            format_warnings.extend(
                _check_format_issues(cell, chunks, date_str, person, row_idx + 1)
            )

            for chunk in chunks:
                parsed = _parse_task_entry(chunk)
                if not parsed:
                    continue
                records.append({
                    'id': str(uuid.uuid4()),
                    'person': person,
                    'client': parsed['client'],
                    'task_desc': parsed['task_desc'],
                    'quantity': parsed['quantity'],
                    'unit': parsed['unit'],
                    'date': date_str,
                    'month': month_str,
                    'year': year_str
                })

    return records, format_warnings


# ── Auth ──

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            # API 路由返回 JSON 401，页面路由重定向到登录页
            if request.path.startswith('/api/') or                request.headers.get('Accept', '').find('application/json') >= 0:
                return jsonify({'error': '未登录，请先登录管理后台'}), 401
            return redirect('/admin/login')
        return f(*args, **kwargs)
    return decorated


@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        body = request.json or {}
        username = body.get('username', '')
        password = body.get('password', '')
        with get_db() as conn:
            row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if row and hash_pw(password) == row['password_hash']:
            session['logged_in'] = True
            session['username'] = row['username']
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': '用户名或密码错误'}), 401
    if session.get('logged_in'):
        return redirect('/admin')
    return render_template('login.html')


@app.route('/admin/logout', methods=['POST'])
def admin_logout():
    session.clear()
    return jsonify({'success': True})


# ── Pages ──

@app.route('/')
def root():
    return redirect('/performance')


@app.route('/performance')
def performance_page():
    return render_template('performance.html')


@app.route('/admin')
@login_required
def admin_page():
    return render_template('admin.html')


# ── Performance APIs ──

@app.route('/admin/performance/config', methods=['GET', 'PUT'])
@login_required
def admin_performance_config():
    if request.method == 'GET':
        return jsonify(load_config_payload())
    body = request.json or {}
    save_config_payload(body)
    return jsonify({'success': True, 'config': load_config_payload()})


@app.route('/admin/performance/upload', methods=['POST'])
@login_required
def admin_performance_upload():
    file = request.files.get('file')
    if not file or not file.filename:
        return jsonify({'success': False, 'error': '请上传 Excel 文件'}), 400
    ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
    if ext not in ('xls', 'xlsx'):
        return jsonify({'success': False, 'error': '仅支持 .xls/.xlsx'}), 400
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, file.filename)
    file.save(tmp_path)
    cfg = load_config_payload()
    records, format_warnings = _parse_perf_excel(tmp_path, cfg)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    if not records:
        return jsonify({'success': False, 'error': '未解析到任何记录',
                        'format_warnings': format_warnings}), 400

    # 本次上传数据的日期范围
    new_dates = sorted({r['date'] for r in records})
    date_min, date_max = new_dates[0], new_dates[-1]

    with get_db() as conn:
        cur = conn.cursor()
        # 只删除与本次上传日期范围重叠的旧记录，历史数据保留
        cur.execute(
            "DELETE FROM performance_records WHERE date >= ? AND date <= ?",
            (date_min, date_max)
        )

        # 合并人员：保留已有人员，追加新人员
        existing_people = {
            row['name']: row['id']
            for row in cur.execute("SELECT id, name FROM people").fetchall()
        }
        people_set = sorted({r['person'] for r in records})
        cfg_people = cfg.get('people', [])
        merged_people = sorted(set(cfg_people) | set(existing_people.keys()) | set(people_set))

        enabled = set(cfg.get('people_enabled', []))
        if not enabled:
            enabled = set(merged_people)

        person_ids = dict(existing_people)
        for name in merged_people:
            if name not in person_ids:
                cur.execute("INSERT INTO people (name, enabled) VALUES (?, ?)",
                            (name, 1 if name in enabled else 0))
                person_ids[name] = cur.lastrowid

        for r in records:
            cur.execute("""
                INSERT OR REPLACE INTO performance_records
                (id, person_id, client, task_desc, quantity, unit, date, month, year, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r['id'],
                person_ids[r['person']],
                r['client'],
                r['task_desc'],
                float(r['quantity']),
                r['unit'],
                r['date'],
                r['month'],
                r['year'],
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        conn.commit()

    # 返回数据库中当前所有人员（含历史）
    with get_db() as conn:
        all_people = [r['name'] for r in conn.execute("SELECT name FROM people ORDER BY name").fetchall()]

    return jsonify({
        'success': True,
        'count': len(records),
        'people': all_people,
        'date_range': f'{date_min} ~ {date_max}',
        'format_warnings': format_warnings
    })


@app.route('/admin/performance/records')
@login_required
def admin_performance_records():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT pr.*, p.name AS person
            FROM performance_records pr
            JOIN people p ON pr.person_id = p.id
        """).fetchall()
    return jsonify({'records': [dict(r) for r in rows]})


import mimetypes

@app.route('/api/app/favicon')
def serve_favicon():
    """返回自定义 favicon（本地上传的文件）"""
    for ext in ['png', 'ico', 'jpg', 'jpeg', 'gif', 'svg', 'webp']:
        path = os.path.join(DATA_DIR, f'favicon.{ext}')
        if os.path.exists(path):
            mime = mimetypes.guess_type(path)[0] or 'image/png'
            return send_file(path, mimetype=mime)
    return ('', 404)


@app.route('/favicon.ico')
def favicon_ico():
    """浏览器自动请求的 /favicon.ico，转发到自定义 favicon"""
    cfg = load_app_config()
    if cfg.get('favicon_url'):
        return redirect(cfg['favicon_url'])
    return serve_favicon()


@app.route('/admin/upload/favicon', methods=['POST'])
@login_required
def admin_upload_favicon():
    """上传本地 favicon 图片"""
    file = request.files.get('file')
    if not file or not file.filename:
        return jsonify({'error': '未选择文件'}), 400
    ext = os.path.splitext(file.filename)[1].lower().lstrip('.')
    allowed = {'png', 'ico', 'jpg', 'jpeg', 'gif', 'svg', 'webp'}
    if ext not in allowed:
        return jsonify({'error': f'不支持的格式 .{ext}'}), 400
    # 删除旧的 favicon 文件
    for old_ext in allowed:
        old_path = os.path.join(DATA_DIR, f'favicon.{old_ext}')
        if os.path.exists(old_path):
            os.remove(old_path)
    save_path = os.path.join(DATA_DIR, f'favicon.{ext}')
    file.save(save_path)
    # 清空 favicon_url（本地文件优先）
    _save_favicon_url('')
    return jsonify({'url': '/api/app/favicon', 'type': 'local'})


def _save_favicon_url(url):
    """将 favicon_url 写入 extra_json"""
    with get_db() as conn:
        row = conn.execute("SELECT extra_json FROM app_config WHERE id=1").fetchone()
        extra = {}
        try:
            extra = json.loads((row['extra_json'] if row else None) or '{}')
        except Exception:
            pass
        if url:
            extra['favicon_url'] = url
        else:
            extra.pop('favicon_url', None)
        conn.execute("UPDATE app_config SET extra_json=? WHERE id=1",
                     (json.dumps(extra, ensure_ascii=False),))
        conn.commit()


@app.route('/admin/upload/favicon-url', methods=['POST'])
@login_required
def admin_set_favicon_url():
    """设置外链 favicon URL"""
    data = request.get_json(force=True) or {}
    url = (data.get('url') or '').strip()
    # 删除本地上传的 favicon 文件
    for ext in ['png', 'ico', 'jpg', 'jpeg', 'gif', 'svg', 'webp']:
        path = os.path.join(DATA_DIR, f'favicon.{ext}')
        if os.path.exists(path):
            os.remove(path)
    _save_favicon_url(url)
    return jsonify({'url': url, 'type': 'url'})


@app.route('/api/app/chart-js')
def api_chart_js():
    cfg = load_app_config()
    return redirect(cfg.get('chart_js_url') or 'https://cdn.staticfile.net/Chart.js/3.9.1/chart.min.js')


@app.route('/api/performance/summary')
def api_performance_summary():
    cfg = load_config_payload()
    person = request.args.get('person', '').strip()
    date_from = request.args.get('from', '').strip()
    date_to = request.args.get('to', '').strip()

    person_filters = []
    person_params = []
    if person:
        person_filters.append('p.name = ?')
        person_params.append(person)
    elif cfg.get('people_enabled'):
        person_filters.append('p.name IN (%s)' % (','.join(['?'] * len(cfg['people_enabled']))))
        person_params.extend(cfg['people_enabled'])

    filters = list(person_filters)
    params = list(person_params)
    if date_from:
        filters.append('pr.date >= ?')
        params.append(date_from)
    if date_to:
        filters.append('pr.date <= ?')
        params.append(date_to)

    where_sql = ('WHERE ' + ' AND '.join(filters)) if filters else ''
    where_sql_person = ('WHERE ' + ' AND '.join(person_filters)) if person_filters else ''

    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT pr.*, p.name AS person
            FROM performance_records pr
            JOIN people p ON pr.person_id = p.id
            {where_sql}
            ORDER BY pr.date ASC
        """, params).fetchall()
        rows_for_compare = conn.execute(f"""
            SELECT pr.*, p.name AS person
            FROM performance_records pr
            JOIN people p ON pr.person_id = p.id
            {where_sql_person}
            ORDER BY pr.date ASC
        """, person_params).fetchall()

    records = [dict(r) for r in rows]
    compare_records = [dict(r) for r in rows_for_compare]

    custom_filters = [f for f in (cfg.get('custom_filters') or []) if f.get('enabled')]

    def _split_keywords(keyword_text):
        txt = (keyword_text or '').strip()
        if not txt:
            return []
        parts = re.split(r'[，,;；]+', txt)
        return [p.strip().lower() for p in parts if p and p.strip()]

    def _match_keywords(desc, keyword_text):
        source = (desc or '').lower()
        kws = _split_keywords(keyword_text)
        return any(kw in source for kw in kws)

    def _match_kpi_factor(desc):
        if not custom_filters:
            return 1.0  # 没有筛选项时，kpi_value = quantity 本身
        for flt in custom_filters:
            keyword = (flt.get('keyword') or '').strip()
            if keyword and _match_keywords(desc, keyword):
                return float(flt.get('kpi') or 1)
        return 0.0

    for r in records:
        factor = _match_kpi_factor(r.get('task_desc') or '')
        r['kpi_factor'] = factor
        r['kpi_value'] = round(float(r.get('quantity') or 0) * factor, 4)
    for r in compare_records:
        factor = _match_kpi_factor(r.get('task_desc') or '')
        r['kpi_factor'] = factor
        r['kpi_value'] = round(float(r.get('quantity') or 0) * factor, 4)

    totals = {
        'total_quantity': round(sum(r.get('kpi_value', 0) for r in records), 4),
        'total_kpi': round(sum(r.get('kpi_value', 0) for r in records), 4),
        'total_people': len({r['person'] for r in records}),
        'total_days': len({r['date'] for r in records})
    }

    target_cfg = ((cfg.get('app') or {}).get('target_config') or {})
    default_target = (target_cfg.get('global')
                      if target_cfg.get('global') not in (None, '', 0)
                      else (cfg.get('app') or {}).get('target_value'))
    by_month = target_cfg.get('by_month') or {}
    by_person = target_cfg.get('by_person') or {}
    by_filter = target_cfg.get('by_filter') or {}
    selected_month = date_to[:7] if date_to else (records[-1]['date'][:7] if records else '')

    compare = _build_period_compare(compare_records)

    def _group_sum(key, value_key='kpi_value'):
        out = {}
        for r in records:
            out.setdefault(r[key], 0)
            out[r[key]] += r.get(value_key, 0)
        return [{'label': k, 'value': v} for k, v in sorted(out.items(), key=lambda x: x[0])]

    overall_kpi_by_day = _group_sum('date', 'kpi_value')

    # ── 甲方 KPI 时间走势（按甲方+日期聚合） ─────────────────
    client_kpi_series = {}
    for r in records:
        client = r.get('client') or '未知'
        date   = r.get('date') or ''
        kv     = float(r.get('kpi_value') or 0)
        if not date:
            continue
        if client not in client_kpi_series:
            client_kpi_series[client] = {}
        client_kpi_series[client][date] = client_kpi_series[client].get(date, 0) + kv
    # 转为 [{date, value}] 列表，按日期排序
    client_kpi_series = {
        client: [{'date': d, 'value': round(v, 4) if v % 1 else int(v)}
                 for d, v in sorted(date_map.items())]
        for client, date_map in client_kpi_series.items()
    }

    filter_totals = []
    filter_series = {}
    filter_clients = {}
    for flt in custom_filters:
        keyword = (flt.get('keyword') or '').strip()
        if not keyword:
            continue
        total_qty = 0
        total_kpi = 0
        unit = ''
        day_map = {}
        day_kpi_map = {}
        client_map = {}
        client_kpi_map = {}
        kpi_factor = float(flt.get('kpi') or 1)
        for r in records:
            if _match_keywords(r.get('task_desc') or '', keyword):
                qty = float(r.get('quantity') or 0)
                total_qty += qty
                total_kpi += qty * kpi_factor
                if not unit and (r.get('unit') or '').strip():
                    unit = r.get('unit').strip()
                day_map.setdefault(r['date'], 0)
                day_map[r['date']] += qty
                day_kpi_map.setdefault(r['date'], 0)
                day_kpi_map[r['date']] += qty * kpi_factor
                client = (r.get('client') or '未标注')
                client_map.setdefault(client, 0)
                client_map[client] += qty
                client_kpi_map.setdefault(client, 0)
                client_kpi_map[client] += qty * kpi_factor
        name = flt.get('name') or keyword
        filter_totals.append({
            'id': flt.get('id'),
            'name': name,
            'keyword': keyword,
            'quantity': round(total_qty, 4) if total_qty % 1 else int(total_qty),
            'unit': unit or '',
            'kpi_factor': kpi_factor,
            'kpi_value': round(total_kpi, 4) if total_kpi % 1 else int(total_kpi)
        })
        client_map = {}
        client_kpi_map = {}
        for r in records:
            if _match_keywords(r.get('task_desc') or '', keyword):
                client = (r.get('client') or '未填写').strip() or '未填写'
                qty = float(r.get('quantity') or 0)
                client_map.setdefault(client, 0)
                client_map[client] += qty
                client_kpi_map.setdefault(client, 0)
                client_kpi_map[client] += qty * kpi_factor

        filter_series[name] = {
            'raw': [
                {'date': d, 'value': round(v, 4) if v % 1 else int(v)}
                for d, v in sorted(day_map.items(), key=lambda x: x[0])
            ],
            'kpi': [
                {'date': d, 'value': round(v, 4) if v % 1 else int(v)}
                for d, v in sorted(day_kpi_map.items(), key=lambda x: x[0])
            ],
            'clients_raw': [
                {'label': c, 'value': round(v, 4) if v % 1 else int(v)}
                for c, v in sorted(client_map.items(), key=lambda x: x[0])
            ],
            'clients_kpi': [
                {'label': c, 'value': round(v, 4) if v % 1 else int(v)}
                for c, v in sorted(client_kpi_map.items(), key=lambda x: x[0])
            ]
        }
        filter_clients[name] = {
            'raw': [
                {'label': c, 'value': round(v, 4) if v % 1 else int(v)}
                for c, v in sorted(client_map.items(), key=lambda x: x[1], reverse=True)
            ],
            'kpi': [
                {'label': c, 'value': round(v, 4) if v % 1 else int(v)}
                for c, v in sorted(client_kpi_map.items(), key=lambda x: x[1], reverse=True)
            ]
        }

    layered_target = None
    target_source = 'global'
    if selected_month and selected_month in by_month:
        layered_target = by_month[selected_month]
        target_source = f'month:{selected_month}'
    elif person and person in by_person:
        layered_target = by_person[person]
        target_source = f'person:{person}'
    elif by_filter and filter_totals:
        hit = None
        for ft in filter_totals:
            nm = ft.get('name')
            if nm in by_filter:
                hit = nm
                break
        if hit:
            layered_target = by_filter[hit]
            target_source = f'filter:{hit}'
    if layered_target in (None, '', 0):
        layered_target = default_target
        target_source = 'global'

    resp = jsonify({
        'totals': totals,
        'insights': {
            'trend_text': compare['week']['text'],
            'week_vs_last': compare['week'],
            'month_vs_last': compare['month'],
            'yoy_month': compare['yoy_month'],
            'target': {
                'value': layered_target,
                'source': target_source,
                'month': selected_month,
                'config': target_cfg
            }
        },
        'by_day': _group_sum('date', 'kpi_value'),
        'by_month': _group_sum('month', 'kpi_value'),
        'by_year': _group_sum('year', 'kpi_value'),
        'by_person': _group_sum('person', 'kpi_value'),
        'records': records,
        'filter_totals': filter_totals,
        'filter_series': filter_series,
        'filter_clients': filter_clients,
        'client_kpi_series': client_kpi_series,
        'overall_kpi_by_day': overall_kpi_by_day,
        'config': {
            'people': cfg.get('people', []),
            'people_enabled': cfg.get('people_enabled', []),
            'custom_filters': cfg.get('custom_filters', []),
            'app': cfg.get('app', {})
        }
    })
    resp.headers['Cache-Control'] = 'no-store'
    return resp


# ── 账号管理 API ──────────────────────────────────────────────

@app.route('/admin/users', methods=['GET'])
@login_required
def admin_list_users():
    with get_db() as conn:
        rows = conn.execute("SELECT id, username, role FROM users ORDER BY id").fetchall()
    return jsonify({'users': [dict(r) for r in rows]})


@app.route('/admin/users', methods=['POST'])
@login_required
def admin_create_user():
    data = request.get_json(force=True) or {}
    username = (data.get('username') or '').strip()
    password = (data.get('password') or '').strip()
    role     = (data.get('role') or 'admin').strip()
    if not username or not password:
        return jsonify({'error': '用户名和密码不能为空'}), 400
    if len(password) < 4:
        return jsonify({'error': '密码长度至少 4 位'}), 400
    try:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                (username, hash_pw(password), role)
            )
            conn.commit()
    except sqlite3.IntegrityError:
        return jsonify({'error': f'用户名「{username}」已存在'}), 409
    return jsonify({'ok': True, 'message': f'账号「{username}」创建成功'})


@app.route('/admin/users/<int:uid>', methods=['PUT'])
@login_required
def admin_update_user(uid):
    data = request.get_json(force=True) or {}
    new_username = (data.get('username') or '').strip()
    new_password = (data.get('password') or '').strip()
    if not new_username and not new_password:
        return jsonify({'error': '请提供新用户名或新密码'}), 400
    if new_password and len(new_password) < 4:
        return jsonify({'error': '密码长度至少 4 位'}), 400
    with get_db() as conn:
        row = conn.execute("SELECT id, username FROM users WHERE id=?", (uid,)).fetchone()
        if not row:
            return jsonify({'error': '用户不存在'}), 404
        if new_username and new_username != row['username']:
            exist = conn.execute("SELECT id FROM users WHERE username=? AND id!=?",
                                  (new_username, uid)).fetchone()
            if exist:
                return jsonify({'error': f'用户名「{new_username}」已被占用'}), 409
            conn.execute("UPDATE users SET username=? WHERE id=?", (new_username, uid))
        if new_password:
            conn.execute("UPDATE users SET password_hash=? WHERE id=?",
                         (hash_pw(new_password), uid))
        conn.commit()
    return jsonify({'ok': True, 'message': '账号信息已更新'})


@app.route('/admin/users/<int:uid>', methods=['DELETE'])
@login_required
def admin_delete_user(uid):
    with get_db() as conn:
        # 禁止删除最后一个账号
        cnt = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()['n']
        if cnt <= 1:
            return jsonify({'error': '至少保留一个账号，无法删除'}), 400
        # 禁止删除自己（防止自锁）
        row = conn.execute("SELECT username FROM users WHERE id=?", (uid,)).fetchone()
        if not row:
            return jsonify({'error': '用户不存在'}), 404
        if row['username'] == session.get('username'):
            return jsonify({'error': '不能删除当前登录的账号'}), 400
        conn.execute("DELETE FROM users WHERE id=?", (uid,))
        conn.commit()
    return jsonify({'ok': True, 'message': '账号已删除'})


@app.route('/api/performance/format-check')
def api_format_check():
    """扫描数据库中已入库的记录，检测书写不规范的条目"""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT pr.id, pr.task_desc, pr.quantity, pr.unit, pr.client,
                   pr.date, p.name AS person
            FROM performance_records pr
            JOIN people p ON pr.person_id = p.id
            ORDER BY pr.date DESC, p.name
        """).fetchall()

    warnings = []
    for row in rows:
        # 把数据库里的 task_desc 还原成原始格式检查
        # task_desc 已是单条解析结果，重新构建原始文本做检查
        raw = row['task_desc']
        if row['quantity'] and row['quantity'] != 1:
            raw = f"{raw}{'×' if row['unit'] else '*'}{int(row['quantity']) if row['quantity'] == int(row['quantity']) else row['quantity']}{row['unit'] or ''}"
        chunks = [raw]

        issues = []
        text = row['task_desc']
        unit = (row['unit'] or '').strip()
        client = (row['client'] or '').strip()
        qty = row['quantity']

        # 1. 全角连字符
        if re.search(r'[－—–]', text):
            issues.append('客户与任务之间使用了全角连字符（应使用半角 -）')

        # 2. 乘号使用英文 x/X
        if re.search(r'(?:^|[^a-zA-Z])[xX](?=\d)|(?<=\d)[xX](?=[^a-zA-Z]|$)', text):
            issues.append('数量乘号使用了英文字母 x/X（应使用 × 或 *）')

        # 3. 任务描述为空
        if not text.strip():
            issues.append('任务描述为空')

        # 4. 单位不规范（非空但不在标准列表）
        std_units = {'张', '个', '次', '套', '小时', ''}
        if unit and unit not in std_units:
            issues.append(f'非标准单位「{unit}」（标准：张/个/次/套/小时）')

        # 5. client 与 task_desc 中出现全角符号分隔
        full_text = (client + '-' + text) if client else text
        if re.search(r'[，、]', full_text):
            issues.append('描述中含全角逗号或顿号（建议使用半角）')

        # 6. task_desc 中含疑似多任务混写（换行/分号未拆分）
        if re.search(r'[;；\n]', text):
            issues.append('任务描述中含分隔符（可能是多条任务未拆分）')

        # 7. 未提取到甲方（client 为空，且 task_desc 中不含 - 分隔符）
        if not client and '-' not in text:
            issues.append('未提取到甲方（格式应为「甲方-任务描述」）')

        # 8. 有数量但无单位（quantity > 1 说明原始数据写了数字，但单位为空）
        if qty and qty != 1 and not unit:
            issues.append(f'数量为 {int(qty) if qty == int(qty) else qty} 但未填写单位')



        if issues:
            warnings.append({
                'id':     row['id'],
                'date':   row['date'],
                'person': row['person'],
                'client': row['client'] or '',
                'task_desc': row['task_desc'],
                'quantity': row['quantity'],
                'unit':   row['unit'] or '',
                'issues': issues
            })

    return jsonify({'warnings': warnings, 'total': len(rows), 'issue_count': len(warnings)})


@app.route('/api/performance/export', methods=['POST'])
def api_performance_export():
    body = request.json or {}
    person = (body.get('person') or '').strip()
    date_from = (body.get('from') or '').strip()
    date_to = (body.get('to') or '').strip()

    filters = []
    params = []
    if person:
        filters.append('p.name = ?')
        params.append(person)
    if date_from:
        filters.append('pr.date >= ?')
        params.append(date_from)
    if date_to:
        filters.append('pr.date <= ?')
        params.append(date_to)

    where_sql = ('WHERE ' + ' AND '.join(filters)) if filters else ''

    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT pr.*, p.name AS person
            FROM performance_records pr
            JOIN people p ON pr.person_id = p.id
            {where_sql}
            ORDER BY pr.date ASC
        """, params).fetchall()

    df = pd.DataFrame([dict(r) for r in rows])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='performance')
    buf.seek(0)
    return send_file(buf, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True, download_name='performance_export.xlsx')


init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', '1') == '1'
    app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=debug)
