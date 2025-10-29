import os
import base64
from io import BytesIO
from datetime import datetime, timedelta
from urllib.parse import urlparse

from flask import Flask, request, jsonify, render_template_string, url_for, send_file, abort
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text
from sqlalchemy.sql import text

# -------------------- Config --------------------
DB_URL = os.getenv("DATABASE_URL", "sqlite:///eventos.db")
ADMIN_KEY = os.getenv("ADMIN_KEY", "troque-isto")

PRUNE_THRESHOLD = float(os.getenv("PRUNE_THRESHOLD", "0.80"))
PRUNE_TARGET    = float(os.getenv("PRUNE_TARGET", "0.70"))
PRUNE_BATCH     = int(os.getenv("PRUNE_BATCH", "1000"))
MAX_ROWS        = int(os.getenv("MAX_ROWS", "500000"))

engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
BACKEND = engine.url.get_backend_name()  # 'sqlite', 'postgresql', etc.

def _sqlite_db_path_from_url(db_url: str) -> str:
    u = urlparse(db_url)
    if u.scheme != "sqlite":
        return ""
    path = u.path
    if path.startswith("//"):
        return path[1:]
    return path.lstrip("/") or "eventos.db"

DB_PATH = os.getenv("DB_PATH", _sqlite_db_path_from_url(DB_URL))

md = MetaData()
eventos_tb = Table(
    "eventos", md,
    Column("id", Integer, primary_key=True),
    Column("timestamp", Text),
    Column("status", Text),
    Column("objeto", Text),
    Column("descricao", Text),
    Column("imagem", Text),   # base64 legado (opcional)
    Column("identificador", Text),
    Column("img_url", Text),  # URL pública quando existir
)

def _ensure_img_url_column():
    if BACKEND == "sqlite":
        with engine.begin() as conn:
            cols = conn.execute(text("PRAGMA table_info(eventos)")).all()
            names = {c[1] for c in cols}
            if "img_url" not in names:
                conn.execute(text("ALTER TABLE eventos ADD COLUMN img_url TEXT"))
    else:
        try:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE eventos ADD COLUMN IF NOT EXISTS img_url TEXT"))
        except Exception:
            pass

def init_db():
    md.create_all(engine)
    _ensure_img_url_column()
    os.makedirs("static", exist_ok=True)
    os.makedirs(os.path.join("static", "ev"), exist_ok=True)

# --------- util: salvar imagem em disco e devolver URL relativa ---------
def _save_image_to_static(b64: str) -> str | None:
    if not b64:
        return None
    try:
        raw = base64.b64decode(b64, validate=False)
    except Exception:
        return None
    day = datetime.now().strftime("%Y-%m-%d")
    folder_rel = os.path.join("ev", day)
    folder_abs = os.path.join("static", folder_rel)
    os.makedirs(folder_abs, exist_ok=True)
    fname = datetime.now().strftime("%H%M%S_%f") + ".jpg"
    path_rel = os.path.join(folder_rel, fname).replace("\\", "/")
    path_abs = os.path.join("static", path_rel)
    try:
        with open(path_abs, "wb") as f:
            f.write(raw)
        # retorna URL relativa resolvida via /static
        return url_for('static', filename=path_rel, _external=False)
    except Exception:
        return None

def salvar_evento(ev: dict):
    with engine.begin() as conn:
        conn.execute(eventos_tb.insert().values(**ev))
        prune_if_needed(conn)

# -------------------- Busca --------------------
def buscar_eventos(filtro=None, data=None, status=None, limit=50, offset=0):
    sql = [
        "SELECT id, timestamp, status, objeto, descricao, identificador,",
        "CASE WHEN imagem IS NULL OR imagem = '' THEN 0 ELSE 1 END AS tem_img,",
        "COALESCE(img_url,'') AS img_url",
        "FROM eventos WHERE 1=1",
    ]
    params = {}

    if filtro:
        termos = [t.strip() for t in filtro.replace(",", " ").split() if t.strip()]
        if termos:
            or_parts = []
            for i, t in enumerate(termos):
                k = f"q{i}"
                if BACKEND == "sqlite":
                    expr = f"(instr(objeto, :{k}) > 0 OR instr(descricao, :{k}) > 0 OR instr(identificador, :{k}) > 0)"
                else:
                    expr = f"(POSITION(:{k} IN objeto) > 0 OR POSITION(:{k} IN descricao) > 0 OR POSITION(:{k} IN identificador) > 0)"
                or_parts.append(expr)
                params[k] = t
            sql.append("AND (" + " OR ".join(or_parts) + ")")

    if data:
        if BACKEND == "sqlite":
            sql.append("AND substr(timestamp,1,10) = :d")
        else:
            sql.append("AND left(timestamp,10) = :d")
        params["d"] = data

    if status:
        sql.append("AND status = :s")
        params["s"] = status

    sql.append("ORDER BY id DESC LIMIT :lim OFFSET :off")
    params["lim"] = int(limit)
    params["off"] = int(offset)

    with engine.begin() as conn:
        rows = conn.execute(text(" ".join(sql)), params).all()

    evs = []
    for r in rows:
        evs.append(dict(
            id=r[0], timestamp=r[1], status=r[2], objeto=r[3],
            descricao=r[4], identificador=r[5],
            tem_img=bool(r[6]), img_url=r[7] or ""
        ))
    return evs

# -------------------- Template --------------------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>{{ page_title }}</title>
  <script>
    setInterval(() => {
      const t = document.activeElement && document.activeElement.tagName;
      if (!['INPUT','TEXTAREA','SELECT'].includes(t)) location.reload();
    }, 20000);
  </script>
  <style>
    :root {
      --bg: #faf6ed;
      --card: #ffffff;
      --ink: #1b1b1b;
      --muted: #666;
      --alert: #c62828;
      --ok: #2e7d32;
      --accent: #111;
    }
    * { box-sizing: border-box; }
    body { margin:0; font-family: Arial, Helvetica, sans-serif; color:var(--ink); background:var(--bg); }
    header {
      background: var(--bg);
      display:flex; align-items:center; justify-content:space-between;
      padding:14px 24px; border-bottom:1px solid #e6e0d5;
    }
    .head-left { display:flex; align-items:center; gap:18px; }
    .logo-iaprotect { height:30px; }
    .logo-rowau { height:34px; }
    h1 { margin:0; font-size:30px; font-weight:700; color:var(--accent); }

    .wrap { padding:20px 28px; max-width:1100px; }
    form { margin: 0 0 16px 0; display:flex; gap:8px; }
    input[type="text"], input[type="date"] {
      padding:8px 10px; border:1px solid #d9d3c6; border-radius:6px; background:#fff;
    }
    button { padding:8px 12px; border:1px solid #bdb6a7; background:#fff; border-radius:6px; cursor:pointer; }

    .card {
      display:grid; grid-template-columns: 200px 1fr;
      gap:16px; align-items:start;
      background:var(--card); padding:14px; margin:14px 0;
      border-radius:10px; border:1px solid #e6e0d5;
    }
    .card.alerta { border-left:6px solid var(--alert); }
    .thumb {
      width:200px; height:140px; object-fit:cover; border:1px solid #ddd; border-radius:6px; background:#f5f5f5;
    }
    .meta { font-size:14px; color:var(--muted); margin-bottom:6px; }
    .kv { margin:4px 0; }
    .kv b { display:inline-block; width:160px; }
    .ctx { margin-top:6px; line-height:1.35; }
    .pager { margin-top:12px; }
    .pager a { margin-right:10px; }
  </style>
</head>
<body>
  <header>
    <div class="head-left">
      <img src="{{ iaprotect_url }}" class="logo-iaprotect" alt="IAProtect">
      <h1>{{ page_title }}</h1>
    </div>
    <img src="{{ logo_url }}" class="logo-rowau" alt="ROWAU">
  </header>

  <div class="wrap">
    <form method="get">
      <input type="text" name="filtro" placeholder="Palavra-chave" value="{{ filtro }}">
      <input type="date" name="data" value="{{ data }}">
      <button type="submit">Buscar</button>
    </form>

    {% for e in eventos %}
      <div class="card {% if e.status == 'alerta' %}alerta{% endif %}">
        {% if e.img_url %}
          <img class="thumb" src="{{ e.img_url }}" loading="lazy" alt="frame do evento">
        {% elif e.tem_img %}
          <img class="thumb" src="{{ url_for('img', ev_id=e.id) }}" loading="lazy" alt="frame do evento">
        {% else %}
          <div style="width:200px;height:140px" class="thumb"></div>
        {% endif %}

        <div>
          <div class="meta">{{ e.timestamp }}</div>

          <div class="kv"><b>Identificador:</b> {{ e.identificador }}</div>
          <div class="kv"><b>Status:</b> {{ e.status|capitalize }}</div>
          <div class="kv"><b>Objeto:</b> {{ e.objeto }}</div>

          <div class="ctx"><b>Analise objeto:</b> {{ e.descricao|safe }}</div>
        </div>
      </div>
    {% else %}
      <p>Nenhum evento encontrado.</p>
    {% endfor %}

    <div class="pager">
      {% if page > 1 %}
        <a href="?filtro={{ filtro }}&data={{ data }}&page={{ page-1 }}">◀ Anterior</a>
      {% endif %}
      <a href="?filtro={{ filtro }}&data={{ data }}&page={{ page+1 }}">Próxima ▶</a>
    </div>
  </div>
</body>
</html>
"""

# -------------------- App --------------------
app = Flask(__name__)

# logos e fallbacks
_TRANSPARENT_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAASsJTYQAAAAASUVORK5CYII="

@app.route("/logo-fallback.png")
def logo_fallback():
    img = base64.b64decode(_TRANSPARENT_PNG_B64)
    return send_file(BytesIO(img), mimetype="image/png", max_age=86400)

@app.route("/logo-uploaded.png")
def logo_uploaded():
    path = "Logo Rowau Preto.png"
    if os.path.exists(path):
        return send_file(path, mimetype="image/png", max_age=86400)
    img = base64.b64decode(_TRANSPARENT_PNG_B64)
    return send_file(BytesIO(img), mimetype="image/png", max_age=86400)

@app.route("/iaprotect-uploaded.png")
def iaprotect_uploaded():
    path = "IAprotect.png"
    if os.path.exists(path):
        return send_file(path, mimetype="image/png", max_age=86400)
    img = base64.b64decode(_TRANSPARENT_PNG_B64)
    return send_file(BytesIO(img), mimetype="image/png", max_age=86400)

def _logo_url():
    if os.path.exists(os.path.join("static", "logo_rowau.png")):
        return url_for('static', filename='logo_rowau.png')
    if os.path.exists("Logo Rowau Preto.png"):
        return url_for('logo_uploaded')
    return url_for('logo_fallback')

def _iaprotect_url():
    if os.path.exists(os.path.join("static", "iaprotect.png")):
        return url_for('static', filename='iaprotect.png')
    if os.path.exists("IAprotect.png"):
        return url_for('iaprotect_uploaded')
    return url_for('logo_fallback')

@app.after_request
def no_cache(resp):
    if request.path.startswith(("/logo-", "/img/", "/static/")):
        return resp
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

# -------------------- Reset admin --------------------
@app.route("/admin/reset")
def admin_reset():
    key = request.args.get("key", "")
    if key != ADMIN_KEY:
        abort(403)
    try:
        if BACKEND == "sqlite" and DB_PATH and os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            init_db()
        else:
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM eventos"))
        return "OK: banco recriado", 200
    except Exception as e:
        return f"ERRO: {e}", 500

# -------------------- Imagens legadas (base64) --------------------
@app.route("/img/<int:ev_id>")
def img(ev_id: int):
    with engine.begin() as conn:
        row = conn.execute(text("SELECT imagem FROM eventos WHERE id=:i"), {"i": ev_id}).first()
    if not row or not row[0]:
        abort(404)
    try:
        b = base64.b64decode(row[0], validate=False)
    except Exception:
        abort(404)
    return send_file(BytesIO(b), mimetype="image/jpeg", max_age=3600)

# -------------------- Rotas principais --------------------
@app.route("/")
def index():
    return "Online. POST /evento | POST /resposta_ia | GET /historico | GET /alertas | GET /admin/reset?key=..."

@app.route("/evento", methods=["POST"])
def receber_evento():
    dados = request.json or {}

    # prioriza URL pública enviada pelo cliente; salva base64 só se necessário
    img_url_in = (dados.get("img_url") or "").strip()
    img_b64 = (dados.get("image") or "").strip()
    img_url = img_url_in or (_save_image_to_static(img_b64) if img_b64 else "")

    evento = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "alerta" if dados.get("detected") else "ok",
        "objeto": dados.get("object", ""),
        "descricao": (dados.get("description", "") or "").replace("\n", "<br>"),
        "imagem": "",                 # legado vazio
        "img_url": img_url,           # URL pública ou ""
        "identificador": dados.get("identificador", "desconhecido"),
    }
    salvar_evento(evento)
    return jsonify({"ok": True})

@app.route("/resposta_ia", methods=["POST"])
def receber_resposta_ia():
    dados = request.json or {}
    evento = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "ok",
        "objeto": "Análise IA",
        "descricao": (dados.get("resposta", "") or "").replace("\n", "<br>"),
        "imagem": "",
        "img_url": "",
        "identificador": dados.get("identificador", "desconhecido"),
    }
    salvar_evento(evento)
    return jsonify({"ok": True})

@app.route("/historico")
def historico():
    filtro = (request.args.get("filtro") or "").strip()
    data = (request.args.get("data") or "").strip()
    page = max(int(request.args.get("page") or 1), 1)
    evs = buscar_eventos(filtro if filtro else None, data if data else None,
                         status=None, limit=50, offset=(page-1)*50)
    return render_template_string(
        HTML_TEMPLATE,
        page_title="Painel Histórico",
        eventos=evs,
        filtro=filtro,
        data=data,
        page=page,
        logo_url=_logo_url(),
        iaprotect_url=_iaprotect_url()
    )

@app.route("/alertas")
def alertas():
    raw = (request.args.get("filtro") or "Perigo Sim").strip()
    data = (request.args.get("data") or "").strip()
    page = max(int(request.args.get("page") or 1), 1)

    evs = buscar_eventos(
        filtro=raw,
        data=data if data else None,
        status=None,
        limit=50,
        offset=(page-1)*50
    )

    limiar = datetime.now() - timedelta(minutes=60)
    recentes = []
    for e in evs:
        ts = e.get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            if dt >= limiar:
                recentes.append(e)
        except ValueError:
            continue

    return render_template_string(
        HTML_TEMPLATE,
        page_title="Painel de Alertas",
        eventos=recentes,
        filtro=raw,
        data=data,
        page=page,
        logo_url=_logo_url(),
        iaprotect_url=_iaprotect_url()
    )

# -------------------- Poda automática --------------------
def _disk_usage_for_path(path: str):
    try:
        st = os.statvfs(os.path.abspath(path))
        total = st.f_frsize * st.f_blocks
        used  = total - (st.f_frsize * st.f_bavail)
        return used, total
    except Exception:
        return 0, 0

def prune_if_needed(conn):
    removed_total = 0

    if BACKEND == "sqlite" and DB_PATH:
        used, total = _disk_usage_for_path(DB_PATH)
        if total <= 0:
            return
        uso = used / total
        if uso < PRUNE_THRESHOLD:
            return
        while uso > PRUNE_TARGET:
            r = conn.execute(text("""
                DELETE FROM eventos
                WHERE id IN (
                    SELECT id FROM eventos
                    ORDER BY timestamp ASC
                    LIMIT :lim
                )
            """), {"lim": PRUNE_BATCH})
            conn.commit()
            removed = r.rowcount or 0
            if removed == 0:
                break
            if BACKEND == "sqlite":
                try:
                    conn.execute(text("VACUUM"))
                    conn.commit()
                except Exception:
                    pass
            used, total = _disk_usage_for_path(DB_PATH)
            uso = used / total
            removed_total += removed
    else:
        total_rows = conn.execute(text("SELECT COUNT(*) FROM eventos")).scalar_one()
        if total_rows <= int(MAX_ROWS * PRUNE_THRESHOLD):
            return
        target_rows = int(MAX_ROWS * PRUNE_TARGET)
        to_remove = max(0, total_rows - target_rows)
        while to_remove > 0:
            step = min(PRUNE_BATCH, to_remove)
            r = conn.execute(text("""
                DELETE FROM eventos
                WHERE id IN (
                    SELECT id FROM eventos
                    ORDER BY timestamp ASC
                    LIMIT :lim
                )
            """), {"lim": step})
            conn.commit()
            removed = r.rowcount or 0
            if removed == 0:
                break
            removed_total += removed
            to_remove -= removed

    try:
        print(f"[PRUNE] removidos={removed_total}")
    except Exception:
        pass

# -------------------- Main --------------------
if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=10000)
