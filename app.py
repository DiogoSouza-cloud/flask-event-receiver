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

STATIC_ROOT = os.path.join(os.getcwd(), "static")
EV_DIR      = os.path.join(STATIC_ROOT, "ev")
os.makedirs(EV_DIR, exist_ok=True)

# Detecta backend (render costuma usar sqlite)
def _detect_backend(db_url: str) -> str:
    try:
        u = urlparse(db_url)
        return u.scheme or "sqlite"
    except Exception:
        return "sqlite"

BACKEND = _detect_backend(DB_URL)

def _sqlite_db_path_from_url(db_url: str) -> str:
    try:
        u = urlparse(db_url)
        if u.scheme != "sqlite":
            return ""
        path = u.path
        if path.startswith("//"):
            return path[1:]
        return path.lstrip("/") or "eventos.db"
    except Exception:
        return ""

DB_PATH = os.getenv("DB_PATH", _sqlite_db_path_from_url(DB_URL))

md = MetaData()
eventos_tb = Table(
    "eventos", md,
    Column("id", Integer, primary_key=True),
    Column("timestamp", Text),          # "YYYY-MM-DD HH:MM:SS"
    Column("status", Text),             # 'alerta'
    Column("objeto", Text),             # CSV objetos YOLO
    Column("descricao", Text),          # texto final traduzido
    Column("identificador", Text),      # origem
    Column("imagem", Text),             # base64 opcional (p/ futuras migrações)
    Column("img_url", Text),            # URL relativa: /static/ev/YYYY-MM-DD/HHMMSS_rand.jpg
    Column("camera_id", Text),          # adicionado
    Column("local", Text),              # adicionado
)

engine = create_engine(DB_URL, future=True)

def ensure_schema():
    with engine.begin() as conn:
        # Cria tabela se não existir
        if BACKEND == "sqlite":
            conn.exec_driver_sql("""
                CREATE TABLE IF NOT EXISTS eventos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    status TEXT,
                    objeto TEXT,
                    descricao TEXT,
                    identificador TEXT,
                    imagem TEXT,
                    img_url TEXT,
                    camera_id TEXT,
                    local TEXT
                )
            """)
        else:
            # Render (Postgres) – criar se necessário
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS eventos (
                    id SERIAL PRIMARY KEY,
                    timestamp TEXT,
                    status TEXT,
                    objeto TEXT,
                    descricao TEXT,
                    identificador TEXT,
                    imagem TEXT,
                    img_url TEXT,
                    camera_id TEXT,
                    local TEXT
                )
            """))

        # Garantir colunas em bases antigas
        try:
            cols = {r[1] for r in conn.exec_driver_sql("PRAGMA table_info(eventos)")} if BACKEND=="sqlite" \
                   else {r[0] for r in conn.execute(text("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name='eventos'
                   """))}
        except Exception:
            cols = set()

        if "camera_id" not in cols:
            conn.execute(text("ALTER TABLE eventos ADD COLUMN camera_id TEXT"))
        if "local" not in cols:
            conn.execute(text("ALTER TABLE eventos ADD COLUMN local TEXT"))

ensure_schema()

# -------------------- Util --------------------
def save_image_from_base64(b64: str) -> str:
    """
    Salva imagem base64 em static/ev/YYYY-MM-DD/HHMMSS_rand.jpg
    Retorna o caminho relativo começando por /static/...
    """
    try:
        day = datetime.now().strftime("%Y-%m-%d")
        outdir = os.path.join(EV_DIR, day)
        os.makedirs(outdir, exist_ok=True)

        raw = base64.b64decode(b64, validate=True)
        stamp = datetime.now().strftime("%H%M%S")
        rand  = f"{int.from_bytes(os.urandom(3), 'big')%1000000:06d}"
        fname = f"{stamp}_{rand}.jpg"

        fpath = os.path.join(outdir, fname)
        with open(fpath, "wb") as f:
            f.write(raw)

        # URL relativa servida pelo Flask
        return f"/static/ev/{day}/{fname}"
    except Exception:
        return ""

def prune_if_needed():
    """
    Para SQLite local: quando o arquivo do DB passar de PRUNE_THRESHOLD,
    apaga registros mais antigos até cair para PRUNE_TARGET.
    Em Postgres no Render, o custo é do plano do banco, então não prune aqui.
    """
    if BACKEND != "sqlite":
        return
    if not DB_PATH or not os.path.exists(DB_PATH):
        return
    try:
        size = os.path.getsize(DB_PATH)
        # threshold “aproximado”: 5GB * PRUNE_THRESHOLD. Se não souber o limite, pule.
        limit = float(os.getenv("DB_LIMIT_BYTES", "0"))
        if limit <= 0:
            return
        if size/limit < PRUNE_THRESHOLD:
            return

        target_bytes = PRUNE_TARGET * limit
        to_delete = 0
        with engine.begin() as conn:
            # Conta e apaga por lote
            total = conn.execute(text("SELECT COUNT(*) FROM eventos")).scalar_one()
            if total <= 0:
                return
            # heurística: apaga 10% e verifica
            step = max(1, int(total * 0.10))
            while True:
                conn.execute(text("DELETE FROM eventos WHERE id IN (SELECT id FROM eventos ORDER BY id ASC LIMIT :n)"),
                             {"n": step})
                to_delete += step
                conn.commit()
                size = os.path.getsize(DB_PATH)
                if size <= target_bytes:
                    break
                if step == 1:
                    break
    except Exception:
        pass

# -------------------- Ingestão --------------------
app = Flask(__name__)

@app.post("/evento")
@app.post("/alerta")  # compat
def evento():
    data = request.get_json(force=True, silent=True) or {}

    identificador = str(data.get("identificador") or "").strip() or "desconhecido"
    status        = "alerta"
    objeto        = str(data.get("object") or data.get("objeto") or "").strip()
    descricao_pt  = str(data.get("descricao_pt") or data.get("description") or data.get("descricao_raw") or "").strip()

    # novos campos
    camera_id     = str(data.get("camera_id") or "").strip()
    local_txt     = str(data.get("local") or "").strip()

    # imagem base64 (opcional)
    img_b64 = data.get("image") or ""
    img_url = save_image_from_base64(img_b64) if img_b64 else ""

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO eventos (timestamp, status, objeto, descricao, identificador, imagem, img_url, camera_id, local)
            VALUES (:ts, :st, :obj, :desc, :idn, :img, :imgurl, :cam, :loc)
        """), dict(
            ts=ts, st=status, obj=objeto, desc=descricao_pt, idn=identificador,
            img="", imgurl=img_url, cam=camera_id, loc=local_txt
        ))
    prune_if_needed()
    return jsonify({"ok": True})

# -------------------- Consulta --------------------
def buscar_eventos(filtro=None, data=None, status=None, limit=50, offset=0):
    sql = [
        "SELECT id, timestamp, status, objeto, descricao, identificador,",
        "CASE WHEN imagem IS NULL OR imagem = '' THEN 0 ELSE 1 END AS tem_img,",
        "COALESCE(img_url,'') AS img_url,",
        "COALESCE(camera_id,'') AS camera_id,",
        "COALESCE(local,'') AS local",
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
                    expr = (
                        f"(instr(objeto, :{k}) > 0 OR instr(descricao, :{k}) > 0 OR "
                        f"instr(identificador, :{k}) > 0 OR instr(camera_id, :{k}) > 0 OR instr(local, :{k}) > 0)"
                    )
                else:
                    expr = (
                        f"(POSITION(:{k} IN objeto) > 0 OR POSITION(:{k} IN descricao) > 0 OR "
                        f"POSITION(:{k} IN identificador) > 0 OR POSITION(:{k} IN camera_id) > 0 OR POSITION(:{k} IN local) > 0)"
                    )
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
            tem_img=bool(r[6]), img_url=(r[7] or ""),
            camera_id=(r[8] or ""), local=(r[9] or "")
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
      --bg: #faf6ed; --card: #ffffff; --ink: #1b1b1b; --muted: #666;
      --alert: #c62828; --ok: #2e7d32; --accent: #111;
    }
    * { box-sizing: border-box; }
    body { margin:0; font-family: Arial, Helvetica, sans-serif; color:var(--ink); background:var(--bg); }
    header { background: var(--bg); display:flex; align-items:center; justify-content:space-between;
      padding:14px 24px; border-bottom:1px solid #e6e0d5; }
    .head-left { display:flex; align-items:center; gap:18px; }
    .logo-iaprotect { height:30px; }
    .logo-rowau { height:22px; opacity:.9 }
    main { max-width:1000px; margin:20px auto; padding:0 12px; }
    .toolbar { display:flex; gap:10px; align-items:center; margin:10px 0 18px; }
    input,button,select { padding:8px 10px; font:inherit; }
    .grid { display:flex; flex-direction:column; gap:14px; }
    .card { display:flex; gap:14px; background:var(--card); border:1px solid #e6e0d5; border-radius:12px; padding:12px; }
    .thumb { width:200px; height:140px; object-fit:cover; border-radius:8px; border:1px solid #ddd; background:#eee }
    .meta { font-size:13px; color:var(--muted); margin-bottom:6px; }
    .kv { margin:3px 0; }
    .ctx { margin-top:8px; line-height:1.35 }
  </style>
</head>
<body>
  <header>
    <div class="head-left">
      <img class="logo-iaprotect" src="https://raw.githubusercontent.com/weavertechbr/assets/main/logo_iaprotect.svg" alt="IAProtect" />
      <img class="logo-rowau" src="https://raw.githubusercontent.com/weavertechbr/assets/main/logo_rowau.svg" alt="Rowau" />
    </div>
  </header>

  <main>
    <h2>{{ page_title }}</h2>
    <form class="toolbar" method="get" action="/historico">
      <input type="text" name="filtro" placeholder="Palavra-chave" value="{{ filtro or '' }}">
      <input type="date" name="data" value="{{ data or '' }}">
      <button>Buscar</button>
    </form>

    <div class="grid">
      {% for e in eventos %}
      <div class="card">
        {% if e.tem_img and e.img_url %}
          <img class="thumb" src="{{ e.img_url }}" alt="frame">
        {% else %}
          <div style="width:200px;height:140px" class="thumb"></div>
        {% endif %}
        <div>
          <div class="meta">{{ e.timestamp }}</div>
          <div class="kv"><b>Identificador:</b> {{ e.identificador }}</div>
          <div class="kv"><b>Status:</b> {{ e.status|capitalize }}</div>
          <div class="kv"><b>Objeto:</b> {{ e.objeto }}</div>
          <div class="kv"><b>CAM:</b> {{ e.camera_id }}</div>
          <div class="kv"><b>Local:</b> {{ e.local }}</div>
          <div class="ctx"><b>Analise objeto:</b> {{ e.descricao|safe }}</div>
        </div>
      </div>
      {% else %}
      <p style="color:#666">Sem eventos.</p>
      {% endfor %}
    </div>
  </main>
</body>
</html>
"""

# -------------------- Rotas --------------------
@app.get("/historico")
def historico():
    filtro = (request.args.get("filtro") or "").strip()
    data   = (request.args.get("data") or "").strip()
    page_title = "Painel Histórico"
    evs = buscar_eventos(filtro=filtro if filtro else None,
                         data=data if data else None,
                         status=None, limit=200, offset=0)
    return render_template_string(HTML_TEMPLATE, page_title=page_title,
                                  eventos=evs, filtro=filtro, data=data)

@app.get("/health")
def health():
    return "ok"

@app.get("/static/ev/<path:relpath>")
def static_ev(relpath):
    # Servir arquivos de imagem gravados localmente
    full = os.path.join(EV_DIR, relpath)
    if not os.path.isfile(full):
        abort(404)
    return send_file(full, mimetype="image/jpeg")

# -------------------- Admin prune (opcional) --------------------
@app.post("/admin/prune")
def admin_prune():
    if (request.args.get("key") or request.headers.get("X-Admin-Key") or "") != ADMIN_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    prune_if_needed()
    return jsonify({"ok": True})

# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
