import os
import base64
from io import BytesIO
from datetime import datetime, timedelta
from urllib.parse import urlparse
import re
import hashlib

from flask import Flask, request, jsonify, render_template_string, url_for, send_file, abort, redirect
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text
from sqlalchemy.sql import text

# -------------------- Config --------------------
DB_URL = os.getenv("DATABASE_URL", "sqlite:///eventos.db")
ADMIN_KEY = os.getenv("ADMIN_KEY", "troque-isto")

PRUNE_THRESHOLD = float(os.getenv("PRUNE_THRESHOLD", "0.80"))
PRUNE_TARGET    = float(os.getenv("PRUNE_TARGET", "0.70"))
PRUNE_BATCH     = int(os.getenv("PRUNE_BATCH", "1000"))
MAX_ROWS        = int(os.getenv("MAX_ROWS", "500000"))
UPDATE_WINDOW_SEC = int(os.getenv("UPDATE_WINDOW_SEC", "15"))

CONFIRM_VALUE = "SIM"

engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
BACKEND = engine.url.get_backend_name()

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
    Column("descricao", Text),      # YOLO puro (renderizado no painel)
    Column("imagem", Text),         # base64 armazenado (legado)
    Column("identificador", Text),
    Column("img_url", Text),

    Column("camera_id", Text),
    Column("camera_name", Text),    # << NOME DA CÂMERA
    Column("local", Text),
    Column("descricao_raw", Text),  # texto bruto recebido
    Column("descricao_pt", Text),   # espelho do YOLO puro
    Column("model_yolo", Text),
    Column("classes", Text),
    Column("yolo_conf", Text),
    Column("yolo_imgsz", Text),

    # chaves de correlação imagem/análise
    Column("job_id", Text),
    Column("sha256", Text),
    Column("file_name", Text),

    # resposta LLaVA (nunca misturar em 'descricao')
    Column("llava_pt", Text),
    Column("dur_llava_ms", Text),

    # confirmação do operador (NOVO)
    Column("confirmado", Text),
    Column("relato_operador", Text),
    Column("confirmado_por", Text),
    Column("confirmado_em", Text),
)

# =========================
# Qualificação de Incidente (lookup + relação N:N)
# =========================
QUALIFICACOES_FIXAS = [
    "Tentativa de acesso não autorizado",
    "Furto",
    "Dano ao patrimônio público ou privado",
    "Roubo",
    "Ostentação de arma de fogo",
    "Porte de arma oculta",
    "Acidente em via pública",
    "Agressão física sem arma",
    "Ataque com arma branca",
    "Ataque com arma de fogo",
    "Violência sexual",
    "Conflito generalizado",
]

qualificacoes_tb = Table(
    "qualificacao_incidente",
    md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("nome", Text, nullable=False, unique=True),
)

evento_qualificacao_tb = Table(
    "evento_qualificacao",
    md,
    Column("evento_id", Integer, primary_key=True, nullable=False),
    Column("qualificacao_id", Integer, primary_key=True, nullable=False),
)

def _seed_qualificacoes():
    """Garante que a tabela de qualificações exista e esteja populada com os valores fixos."""
    with engine.begin() as conn:
        # cria tabelas (caso ainda não existam)
        md.create_all(engine)

        # busca existentes
        existentes = {
            (r[0] or "").strip()
            for r in conn.execute(text("SELECT nome FROM qualificacao_incidente")).fetchall()
        }

        faltantes = [q for q in QUALIFICACOES_FIXAS if q not in existentes]
        if faltantes:
            conn.execute(
                text("INSERT INTO qualificacao_incidente (nome) VALUES " + ",".join(["(:n%d)" % i for i in range(len(faltantes))])),
                {f"n{i}": faltantes[i] for i in range(len(faltantes))}
            )

def _listar_qualificacoes():
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id, nome FROM qualificacao_incidente ORDER BY id")).fetchall()
        return [{"id": int(r[0]), "nome": r[1]} for r in rows]

def _qualificacoes_do_evento(evento_id: int):
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT qualificacao_id FROM evento_qualificacao WHERE evento_id=:id ORDER BY qualificacao_id"),
            {"id": int(evento_id)},
        ).fetchall()
        return {int(r[0]) for r in rows}

def _salvar_qualificacoes_evento(conn, evento_id: int, qual_ids):
    """Atualiza a relação evento->qualificações na mesma transação."""
    conn.execute(text("DELETE FROM evento_qualificacao WHERE evento_id=:id"), {"id": int(evento_id)})
    if not qual_ids:
        return
    # insere (evita duplicar)
    for qid in qual_ids:
        conn.execute(
            text(
                "INSERT INTO evento_qualificacao (evento_id, qualificacao_id) "
                "VALUES (:e, :q) ON CONFLICT DO NOTHING"
            ),
            {"e": int(evento_id), "q": int(qid)},
        )


def _ensure_columns():
    """Migração leve: adiciona colunas que faltarem."""
    if BACKEND == "sqlite":
        with engine.begin() as conn:
            cols = conn.execute(text("PRAGMA table_info(eventos)")).all()
            names = {c[1] for c in cols}
            def add(colname): conn.execute(text(f"ALTER TABLE eventos ADD COLUMN {colname} TEXT"))

            # existentes
            if "img_url"       not in names: add("img_url")
            if "camera_id"     not in names: add("camera_id")
            if "camera_name"   not in names: add("camera_name")
            if "local"         not in names: add("local")
            if "descricao_raw" not in names: add("descricao_raw")
            if "descricao_pt"  not in names: add("descricao_pt")
            if "model_yolo"    not in names: add("model_yolo")
            if "classes"       not in names: add("classes")
            if "yolo_conf"     not in names: add("yolo_conf")
            if "yolo_imgsz"    not in names: add("yolo_imgsz")
            if "job_id"        not in names: add("job_id")
            if "sha256"        not in names: add("sha256")
            if "file_name"     not in names: add("file_name")
            if "llava_pt"      not in names: add("llava_pt")
            if "dur_llava_ms"  not in names: add("dur_llava_ms")

            # NOVO (confirmação)
            if "confirmado"       not in names: add("confirmado")
            if "relato_operador"  not in names: add("relato_operador")
            if "confirmado_por"   not in names: add("confirmado_por")
            if "confirmado_em"    not in names: add("confirmado_em")
    else:
        stmts = [
            # existentes
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS img_url TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS camera_id TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS camera_name TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS local TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS descricao_raw TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS descricao_pt TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS model_yolo TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS classes TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS yolo_conf TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS yolo_imgsz TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS job_id TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS sha256 TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS file_name TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS llava_pt TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS dur_llava_ms TEXT",

            # NOVO (confirmação)
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS confirmado TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS relato_operador TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS confirmado_por TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS confirmado_em TEXT",
        ]
        with engine.begin() as conn:
            for s in stmts:
                try:
                    conn.execute(text(s))
                except Exception:
                    pass

def init_db():
    md.create_all(engine)
    _ensure_columns()
    try:
        _seed_qualificacoes()
    except Exception as _e:
        # não impede subida do serviço se a seed falhar
        print('WARN: seed qualificacoes falhou:', _e)
    os.makedirs("static", exist_ok=True)
    os.makedirs(os.path.join("static", "ev"), exist_ok=True)

def salvar_evento(ev: dict):
    with engine.begin() as conn:
        conn.execute(eventos_tb.insert().values(**ev))
        prune_if_needed(conn)

# -------------------- Busca p/ painel --------------------
def buscar_eventos(filtro=None, data=None, status=None, confirmado=None, limit=50, offset=0):
    sql = [
        "SELECT id, timestamp, status, objeto, descricao, identificador,",
        "CASE WHEN imagem IS NULL OR imagem = '' THEN 0 ELSE 1 END AS tem_img,",
        "COALESCE(img_url,'') AS img_url,",
        "COALESCE(camera_id,'') AS camera_id,",
        "COALESCE(camera_name,'') AS camera_name,",
        "COALESCE(local,'') AS local,",
        "COALESCE(model_yolo,'') AS model_yolo,",
        "COALESCE(classes,'') AS classes,",
        "COALESCE(yolo_conf,'') AS yolo_conf,",
        "COALESCE(yolo_imgsz,'') AS yolo_imgsz,",
        "COALESCE(llava_pt,'') AS llava_pt,",
        "COALESCE(job_id,'') AS job_id,",
        "COALESCE(sha256,'') AS sha256,",
        "COALESCE(file_name,'') AS file_name,",
        "COALESCE(confirmado,'') AS confirmado,",
        "COALESCE(relato_operador,'') AS relato_operador,",
        "COALESCE(confirmado_por,'') AS confirmado_por,",
        "COALESCE(confirmado_em,'') AS confirmado_em",
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
                        f"instr(identificador, :{k}) > 0 OR instr(camera_id, :{k}) > 0 OR "
                        f"instr(camera_name, :{k}) > 0 OR instr(local, :{k}) > 0 OR instr(job_id, :{k}) > 0 OR "
                        f"instr(relato_operador, :{k}) > 0)"
                    )
                else:
                    expr = (
                        f"(POSITION(:{k} IN objeto) > 0 OR POSITION(:{k} IN descricao) > 0 OR "
                        f"POSITION(:{k} IN identificador) > 0 OR POSITION(:{k} IN camera_id) > 0 OR "
                        f"POSITION(:{k} IN camera_name) > 0 OR POSITION(:{k} IN local) > 0 OR POSITION(:{k} IN job_id) > 0 OR "
                        f"POSITION(:{k} IN relato_operador) > 0)"
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

    # confirmado: "SIM" ou "NAO"
    if confirmado == "SIM":
        sql.append("AND confirmado = :c1")
        params["c1"] = CONFIRM_VALUE
    elif confirmado == "NAO":
        sql.append("AND (confirmado IS NULL OR confirmado = '' OR confirmado <> :c2)")
        params["c2"] = CONFIRM_VALUE

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
            tem_img=bool(r[6]), img_url=r[7] or "",
            camera_id=r[8] or "",
            camera_name=r[9] or "",
            local=r[10] or "",
            model_yolo=r[11] or "", classes=r[12] or "",
            yolo_conf=r[13] or "", yolo_imgsz=r[14] or "",
            llava_pt=r[15] or "",
            job_id=r[16] or "", sha256=r[17] or "", file_name=r[18] or "",
            confirmado=r[19] or "",
            relato_operador=r[20] or "",
            confirmado_por=r[21] or "",
            confirmado_em=r[22] or "",
        ))
    return evs

# -------------------- Template --------------------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>{{ page_title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <script>
    setInterval(() => {
      const t = document.activeElement && document.activeElement.tagName;
      if (!['INPUT','TEXTAREA','SELECT','BUTTON'].includes(t)) location.reload();
    }, 20000);
  </script>
  <style>
    :root{
      --bg: #f7f7f8; --surface: #ffffff; --ink: #101010;
      --muted:#6b7280; --line:#e5e7eb; --brand:#111827;
      --danger:#dc2626; --ok:#16a34a; --chip:#eef2ff; --chip-ink:#3730a3;
    }
    *{box-sizing:border-box}
    html,body{height:100%}
    body{ margin:0; font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial; color:var(--ink); background:var(--bg); }
    header{ position:sticky; top:0; z-index:10; background:linear-gradient(180deg,#ffffff 0%,#fafafa 100%); border-bottom:1px solid var(--line);
      display:flex; align-items:center; justify-content:space-between; gap:16px; padding:12px 20px; }
    .brand{display:flex; align-items:center; gap:14px}
    .logo-iaprotect{height:28px} .logo-rowau{height:34px}
    h1{margin:0; font-size:20px; color:var(--brand); font-weight:700}
    .wrap{max-width:1080px; margin:0 auto; padding:18px}
    .toolbar{ position:sticky; top:64px; z-index:9; background:var(--surface); border:1px solid var(--line);
      padding:10px; border-radius:10px; box-shadow:0 2px 6px rgba(0,0,0,.04); display:flex; gap:8px; align-items:center; margin-bottom:16px; }
    .toolbar input[type="text"], .toolbar input[type="date"]{ border:1px solid var(--line); background:#fff; color:var(--ink);
      padding:8px 10px; border-radius:8px; outline:none; min-width:220px; }
    .toolbar button{ border:1px solid var(--line); background:#111827; color:#fff; padding:8px 14px; border-radius:8px; cursor:pointer; }
    .grid{display:grid; grid-template-columns: 280px 1fr; gap:16px}
    @media (max-width: 860px){ .grid{ grid-template-columns: 1fr; } }
    .thumb{ width:100%; aspect-ratio: 4/3; object-fit:cover; border:1px solid var(--line); border-radius:10px; background:#f3f4f6; }
    .card{ background:var(--surface); border:1px solid var(--line); border-left:6px solid transparent; border-radius:12px; padding:14px; margin:14px 0; }
    .card.alerta{ border-left-color: var(--danger); }
    .meta{ color:var(--muted); font-size:12.5px; margin-bottom:6px }
    .kv{ margin:4px 0; font-size:14.5px } .kv b{ color:#374151; display:inline-block; min-width:148px }
    .ctx{ margin-top:8px; line-height:1.45 }
    .badge{ display:inline-block; font-size:12px; padding:3px 8px; border-radius:999px; border:1px solid var(--line); background:#fff; color:#374151; margin-left:8px; }
    .badge.alerta{ background:#fee2e2; color:#991b1b; border-color:#fecaca }
    .chips{ display:flex; flex-wrap:wrap; gap:6px; margin-top:6px }
    .chip{ background:var(--chip); color:var(--chip-ink); border:1px solid #e0e7ff; padding:3px 8px; border-radius:999px; font-size:12px }
    .pager{ display:flex; gap:12px; margin-top:18px } .pager a{ color:#2563eb; text-decoration:none; font-size:14px }
    .sep{ height:1px; background:var(--line); margin:10px 0 }
    .confirmBox{ background:#fafafa; border:1px solid var(--line); border-radius:10px; padding:10px; }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <img src="{{ iaprotect_url }}" class="logo-iaprotect" alt="IAProtect">
      <h1>{{ page_title }}</h1>
      {% if eventos and eventos|length > 0 %}
        <span class="badge">Itens: {{ eventos|length }}</span>
      {% endif %}
    </div>
    <img src="{{ logo_url }}" class="logo-rowau" alt="ROWAU">
  </header>

  <div class="wrap">
    <form method="get" class="toolbar">
      <input type="text" name="filtro" placeholder="Palavra-chave" value="{{ filtro }}">
      <input type="date" name="data" value="{{ data }}">
      <button type="submit">Buscar</button>
    </form>

    {% for e in eventos %}
      <div class="card {% if e.status.lower() == 'alerta' %}alerta{% endif %}">
        <div class="grid">
          <div>
            {% if e.tem_img %}
              <img class="thumb" src="{{ url_for('img', ev_id=e.id) }}" loading="lazy" alt="frame do evento">
            {% else %}
              <div class="thumb"></div>
            {% endif %}
          </div>
          <div>
            <div class="meta">{{ e.timestamp }}</div>

            <div class="kv"><b>Evento ID:</b> {{ e.id }}
              {% if e.job_id %}<span class="badge">JOB {{ e.job_id }}</span>{% endif %}
              {% if e.file_name %}<span class="badge">FILE {{ e.file_name }}</span>{% endif %}
              {% if e.sha256 %}<span class="badge">SHA {{ e.sha256[:10] }}…</span>{% endif %}
            </div>

            <div class="kv"><b>Identificador:</b> {{ e.identificador }}
              <span class="badge {% if e.status.lower() == 'alerta' %}alerta{% endif %}">{{ e.status|capitalize }}</span>
              {% if e.confirmado == 'SIM' %}
                <span class="badge" style="background:#dcfce7;border-color:#bbf7d0;color:#166534;">Violência confirmada</span>
              {% endif %}
            </div>

            <div class="kv"><b>Objeto:</b> {{ e.objeto }}</div>

            <div class="kv"><b>CÂMERA:</b>
              {{ e.camera_id or '-' }}
              {% if e.camera_name %}
                – {{ e.camera_name }}
              {% endif %}
            </div>

            <div class="kv"><b>Local:</b> {{ e.local or '-' }}</div>

            <div class="sep"></div>
            <div class="ctx">
              <b>Analise objeto:</b> {{ e.descricao|safe }}
              {% if e.llava_pt %}
                <div class="sep"></div>
                <div class="ctx"><b>Diagnóstico:</b> {{ e.llava_pt }}</div>
              {% endif %}
            </div>

            {% if e.confirmado == 'SIM' %}
              <div class="sep"></div>
              <div class="confirmBox">
                <div class="kv"><b>Relato do operador:</b> {{ e.relato_operador }}</div>
                <div class="kv"><b>Confirmado por:</b> {{ e.confirmado_por or '-' }}</div>
                <div class="kv"><b>Confirmado em:</b> {{ e.confirmado_em or '-' }}</div>
              </div>
            {% endif %}

          </div>
        </div>
      </div>
    {% else %}
      <p style="color:#6b7280">Nenhum evento encontrado.</p>
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

# -------------------- Imagem base64 (legado) --------------------
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

# -------------------- Helpers de parsing --------------------
_LAVA_MARKER = re.compile(r"(?:^|\n)\s*🌐\s*Analisar\s+local:\s*", re.IGNORECASE)

def _split_yolo_llava(desc: str):
    if not desc:
        return "", ""
    m = _LAVA_MARKER.split(desc, maxsplit=1)
    if len(m) == 1:
        return desc.strip(), ""
    yolo = m[0].strip()
    llava = m[1].strip()
    return yolo, llava

# -------------------- Rotas principais --------------------
@app.route("/")
def index():
    return (
        "Online. "
        "POST /evento | POST /resposta_ia | "
        "GET /historico | GET /alertas | GET /indicios | GET /confirmados | "
        "POST /api/confirmar?key=... | POST /api/desconfirmar?key=... | "
        "GET /api/events | GET /api/stats | GET /admin/reset?key=..."
    )

def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _close_window_html(next_url: str, message: str = "Registro atualizado. Você pode fechar esta janela."):
    """Página HTML que tenta fechar a janela (aba aberta via Grafana).
    Se o browser bloquear o close(), redireciona para next_url como fallback."""
    msg = (message or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    nxt = (next_url or "/confirmados").replace('"', '%22')
    return f"""<!doctype html>
<html><head><meta charset='utf-8'><title>OK</title></head>
<body style='font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; padding: 18px;'>
  <p>{msg}</p>
  <script>
    try {{ if (window.opener && window.opener.location) window.opener.location.reload(); }} catch(e) {{}}
    try {{ window.close(); }} catch(e) {{}}
    setTimeout(function() {{ try {{ window.close(); }} catch(e) {{}} }}, 250);
    setTimeout(function() {{ window.location.href = \"{nxt}\"; }}, 800);
  </script>
</body></html>"""

def _trim(s):
    return (s or "").strip()

def _sha1_from_b64_image(img_b64: str) -> str:
    """
    Calcula SHA-1 do JPEG/bytes armazenados em base64.
    Retorna '' se falhar.
    Aceita base64 puro ou data URL (data:image/...;base64,...).
    """
    if not img_b64:
        return ""
    try:
        if "," in img_b64:
            img_b64 = img_b64.split(",", 1)[1].strip()
        raw = base64.b64decode(img_b64, validate=False)
        return hashlib.sha1(raw).hexdigest()
    except Exception:
        return ""

def _admin_ok():
    # aceita ?key=... (querystring), key em form-data (POST), ou header X-Admin-Key
    kq = (request.values.get("key") or "").strip()
    kh = (request.headers.get("X-Admin-Key") or "").strip()
    return (kq and kq == ADMIN_KEY) or (kh and kh == ADMIN_KEY)

@app.route("/evento", methods=["POST"])
def receber_evento():
    dados = request.json or {}

    # Novos + legado
    job_id      = _trim(dados.get("job_id"))
    camera_id   = _trim(dados.get("camera_id"))
    camera_name = _trim(dados.get("camera_name"))
    local       = _trim(dados.get("local"))

    img_url     = _trim(dados.get("img_url") or dados.get("url") or dados.get("image_url") or dados.get("img") or "")

    # Preferir YOLO puro se vier no campo dedicado
    yolo_desc_in = _trim(dados.get("descricao_yolo_pt"))
    desc_raw_in  = _trim(dados.get("descricao_raw") or dados.get("description"))
    desc_pt_in   = _trim(dados.get("descricao_pt") or dados.get("description") or "")

    if not yolo_desc_in and desc_pt_in:
        yolo_desc_in, llava_extra = _split_yolo_llava(desc_pt_in)
    else:
        llava_extra = ""

    model_yolo  = _trim(dados.get("model_yolo") or dados.get("model"))
    classes     = _trim(dados.get("classes"))
    yolo_conf   = _trim(str(dados.get("yolo_conf") or dados.get("conf") or ""))
    yolo_imgsz  = _trim(str(dados.get("yolo_imgsz") or dados.get("imgsz") or ""))

    sha256 = _trim(dados.get("sha256") or dados.get("img_hash") or dados.get("sha"))
    file_name   = _trim(dados.get("file_name"))
    img_b64     = _trim(dados.get("image"))

    llava_pt_in = _trim(dados.get("llava_pt")) or ""
    if not llava_pt_in:
        llava_pt_in = llava_extra

    base_row = {
        "timestamp": _now_str(),
        "status": "alerta" if dados.get("detected") else "ok",
        "objeto": dados.get("object", ""),
        "descricao": (yolo_desc_in or "").replace("\n", "<br>"),
        "imagem": img_b64,
        "img_url": img_url,
        "identificador": dados.get("identificador", "desconhecido"),
        "camera_id": camera_id,
        "camera_name": camera_name,
        "local": local,
        "descricao_raw": desc_raw_in,
        "descricao_pt": (yolo_desc_in or ""),
        "model_yolo": model_yolo,
        "classes": classes,
        "yolo_conf": yolo_conf,
        "yolo_imgsz": yolo_imgsz,
        "job_id": job_id or sha256,
        "sha256": sha256,
        "file_name": file_name,
        "llava_pt": llava_pt_in,
    }

    def _row_by_keys(conn):
        """
        ATUALIZA SOMENTE quando:
          1) for a MESMA imagem (sha256 igual), ou
          2) houver job_id igual E o registro existente for muito recente (<= UPDATE_WINDOW_SEC).
        NUNCA corrige por 'file_name' para evitar colisões com nomes estáticos.
        """
        if not base_row["job_id"]:
            return None
        # 1) sha256 idêntico (imagem igual)
        if sha256:
            r = conn.execute(
                text("""SELECT id, timestamp FROM eventos
                        WHERE job_id=:j AND sha256=:s
                        ORDER BY id DESC LIMIT 1"""),
                {"j": base_row["job_id"], "s": sha256}
            ).first()
            if r:
                return r
        # 2) job_id igual e janela de tempo curta
        r = conn.execute(
            text("""SELECT id, timestamp FROM eventos
                    WHERE job_id=:j
                    ORDER BY id DESC LIMIT 1"""),
            {"j": base_row["job_id"]}
        ).first()
        if r:
            try:
                dt_prev = datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - dt_prev).total_seconds() <= UPDATE_WINDOW_SEC:
                    return r
            except Exception:
                pass
        return None

    with engine.begin() as conn:
        row = _row_by_keys(conn)

        if row:
            # UPDATE dos campos variáveis + timestamp
            set_parts = []
            params = {}
            for k, v in base_row.items():
                if k in ("timestamp",):
                    continue
                set_parts.append(f"{k}=:{k}")
                params[k] = v
            params["id"] = row[0]

            conn.execute(
                text("UPDATE eventos SET " + ", ".join(set_parts) + ", timestamp=:ts WHERE id=:id"),
                dict(params, ts=_now_str())
            )
            ev_id = row[0]
        else:
            r = conn.execute(eventos_tb.insert().values(**base_row))
            try:
                ev_id = r.inserted_primary_key[0]
            except Exception:
                # fallback (sqlite)
                try:
                    ev_id = conn.execute(text("SELECT last_insert_rowid()")).scalar_one()
                except Exception:
                    # postgres: pega último id pelo MAX
                    ev_id = conn.execute(text("SELECT MAX(id) FROM eventos")).scalar_one()

        prune_if_needed(conn)

    return jsonify({"ok": True, "id": int(ev_id)})

@app.route("/resposta_ia", methods=["POST"])
def receber_resposta_ia():
    dados = request.json or {}
    job_id     = _trim(dados.get("job_id"))
    ident      = _trim(dados.get("identificador"))
    camera_id  = _trim(dados.get("camera_id"))
    camera_name = _trim(dados.get("camera_name"))
    local      = _trim(dados.get("local"))
    llava_pt   = _trim(dados.get("resposta") or dados.get("llava_pt"))
    dur_ms     = _trim(str(dados.get("dur_llava_ms") or ""))
    sha256     = _trim(dados.get("sha256") or dados.get("img_hash") or dados.get("sha"))
    file_name  = _trim(dados.get("file_name"))


    with engine.begin() as conn:
        target_id = None
        if job_id:
            row = conn.execute(
                text("SELECT id FROM eventos WHERE job_id=:j ORDER BY id DESC LIMIT 1"),
                {"j": job_id}
            ).first()
            if row:
                target_id = row[0]

        if target_id is None:
            ev = {
                "timestamp": _now_str(),
                "status": "ok",
                "objeto": "Análise IA",
                "descricao": (llava_pt or "").replace("\n", "<br>"),
                "imagem": "",
                "img_url": "",
                "identificador": ident or "desconhecido",
                "camera_id": camera_id,
                "camera_name": camera_name,
                "local": local,
                "descricao_raw": "",
                "descricao_pt": "",
                "model_yolo": "",
                "classes": "",
                "yolo_conf": "",
                "yolo_imgsz": "",
                "job_id": job_id or sha256,
                "sha256": sha256,
                "file_name": file_name,
                "llava_pt": llava_pt,
                "dur_llava_ms": dur_ms,

            }
            conn.execute(eventos_tb.insert().values(**ev))
        else:
            conn.execute(
                text("""
                UPDATE eventos
                   SET llava_pt=:llp,
                       dur_llava_ms=:dur,
                       local=COALESCE(NULLIF(:loc,''), local),
                       camera_name=COALESCE(NULLIF(:cam_name,''), camera_name),
                       sha256=COALESCE(NULLIF(sha256,''), NULLIF(:sha,'')),
                       file_name=COALESCE(NULLIF(file_name,''), NULLIF(:file,''))
                 WHERE id=:id
                """),
                {
                    "llp": llava_pt,
                    "dur": dur_ms,
                    "loc": local,
                    "cam_name": camera_name,
                    "sha": sha256,
                    "file": file_name,
                    "id": target_id
                }
            )


        prune_if_needed(conn)

    return jsonify({"ok": True})
# -------------------- UI de confirmação (navegador) --------------------
CONFIRM_UI_TEMPLATE = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <title>Confirmar Violência</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root{ --bg:#f7f7f8; --card:#fff; --line:#e5e7eb; --ink:#111827; --muted:#6b7280; --ok:#16a34a; --no:#dc2626; }
    *{box-sizing:border-box}
    body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;background:var(--bg);color:var(--ink)}
    .wrap{max-width:1100px;margin:0 auto;padding:18px}
    .top{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px}
    .top a{color:#2563eb;text-decoration:none}
    .card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px}
    .grid{display:grid;grid-template-columns:420px 1fr;gap:16px}
    @media(max-width:980px){.grid{grid-template-columns:1fr}}
    img{width:100%;border-radius:12px;border:1px solid var(--line);object-fit:cover;background:#f3f4f6}
    .kv{margin:6px 0;font-size:14px}
    .kv b{display:inline-block;min-width:160px;color:#374151}
    textarea,input{width:100%;border:1px solid var(--line);border-radius:12px;padding:10px;font-size:14px;background:#fff}
    textarea{min-height:120px;resize:vertical}
    .row{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-top:12px}
    .btn{border:0;border-radius:12px;padding:10px 14px;font-weight:700;cursor:pointer}
    .ok{background:var(--ok);color:#fff}
    .no{background:var(--no);color:#fff}
    .muted{color:var(--muted);font-size:12px}
    hr{border:none;border-top:1px solid var(--line);margin:12px 0}
    .badge{display:inline-block;font-size:12px;padding:3px 10px;border-radius:999px;border:1px solid var(--line);background:#fff;color:#374151;margin-left:8px}
  

.qualBox{
  margin-top:14px;
  border:1px solid #e5e7eb;
  border-radius:14px;
  padding:12px;
  background:#fff;
}
.qualTitle{
  font-weight:700;
  margin-bottom:8px;
}
.qualGrid{
  display:grid;
  grid-template-columns: 1fr 1fr;
  gap:6px 12px;
  align-items:start;
  max-height: 340px;
  overflow:auto;
  padding-right:4px;
}
.qitem{
  display:flex;
  gap:8px;
  align-items:flex-start;
  font-size:14px;
  line-height:1.2;
}
.qitem input{ margin-top:2px; }

</style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h2 style="margin:0">Confirmar caso real de Violência</h2>
      <a href="{{ next_url }}">Voltar</a>
    </div>

    <div class="card">
      
      <div class="grid">
        <div>
          {% if ev.tem_img %}
            <img src="{{ img_src }}" alt="Imagem do evento">
          {% else %}
            <div class="muted">Evento sem imagem salva.</div>
          {% endif %}

<div class="qualBox">
  <div class="qualTitle">Qualificação do Incidente</div>
  <div class="qualGrid">
    {% for q in qualificacoes %}
      <label class="qitem">
        <input type="checkbox" name="qualificacoes" value="{{q.id}}" {% if q.id in qual_sel %}checked{% endif %}>
        <span>{{q.nome}}</span>
      </label>
    {% endfor %}
  </div>
</div>

        </div>
        <div>
          <div class="kv"><b>ID:</b> {{ ev.id }}</div>
          <div class="kv"><b>Timestamp:</b> {{ ev.timestamp }}</div>
          <div class="kv"><b>Status:</b> {{ ev.status }} {% if ev.status %}<span class="badge">{{ ev.status }}</span>{% endif %}</div>
          <div class="kv"><b>Identificador:</b> {{ ev.identificador }}</div>
          <div class="kv"><b>Câmera:</b> {{ ev.camera_id or '-' }}{% if ev.camera_name %} – {{ ev.camera_name }}{% endif %}</div>
          <div class="kv"><b>Local:</b> {{ ev.local or '-' }}</div>
          <div class="kv"><b>Objeto:</b> {{ ev.objeto or '-' }}</div>
          <hr>
          <div class="kv"><b>Analise objeto:</b></div>
          <div style="white-space:pre-wrap;line-height:1.35">{{ ev.descricao_plain }}</div>

          {% if ev.llava_pt %}
            <hr>
            <div class="kv"><b>Diagnóstico:</b></div>
            <div style="white-space:pre-wrap;line-height:1.35">{{ ev.llava_pt }}</div>
          {% endif %}

          <hr>

          <form method="post">
            <input type="hidden" name="key" value="{{ key_value }}">
            <input type="hidden" name="id" value="{{ ev.id }}">
            <input type="hidden" name="next" value="{{ next_url }}">

            <div class="kv"><b>Operador:</b></div>
            <input name="operador" placeholder="ex.: op1" value="">

            <div style="height:10px"></div>
            <div class="kv"><b>Relato do operador:</b></div>
            <textarea name="relato" placeholder="Descreva o que foi visto na imagem..."></textarea>

            <div class="row">
              <button class="btn ok" type="submit" name="action" value="confirmar">Confirmar</button>
              <button class="btn no" type="submit" name="action" value="desconfirmar">Desfazer</button>
              <span class="muted">O relato é obrigatório para confirmar.</span>
            </div>
          </form>

        </div>
      </div>
    </div>
  </div>
</body>
</html>
"Descreva o que foi visto na imagem..."></textarea>

            <div class="row">
              <button class="btn ok" type="submit" name="action" value="confirmar">Confirmar</button>
              <button class="btn no" type="submit" name="action" value="desconfirmar">Desfazer</button>
              <span class="muted">O relato é obrigatório para confirmar.</span>
            </div>
          </form>

        </div>
      </div>
    </div>
  </div>
</body>
</html>
"""

def _load_event_by_id(ev_id: int):
    with engine.begin() as conn:
        r = conn.execute(text("""
            SELECT id, timestamp, status, objeto, descricao, identificador,
                   CASE
                     WHEN (imagem IS NULL OR imagem = '') AND COALESCE(img_url,'') = '' THEN 0
                     ELSE 1
                   END AS tem_img,
                   COALESCE(img_url,'') AS img_url,
                   COALESCE(camera_id,''), COALESCE(camera_name,''), COALESCE(local,''),
                   COALESCE(llava_pt,'')
            FROM eventos
            WHERE id=:id
        """), {"id": ev_id}).first()
    if not r:
        return None

    desc_html = (r[4] or "")
    desc_plain = desc_html.replace("<br>", "\n")

    return {
        "id": r[0],
        "timestamp": r[1] or "",
        "status": r[2] or "",
        "objeto": r[3] or "",
        "descricao_plain": desc_plain.strip(),
        "identificador": r[5] or "",
        "tem_img": bool(r[6]),
        "img_url": r[7] or "",
        "camera_id": r[8] or "",
        "camera_name": r[9] or "",
        "local": r[10] or "",
        "llava_pt": r[11] or "",
    }



def _load_event_by_ident(ident: str):
    if not ident:
        return None
    with engine.begin() as conn:
        r = conn.execute(text("""
            SELECT id
            FROM eventos
            WHERE identificador=:ident
            ORDER BY id DESC
            LIMIT 1
        """), {"ident": ident}).first()
    if not r:
        return None
    return _load_event_by_id(int(r[0]))

def _load_event_by_sha(sha: str):
    sha = _trim(sha)
    if not sha:
        return None
    with engine.begin() as conn:
        r = conn.execute(text("""
            SELECT id
            FROM eventos
            WHERE sha256 = :sha
            ORDER BY id DESC
            LIMIT 1
        """), {"sha": sha}).first()
    if not r:
        return None
    return _load_event_by_id(int(r[0]))

def _try_attach_sha_to_recent_events(sha: str, limit: int = 400):
    """
    Quando vem um sha do Loki, mas o registro "real" no Postgres ainda não tem sha256 preenchido,
    tentamos localizar um evento recente (com imagem base64) cujo hash bata, e então gravamos sha256 nele.
    Retorna o ID do evento encontrado, ou None.
    """
    sha = _trim(sha)
    if not sha:
        return None

    try:
        limit = int(limit)
    except Exception:
        limit = 400

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, imagem
              FROM eventos
             WHERE (sha256 IS NULL OR sha256 = '')
               AND (imagem IS NOT NULL AND imagem <> '')
             ORDER BY id DESC
             LIMIT :lim
        """), {"lim": limit}).all()

        for ev_id, img_b64 in rows:
            try:
                calc = _sha1_from_b64_image(img_b64)
                if calc == sha:
                    conn.execute(text("""
                        UPDATE eventos
                           SET sha256 = :sha
                         WHERE id = :id
                           AND (sha256 IS NULL OR sha256 = '')
                    """), {"sha": sha, "id": int(ev_id)})
                    return int(ev_id)
            except Exception:
                # ignora linhas inválidas e continua
                pass

    return None



def _infer_meta_from_url(url_img: str):
    """Extrai camera_id/camera_name e timestamp do nome do arquivo na URL.
    Padrões suportados:
      - .../cam1_YYYYMMDD_HHMMSS_123.jpg  -> camera_id=1
      - .../C%C3%A2mera%20Especial_YYYYMMDD_HHMMSS_123.jpg -> camera_name="Câmera Especial"
    Retorna (camera_id, camera_name, ts_db_str) ou ("","","") se falhar.
    """
    url_img = _trim(url_img)
    if not url_img:
        return ("", "", "")

    try:
        from urllib.parse import urlparse, unquote
        p = urlparse(url_img)
        base = unquote((p.path or "").split("/")[-1])
    except Exception:
        base = url_img.split("/")[-1]

    base = _trim(base)
    if not base:
        return ("", "", "")

    name = base.rsplit(".", 1)[0]  # sem extensão

    m = re.search(r"_(\d{8})_(\d{6})_", name)
    if not m:
        return ("", "", "")

    ymd, hms = m.group(1), m.group(2)
    try:
        ts = datetime.strptime(ymd + hms, "%Y%m%d%H%M%S")
        ts_db = ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        ts_db = ""

    prefix = _trim(name[:m.start()])  # parte antes do _YYYYMMDD...

    camera_id = ""
    camera_name = ""

    m2 = re.match(r"^cam(\d+)$", prefix, flags=re.IGNORECASE)
    if m2:
        camera_id = m2.group(1)
    else:
        camera_name = prefix

    return (camera_id, camera_name, ts_db)


def _try_attach_sha_by_urlmeta(sha: str, url_img: str, window_seconds: int = 8):
    """Tenta localizar um evento já existente no Postgres (sem sha256) que corresponda à URL,
    e grava sha256 nele. Evita criar placeholder com novo ID.
    """
    sha = _trim(sha)
    url_img = _trim(url_img)
    if not sha or not url_img:
        return None

    cam_id, cam_name, ts_db = _infer_meta_from_url(url_img)
    if not ts_db or (not cam_id and not cam_name):
        return None

    try:
        center = datetime.strptime(ts_db, "%Y-%m-%d %H:%M:%S")
        t0 = (center - timedelta(seconds=window_seconds)).strftime("%Y-%m-%d %H:%M:%S")
        t1 = (center + timedelta(seconds=window_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

    with engine.begin() as conn:
        row = None

        if cam_id:
            row = conn.execute(text("""
                SELECT id
                  FROM eventos
                 WHERE COALESCE(sha256,'') = ''
                   AND timestamp BETWEEN :t0 AND :t1
                   AND camera_id = :cam_id
                 ORDER BY id DESC
                 LIMIT 1
            """), {"t0": t0, "t1": t1, "cam_id": cam_id}).first()

        if not row and cam_name:
            row = conn.execute(text("""
                SELECT id
                  FROM eventos
                 WHERE COALESCE(sha256,'') = ''
                   AND timestamp BETWEEN :t0 AND :t1
                   AND camera_name = :cam_name
                 ORDER BY id DESC
                 LIMIT 1
            """), {"t0": t0, "t1": t1, "cam_name": cam_name}).first()

        if not row:
            return None

        ev_id = int(row[0])

        conn.execute(text("""
            UPDATE eventos
               SET sha256 = :sha,
                   img_url = COALESCE(NULLIF(img_url,''), :url)
             WHERE id = :id
               AND COALESCE(sha256,'') = ''
        """), {"sha": sha, "url": url_img, "id": ev_id})

        return ev_id


def _parse_ts_any(s: str) -> str:
    s = _trim(s)
    if not s:
        return _now_str()
    # aceita ISO: 2026-01-02T19:57:30.855Z
    try:
        if "T" in s:
            s2 = s.replace("Z", "").split(".", 1)[0]
            dt = datetime.fromisoformat(s2)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    # aceita já no formato do BD
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return _now_str()


def _ensure_event_from_sha_query(sha: str):
    """
    Se o sha não existir no Postgres, cria um evento mínimo com dados vindos da querystring,
    para permitir a confirmação via UI.
    Espera pelo menos: sha e (idealmente) url.
    """
    sha = _trim(sha)
    if not sha:
        return None

    # Já existe?
    ev = _load_event_by_sha(sha)
    if ev:
        return ev

    # Dados vindos do Grafana (adicione na URL do botão)
    img_url   = _trim(request.args.get("url") or request.args.get("img") or "")
    camera_id = _trim(request.args.get("camera_id") or request.args.get("camera") or "")
    camera_name = _trim(request.args.get("camera_name") or request.args.get("cam_name") or "")
    local     = _trim(request.args.get("local") or "")
    objeto    = _trim(request.args.get("objeto") or "Indício (Loki)")
    llava_pt  = _trim(request.args.get("llava_pt") or request.args.get("ctx") or "")
    ident     = _trim(request.args.get("ident") or request.args.get("host") or "Loki")
    status    = _trim(request.args.get("status") or "ok")
    ts_in     = _trim(request.args.get("timestamp") or request.args.get("ts") or request.args.get("time") or "")
    ts_db     = _parse_ts_any(ts_in)

    ev_row = {
        "timestamp": ts_db,
        "status": status,
        "objeto": objeto,
        "descricao": "",          # pode ficar vazio (indício)
        "imagem": "",             # por padrão sem base64
        "img_url": img_url,       # importante para exibir na UI
        "identificador": ident,
        "camera_id": camera_id,
        "camera_name": camera_name,
        "local": local,
        "descricao_raw": "",
        "descricao_pt": "",
        "model_yolo": "",
        "classes": "",
        "yolo_conf": "",
        "yolo_imgsz": "",
        "job_id": sha,
        "sha256": sha,
        "file_name": "",
        "llava_pt": llava_pt,
        "dur_llava_ms": "",
    }

    with engine.begin() as conn:
        conn.execute(eventos_tb.insert().values(**ev_row))

        # pega o id recém criado (funciona no Postgres)
        new_id = conn.execute(text("SELECT id FROM eventos WHERE sha256=:s ORDER BY id DESC LIMIT 1"), {"s": sha}).scalar_one_or_none()

    if not new_id:
        return None
    return _load_event_by_id(int(new_id))


@app.route("/confirmar", methods=["GET", "POST"])
def confirmar_ui():
    # Proteção: exige ADMIN_KEY via ?key=... (GET) ou form-data key (POST) ou header
    if not _admin_ok():
        abort(403)

    next_url = (request.values.get("next") or "/confirmados").strip()
    key_value = (request.values.get("key") or "").strip()  # para manter no POST

    if request.method == "POST":
        ev_id = int(request.form.get("id") or 0)
        action = (request.form.get("action") or "confirmar").strip()
        relato = _trim(request.form.get("relato"))
        operador = _trim(request.form.get("operador"))

        # Qualificações selecionadas (multi-select)
        qual_ids = []
        for _v in request.form.getlist("qualificacoes"):
            try:
                qual_ids.append(int(_v))
            except Exception:
                pass

        if ev_id <= 0:
            return "ID inválido.", 400

        with engine.begin() as conn:
            # Bloqueia o registro para evitar confirmação dupla / condições de corrida
            cur = conn.execute(text("""
                SELECT id, COALESCE(confirmado,''), COALESCE(sha256,'')
                  FROM eventos
                 WHERE id=:id
                 FOR UPDATE
            """), {"id": ev_id}).first()
            if not cur:
                return "Evento não encontrado.", 404
            confirmado_cur = (cur[1] or "").strip()
            sha_cur = (cur[2] or "").strip()

            # Se já existe confirmação para este ID/SHA, não permite confirmar novamente
            if action != "desconfirmar":
                if confirmado_cur == CONFIRM_VALUE:
                    return _close_window_html(next_url, "Este evento já estava confirmado.")
                if sha_cur:
                    other_id = conn.execute(text("""
                        SELECT id
                          FROM eventos
                         WHERE sha256=:sha AND confirmado=:c AND id<>:id
                         ORDER BY id DESC
                         LIMIT 1
                    """), {"sha": sha_cur, "c": CONFIRM_VALUE, "id": ev_id}).scalar()
                    if other_id:
                        return _close_window_html(next_url, f"Este SHA já foi confirmado (ID {other_id}).")

            if action == "desconfirmar":
                conn.execute(text("""
                    UPDATE eventos
                       SET confirmado='',
                           relato_operador='',
                           confirmado_por='',
                           confirmado_em=''
                     WHERE id=:id
                """), {"id": ev_id})
                conn.execute(text("DELETE FROM evento_qualificacao WHERE evento_id=:id"), {"id": ev_id})
            else:
                if not relato:
                    return "Relato é obrigatório para confirmar.", 400

                # Preenche sha256 automaticamente ao confirmar, se estiver vazio e existir imagem base64
                row = conn.execute(
                    text("SELECT COALESCE(sha256,''), COALESCE(imagem,'') FROM eventos WHERE id=:id"),
                    {"id": ev_id}
                ).first()
                sha_atual = (row[0] or "") if row else ""
                img_atual = (row[1] or "") if row else ""

                sha_calc = ""
                if not sha_atual and img_atual:
                    sha_calc = _sha1_from_b64_image(img_atual)

                conn.execute(text("""
                    UPDATE eventos
                       SET confirmado=:c,
                           relato_operador=:r,
                           confirmado_por=:p,
                           confirmado_em=:em,
                           sha256 = COALESCE(NULLIF(sha256,''), NULLIF(:sha,''))
                     WHERE id=:id
                """), {
                    "c": CONFIRM_VALUE,
                    "r": relato,
                    "p": operador,
                    "em": _now_str(),
                    "sha": sha_calc,
                    "id": ev_id
                })

                # Atualiza relação N:N com as qualificações escolhidas
                _salvar_qualificacoes_evento(conn, ev_id, qual_ids)

                # Após confirmar/desfazer: tenta fechar a aba. Se o browser bloquear, redireciona.
        return _close_window_html(next_url)


    # GET
    ev_id = int(request.args.get("id") or 0)
    ident = _trim(request.args.get("ident"))
    sha = _trim(request.args.get("sha"))
    url_img = _trim(request.args.get("url"))  # vem do Grafana/Loki

    ev = None
    if ev_id:
        ev = _load_event_by_id(ev_id)
    elif sha:
        # 1) tenta por sha direto
        ev = _load_event_by_sha(sha)

        # 2) se não achou, tenta "grudar" esse sha em um evento real recente (com imagem base64)
        if not ev:
            attached_id = _try_attach_sha_to_recent_events(sha)
            if attached_id:
                ev = _load_event_by_id(attached_id)

        # 2.1) se ainda não achou, tenta achar um evento já gravado (sem sha256) pelo timestamp/câmera extraídos da URL
        if not ev and url_img:
            attached_id = _try_attach_sha_by_urlmeta(sha, url_img)
            if attached_id:
                ev = _load_event_by_id(attached_id)

        # 3) se ainda não achou e veio URL, cria um placeholder (somente como último recurso)
        if not ev and url_img:
            with engine.begin() as conn:
                # evita duplicar placeholder se já existir
                r = conn.execute(text("""
                    SELECT id FROM eventos
                     WHERE sha256 = :sha
                     ORDER BY id DESC
                     LIMIT 1
                """), {"sha": sha}).first()

                if not r:
                    conn.execute(text("""
                        INSERT INTO eventos (timestamp, status, objeto, descricao, imagem, img_url,
                                             identificador, camera_id, camera_name, local,
                                             descricao_raw, descricao_pt, model_yolo, classes, yolo_conf, yolo_imgsz,
                                             job_id, sha256, file_name, llava_pt)
                        VALUES (:ts, :st, :obj, :desc, :img, :img_url,
                                :ident, :cam_id, :cam_name, :loc,
                                :raw, :pt, :model, :classes, :conf, :imgsz,
                                :job, :sha, :fn, :llava)
                    """), {
                        "ts": _now_str(),
                        "st": "ok",
                        "obj": "Indício (Loki)",
                        "desc": "",
                        "img": "",
                        "img_url": url_img,
                        "ident": "Loki",
                        "cam_id": "",
                        "cam_name": "",
                        "loc": "",
                        "raw": "",
                        "pt": "",
                        "model": "",
                        "classes": "",
                        "conf": "",
                        "imgsz": "",
                        "job": sha,
                        "sha": sha,
                        "fn": "",
                        "llava": ""
                    })

            ev = _load_event_by_sha(sha)

    elif ident:
        ev = _load_event_by_ident(ident)

    if not ev:
        return "Evento não encontrado (id/sha/ident inválido).", 404

    # se tem imagem no BD, usa /img/<id>; senão, usa a URL externa (do Grafana)
    img_src = url_for("img", ev_id=ev["id"], _external=True) if ev["tem_img"] else (url_img or "")

    qualificacoes = _listar_qualificacoes()
    qual_sel = _qualificacoes_do_evento(ev["id"]) if ev and ev.get("id") else set()

    return render_template_string(
        CONFIRM_UI_TEMPLATE,
        ev=ev,
        img_src=img_src,
        next_url=next_url,
        key_value=key_value,
        qualificacoes=qualificacoes,
        qual_sel=qual_sel,
    )




# -------------------- Confirmação do operador (BD) --------------------
@app.route("/api/confirmar", methods=["POST"])
def api_confirmar():
    if not _admin_ok():
        abort(403)

    dados = request.json or {}
    ev_id = int(dados.get("id") or 0)
    relato = _trim(dados.get("relato") or dados.get("relato_operador"))
    operador = _trim(dados.get("operador") or dados.get("confirmado_por"))
    confirmado = _trim(dados.get("confirmado") or CONFIRM_VALUE) or CONFIRM_VALUE

    if ev_id <= 0 or not relato:
        return jsonify({"ok": False, "error": "Campos obrigatórios: id, relato"}), 400

    with engine.begin() as conn:
        # Bloqueia o registro para evitar confirmação dupla / condições de corrida
        cur = conn.execute(text("""
            SELECT id, COALESCE(confirmado,''), COALESCE(sha256,'')
              FROM eventos
             WHERE id=:id
             FOR UPDATE
        """), {"id": ev_id}).first()
        if not cur:
            return jsonify({"ok": False, "error": "Evento não encontrado", "id": ev_id}), 404
        confirmado_cur = (cur[1] or "").strip()
        sha_cur = (cur[2] or "").strip()

        if confirmado_cur == CONFIRM_VALUE:
            return jsonify({"ok": True, "already_confirmed": True, "id": ev_id})

        if sha_cur:
            other_id = conn.execute(text("""
                SELECT id
                  FROM eventos
                 WHERE sha256=:sha AND confirmado=:c AND id<>:id
                 ORDER BY id DESC
                 LIMIT 1
            """), {"sha": sha_cur, "c": CONFIRM_VALUE, "id": ev_id}).scalar()
            if other_id:
                return jsonify({"ok": False, "error": "SHA já confirmado em outro registro", "other_id": other_id, "id": ev_id}), 409

        # Preenche sha256 automaticamente ao confirmar, se estiver vazio e existir imagem base64
        row = conn.execute(
            text("SELECT COALESCE(sha256,''), COALESCE(imagem,'') FROM eventos WHERE id=:id"),
            {"id": ev_id}
        ).first()
        sha_atual = (row[0] or "") if row else ""
        img_atual = (row[1] or "") if row else ""

        sha_calc = ""
        if not sha_atual and img_atual:
            sha_calc = _sha1_from_b64_image(img_atual)

        conn.execute(
            text("""
                UPDATE eventos
                   SET confirmado=:c,
                       relato_operador=:r,
                       confirmado_por=:p,
                       confirmado_em=:em,
                       sha256 = COALESCE(NULLIF(sha256,''), NULLIF(:sha,''))
                 WHERE id=:id
            """),
            {
                "c": confirmado,
                "r": relato,
                "p": operador,
                "em": _now_str(),
                "sha": sha_calc,
                "id": ev_id
            }
        )
    return jsonify({"ok": True})

@app.route("/api/desconfirmar", methods=["POST"])
def api_desconfirmar():
    if not _admin_ok():
        abort(403)

    dados = request.json or {}
    ev_id = int(dados.get("id") or 0)
    if ev_id <= 0:
        return jsonify({"ok": False, "error": "Campo obrigatório: id"}), 400

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE eventos
                   SET confirmado='',
                       relato_operador='',
                       confirmado_por='',
                       confirmado_em=''
                 WHERE id=:id
            """),
            {"id": ev_id}
        )
    return jsonify({"ok": True})

# -------------------- Painéis Flask (BD) --------------------
@app.route("/historico")
def historico():
    filtro = (request.args.get("filtro") or "").strip()
    data = (request.args.get("data") or "").strip()
    page = max(int(request.args.get("page") or 1), 1)
    evs = buscar_eventos(filtro if filtro else None, data if data else None,
                         status=None, confirmado=None, limit=50, offset=(page-1)*50)
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
        confirmado=None,
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

@app.route("/indicios")
def indicios():
    filtro = (request.args.get("filtro") or "").strip()
    data = (request.args.get("data") or "").strip()
    page = max(int(request.args.get("page") or 1), 1)

    evs = buscar_eventos(
        filtro=filtro if filtro else None,
        data=data if data else None,
        status=None,
        confirmado="NAO",
        limit=50,
        offset=(page-1)*50
    )

    return render_template_string(
        HTML_TEMPLATE,
        page_title="Indícios de Violência",
        eventos=evs,
        filtro=filtro,
        data=data,
        page=page,
        logo_url=_logo_url(),
        iaprotect_url=_iaprotect_url()
    )

@app.route("/confirmados")
def confirmados():
    filtro = (request.args.get("filtro") or "").strip()
    data = (request.args.get("data") or "").strip()
    page = max(int(request.args.get("page") or 1), 1)

    evs = buscar_eventos(
        filtro=filtro if filtro else None,
        data=data if data else None,
        status=None,
        confirmado="SIM",
        limit=50,
        offset=(page-1)*50
    )

    return render_template_string(
        HTML_TEMPLATE,
        page_title="Violência Confirmada",
        eventos=evs,
        filtro=filtro,
        data=data,
        page=page,
        logo_url=_logo_url(),
        iaprotect_url=_iaprotect_url()
    )

# -------------------- APIs p/ Grafana --------------------
@app.route("/api/events")
def api_events():
    since = (request.args.get("since") or "").strip()
    limit = int(request.args.get("limit") or 200)
    camera_id = (request.args.get("camera_id") or "").strip()
    local = (request.args.get("local") or "").strip()
    status = (request.args.get("status") or "").strip()
    confirmado = (request.args.get("confirmado") or "").strip().upper()  # SIM/NAO/''

    clauses = ["1=1"]
    params = {}

    if since:
        if len(since) == 10 and since.count("-") == 2:
            since = since + " 00:00:00"
        clauses.append("timestamp >= :since")
        params["since"] = since

    if camera_id:
        clauses.append("camera_id = :cid")
        params["cid"] = camera_id

    if local:
        clauses.append("local LIKE :loc")
        params["loc"] = f"%{local}%"

    if status:
        clauses.append("status = :st")
        params["st"] = status

    if confirmado == "SIM":
        clauses.append("confirmado = :cf")
        params["cf"] = CONFIRM_VALUE
    elif confirmado == "NAO":
        clauses.append("(confirmado IS NULL OR confirmado = '' OR confirmado <> :cf2)")
        params["cf2"] = CONFIRM_VALUE

    sql = f"""
    SELECT id, timestamp, status, identificador, camera_id, camera_name, local, objeto,
           descricao, COALESCE(descricao_raw,''), COALESCE(descricao_pt,''),
           COALESCE(model_yolo,''), COALESCE(classes,''), COALESCE(yolo_conf,''), COALESCE(yolo_imgsz,''),
           COALESCE(llava_pt,''),
           COALESCE(confirmado,''), COALESCE(relato_operador,''), COALESCE(confirmado_por,''), COALESCE(confirmado_em,''),
           CASE WHEN imagem IS NULL OR imagem = '' THEN 0 ELSE 1 END AS has_img
      FROM eventos
     WHERE {" AND ".join(clauses)}
     ORDER BY id DESC
     LIMIT :lim
    """
    params["lim"] = limit

    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).all()

    out = []
    for r in rows:
        out.append({
            "id": r[0],
            "timestamp": r[1],
            "status": r[2],
            "identificador": r[3],
            "camera_id": r[4] or "",
            "camera_name": r[5] or "",
            "local": r[6] or "",
            "objeto": r[7] or "",
            "descricao": r[8] or "",
            "descricao_raw": r[9] or "",
            "descricao_pt": r[10] or "",
            "model_yolo": r[11] or "",
            "classes": r[12] or "",
            "yolo_conf": r[13] or "",
            "yolo_imgsz": r[14] or "",
            "llava_pt": r[15] or "",
            "confirmado": r[16] or "",
            "relato_operador": r[17] or "",
            "confirmado_por": r[18] or "",
            "confirmado_em": r[19] or "",
            "has_img": bool(r[20]),
            "image_url": url_for("img", ev_id=r[0], _external=True) if r[20] else ""
        })
    return jsonify(out)

@app.route("/api/stats")
def api_stats():
    now = datetime.now()
    rng = (request.args.get("range") or "h24").lower()

    if BACKEND == "sqlite":
        if rng == "d7":
            sql = """
            SELECT substr(timestamp,1,10) AS dia,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status='alerta' THEN 1 ELSE 0 END) AS alertas
            FROM eventos
            WHERE timestamp >= :since
            GROUP BY dia
            ORDER BY dia ASC
            """
            since = (now - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
        else:
            sql = """
            SELECT substr(timestamp,1,13) AS hora,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status='alerta' THEN 1 ELSE 0 END) AS alertas
            FROM eventos
            WHERE timestamp >= :since
            GROUP BY hora
            ORDER BY hora ASC
            """
            since = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        with engine.begin() as conn:
            rows = conn.execute(text(sql), {"since": since}).all()
        data = [{"bucket": r[0], "total": int(r[1] or 0), "alertas": int(r[2] or 0)} for r in rows]
        return jsonify({"range": rng, "series": data})
    else:
        if rng == "d7":
            since = now - timedelta(days=7)
            bucket = lambda ts: ts[:10]
        else:
            since = now - timedelta(hours=24)
            bucket = lambda ts: ts[:13]

        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT timestamp, status FROM eventos WHERE timestamp >= :s"),
                {"s": since.strftime("%Y-%m-%d %H:%M:%S")}
            ).all()
        agg = {}
        for ts, st in rows:
            key = bucket(ts)
            a = agg.get(key, {"total": 0, "alertas": 0})
            a["total"] += 1
            if st == "alerta":
                a["alertas"] += 1
            agg[key] = a
        series = [{"bucket": k, "total": v["total"], "alertas": v["alertas"]} for k, v in sorted(agg.items())]
        return jsonify({"range": rng, "series": series})

# -------------------- Poda automática --------------------
def prune_if_needed(conn):
    removed_total = 0

    if BACKEND == "sqlite" and DB_PATH:
        try:
            st = os.statvfs(os.path.abspath(DB_PATH))
            total = st.f_frsize * st.f_blocks
            used  = total - (st.f_frsize * st.f_bavail)
            uso = used / total if total else 0.0
        except Exception:
            uso = 0.0

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
            try:
                conn.execute(text("VACUUM"))
                conn.commit()
            except Exception:
                pass
            try:
                st = os.statvfs(os.path.abspath(DB_PATH))
                total = st.f_frsize * st.f_blocks
                used  = total - (st.f_frsize * st.f_bavail)
                uso = used / total if total else 0.0
            except Exception:
                break
            removed_total += removed
    else:
        # Postgres/Render: controla por quantidade de linhas
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
