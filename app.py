import os
import base64
from io import BytesIO
from datetime import datetime, timedelta

from flask import Flask, request, jsonify, render_template_string, url_for, send_file, abort
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text
from sqlalchemy.sql import text

DB_URL = os.getenv("DATABASE_URL", "sqlite:///eventos.db")
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
md = MetaData()
eventos_tb = Table(
    "eventos", md,
    Column("id", Integer, primary_key=True),
    Column("timestamp", Text),
    Column("status", Text),
    Column("objeto", Text),
    Column("descricao", Text),
    Column("imagem", Text),           # base64 (ideal é mover para armazenamento externo depois)
    Column("identificador", Text),
)

def init_db():
    md.create_all(engine)
    os.makedirs("static", exist_ok=True)

def salvar_evento(ev: dict):
    with engine.begin() as conn:
        conn.execute(eventos_tb.insert().values(**ev))

def buscar_eventos(filtro=None, data=None, status=None, limit=50, offset=0):
    # NÃO traz a coluna imagem. Calcula apenas se existe.
    sql = [
        "SELECT id, timestamp, status, objeto, descricao, identificador,",
        "CASE WHEN imagem IS NULL OR imagem = '' THEN 0 ELSE 1 END AS tem_img",
        "FROM eventos WHERE 1=1",
    ]
    params = {}

    if filtro:
        termos = [t.strip() for t in filtro.replace(",", " ").split() if t.strip()]
        if termos:
            or_parts = []
            for i, t in enumerate(termos):
                k = f"q{i}"
                like = f"%{t.lower()}%"
                or_parts.append(f"(LOWER(objeto) LIKE :{k} OR LOWER(descricao) LIKE :{k} OR LOWER(identificador) LIKE :{k})")
                params[k] = like
            sql.append("AND (" + " OR ".join(or_parts) + ")")

    if data:
        sql.append("AND DATE(timestamp) = :d")
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
            descricao=r[4], identificador=r[5], tem_img=bool(r[6])
        ))
    return evs

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Eventos Recebidos</title>
  <script>
    setInterval(() => {
      const t = document.activeElement && document.activeElement.tagName;
      if (!['INPUT','TEXTAREA','SELECT'].includes(t)) location.reload();
    }, 20000);
  </script>
  <style>
    body { font-family: Arial, sans-serif; margin:0; background:#f4f4f4; }
    header { background:#fff; display:flex; align-items:center; justify-content:space-between;
             padding:10px 16px; box-shadow:0 1px 3px rgba(0,0,0,.08); }
    .head-left { display:flex; align-items:center; gap:16px; }
    .logo-rowau { height:38px; }
    .logo-iaprotect { height:32px; }
    h1 { margin:0; font-size:28px; }
    .wrap { padding:20px 32px; }
    form { margin-bottom: 12px; }
    .evento { background:#fff; padding:15px; margin:10px 0; border-left:5px solid #007bff; }
    .alerta { border-color:red; }
    img.ev { max-width:400px; margin-top:10px; border:1px solid #ccc; }
    .pager { margin-top:12px; }
    .pager a { margin-right:10px; }
  </style>
</head>
<body>
  <header>
    <div class="head-left">
      <img src="{{ logo_url }}" class="logo-rowau" alt="Rowau">
      <h1>📡 Eventos Recebidos</h1>
    </div>
    <img src="{{ iaprotect_url }}" class="logo-iaprotect" alt="IAprotect">
  </header>

  <div class="wrap">
    <form method="get">
      <input type="text" name="filtro" placeholder="Palavra-chave" value="{{ filtro }}">
      <input type="date" name="data" value="{{ data }}">
      <button type="submit">Buscar</button>
    </form>

    {% for e in eventos %}
      <div class="evento {% if e.status == 'alerta' %}alerta{% endif %}">
        <strong>{{ e.timestamp }}</strong><br>
        <strong>Status:</strong> {{ e.status }}<br>
        <strong>Identificador:</strong> {{ e.identificador }}<br>
        <strong>Objeto:</strong> {{ e.objeto }}<br>
        <strong>Descrição:</strong> {{ e.descricao|safe }}<br>
        {% if e.tem_img %}
          <strong>Imagem:</strong><br>
          <img class="ev" src="{{ url_for('img', ev_id=e.id) }}" loading="lazy">
        {% endif %}
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

app = Flask(__name__)

# ----- logos e fallbacks -----
_TRANSPARENT_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAASsJTYQAAAAASUVORK5CYII="

@app.route("/logo-fallback.png")
def logo_fallback():
    img = base64.b64decode(_TRANSPARENT_PNG_B64)
    return send_file(BytesIO(img), mimetype="image/png", cache_timeout=86400)

@app.route("/logo-uploaded.png")
def logo_uploaded():
    path = "Logo Rowau Preto.png"
    if os.path.exists(path):
        return send_file(path, mimetype="image/png", cache_timeout=86400)
    img = base64.b64decode(_TRANSPARENT_PNG_B64)
    return send_file(BytesIO(img), mimetype="image/png", cache_timeout=86400)

@app.route("/iaprotect-uploaded.png")
def iaprotect_uploaded():
    path = "IAprotect.png"
    if os.path.exists(path):
        return send_file(path, mimetype="image/png", cache_timeout=86400)
    img = base64.b64decode(_TRANSPARENT_PNG_B64)
    return send_file(BytesIO(img), mimetype="image/png", cache_timeout=86400)

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
    # não aplicar no cache das imagens estáticas
    if request.path.startswith(("/logo-", "/img/", "/static/")):
        return resp
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

# ----- rota para servir imagem de um evento -----
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
    return send_file(BytesIO(b), mimetype="image/jpeg", cache_timeout=3600)

# ----- rotas principais -----
@app.route("/")
def index():
    return "Online. POST /evento | POST /resposta_ia | GET /historico | GET /alertas"

@app.route("/evento", methods=["POST"])
def receber_evento():
    dados = request.json or {}
    evento = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "alerta" if dados.get("detected") else "ok",
        "objeto": dados.get("object", ""),
        "descricao": (dados.get("description", "") or "").replace("\n", "<br>"),
        "imagem": dados.get("image", ""),
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
        "imagem": None,
        "identificador": dados.get("identificador", "desconhecido"),
    }
    salvar_evento(evento)
    return jsonify({"ok": True})

@app.route("/historico")
def historico():
    filtro = (request.args.get("filtro") or "").strip()
    data = (request.args.get("data") or "").strip()
    page = int(request.args.get("page") or 1)
    page = max(page, 1)
    evs = buscar_eventos(filtro if filtro else None, data if data else None, status=None, limit=50, offset=(page-1)*50)
    return render_template_string(
        HTML_TEMPLATE,
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
    page = int(request.args.get("page") or 1)
    page = max(page, 1)

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
        try:
            ts = datetime.strptime(e["timestamp"], "%Y-%m-%d %H:%M:%S")
            if ts >= limiar:
                recentes.append(e)
        except Exception:
            pass

    return render_template_string(
        HTML_TEMPLATE,
        eventos=recentes,
        filtro=raw,
        data=data,
        page=page,
        logo_url=_logo_url(),
        iaprotect_url=_iaprotect_url()
    )

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=10000)




