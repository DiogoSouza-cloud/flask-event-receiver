import os
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template_string, url_for, send_file
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text
from sqlalchemy.sql import text
from io import BytesIO
import base64

# --- Config DB (Postgres no Render via env; SQLite local como fallback) ---
DB_URL = os.getenv("DATABASE_URL", "sqlite:///eventos.db")
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
md = MetaData()
eventos_tb = Table(
    "eventos", md,
    Column("id", Integer, primary_key=True),
    Column("timestamp", Text),        # ISO string
    Column("status", Text),
    Column("objeto", Text),
    Column("descricao", Text),        # com <br>
    Column("imagem", Text),           # base64 opcional
    Column("identificador", Text),    # cliente/câmera
)

def init_db():
    md.create_all(engine)
    os.makedirs("static", exist_ok=True)  # garante pasta para o logo

def salvar_evento(ev: dict):
    with engine.begin() as conn:
        conn.execute(eventos_tb.insert().values(**ev))

def buscar_eventos(filtro=None, data=None, status=None, limit=200):
    from sqlalchemy.sql import text

    sql = ["SELECT timestamp, status, objeto, descricao, imagem, identificador FROM eventos WHERE 1=1"]
    params = {}

    # múltiplas palavras -> OR (qualquer termo casa)
    if filtro:
        termos = [t.strip() for t in filtro.split() if t.strip()]
        if termos:
            bloco_or = []
            for i, t in enumerate(termos):
                k = f"q{i}"
                bloco_or.append(f"(LOWER(objeto) LIKE :{k} OR LOWER(descricao) LIKE :{k} OR LOWER(identificador) LIKE :{k})")
                params[k] = f"%{t.lower()}%"
            sql.append("AND (" + " OR ".join(bloco_or) + ")")

    if data:
        sql.append("AND DATE(timestamp) = :d")
        params["d"] = data  # YYYY-MM-DD

    if status:
        sql.append("AND status = :s")
        params["s"] = status

    sql.append("ORDER BY id DESC LIMIT :lim")
    params["lim"] = limit

    with engine.begin() as conn:
        rows = conn.execute(text(" ".join(sql)), params).all()

    return [dict(timestamp=r[0], status=r[1], objeto=r[2],
                 descricao=r[3], imagem=r[4], identificador=r[5]) for r in rows]



# --- HTML ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Eventos Recebidos</title>
  <style>
    body { font-family: Arial, sans-serif; margin:0; background:#f4f4f4; }
    header { background:#fff; display:flex; align-items:center; gap:16px; padding:10px 16px; box-shadow:0 1px 3px rgba(0,0,0,.08); }
    header img { height:48px; }
    .wrap { padding:24px 40px; }
    form { margin-bottom: 16px; }
    .evento { background:#fff; padding:15px; margin:10px 0; border-left:5px solid #007bff; }
    .alerta { border-color:red; }
    img.ev { max-width:400px; margin-top:10px; border:1px solid #ccc; }
  </style>
</head>
<body>
  <header>
    <img src="{{ logo_url }}" alt="Rowau">
    <h1 style="margin:0;">📡 Eventos Recebidos</h1>
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
        {% if e.imagem %}
          <strong>Imagem:</strong><br>
          <img class="ev" src="data:image/jpeg;base64,{{ e.imagem }}">
        {% endif %}
      </div>
    {% else %}
      <p>Nenhum evento encontrado.</p>
    {% endfor %}
  </div>
</body>
</html>
"""

app = Flask(__name__)

# --- Fallback do logo: PNG transparente 1x1 ---
_TRANSPARENT_PNG_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMA"
    "ASsJTYQAAAAASUVORK5CYII="
)

@app.route("/logo-fallback.png")
def logo_fallback():
    img = base64.b64decode(_TRANSPARENT_PNG_B64)
    return send_file(BytesIO(img), mimetype="image/png")

def _logo_url():
    # 1) se existir em static
    if os.path.exists(os.path.join("static", "logo_rowau.png")):
        return url_for('static', filename='logo_rowau.png')
    # 2) se existir no root com o nome que você enviou
    if os.path.exists("Logo Rowau Preto.png"):
        return url_for('logo_uploaded')
    # 3) fallback transparente
    return url_for('logo_fallback')


# --- Rotas ---
@app.route("/")
def index():
    return "Online. POST /evento | POST /resposta_ia | GET /historico | GET /alertas"

@app.route("/logo-uploaded.png")
def logo_uploaded():
    # arquivo que você subiu: "Logo Rowau Preto.png"
    path = "Logo Rowau Preto.png"
    if os.path.exists(path):
        return send_file(path, mimetype="image/png")
    return send_file(BytesIO(base64.b64decode(_TRANSPARENT_PNG_B64)), mimetype="image/png")


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
    data = (request.args.get("data") or "").strip()  # YYYY-MM-DD
    evs = buscar_eventos(filtro if filtro else None, data if data else None)
    return render_template_string(
    HTML_TEMPLATE,
    eventos=evs,
    filtro=filtro,
    data=data,
    logo_url=_logo_url()
    )


@app.route("/alertas")
def alertas():
    import re
    raw = (request.args.get("filtro") or "").strip()
    data = (request.args.get("data") or "").strip()

    # tokens por espaço ou vírgula
    tokens = [t.lower() for t in re.split(r"[,\s]+", raw) if t.strip()]

    # mapear presets
    status_tokens = {t for t in tokens if t in ("sim", "perigo")}
    if status_tokens == {"sim"}:
        status = "ok"
    elif status_tokens == {"perigo"}:
        status = "alerta"
    else:
        # nenhum ou ambos -> sem filtro de status
        status = None

    # termos de texto restantes (OR dentro de buscar_eventos)
    termos = [t for t in tokens if t not in ("sim", "perigo")]
    filtro_texto = " ".join(termos) if termos else None

    evs = buscar_eventos(
        filtro=filtro_texto,
        data=data if data else None,
        status=status,
        limit=500
    )

    # manter janela de 60min só quando for alerta/perigo selecionado
    if status == "alerta" or ("perigo" in status_tokens and "sim" not in status_tokens):
        limiar = datetime.now() - timedelta(minutes=60)
        kept = []
        for e in evs:
            try:
                ts = datetime.strptime(e["timestamp"], "%Y-%m-%d %H:%M:%S")
                if ts >= limiar:
                    kept.append(e)
            except Exception:
                pass
        evs = kept

    return render_template_string(
        HTML_TEMPLATE,
        eventos=evs,
        filtro=raw,
        data=data,
        logo_url=_logo_url()
    )




# --- Main ---
if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=10000)
