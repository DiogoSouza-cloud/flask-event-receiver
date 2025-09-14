import os
from flask import Flask, request, jsonify, render_template_string
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text
from sqlalchemy.sql import text

app = Flask(__name__)

# DB: Postgres no Render via env; SQLite local como fallback
DB_URL = os.getenv("DATABASE_URL", "sqlite:///eventos.db")
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
md = MetaData()
eventos_tb = Table(
    "eventos", md,
    Column("id", Integer, primary_key=True),
    Column("timestamp", Text),        # ISO string
    Column("status", Text),
    Column("objeto", Text),
    Column("descricao", Text),        # já com <br>
    Column("imagem", Text),           # base64 opcional
    Column("identificador", Text),    # cliente/câmera
)

def init_db():
    md.create_all(engine)

def salvar_evento(ev: dict):
    with engine.begin() as conn:
        conn.execute(eventos_tb.insert().values(**ev))

def buscar_eventos(filtro=None, data=None):
    sql = """
    SELECT timestamp, status, objeto, descricao, imagem, identificador
    FROM eventos
    WHERE 1=1
    """
    params = {}
    if filtro:
        sql += " AND (LOWER(objeto) LIKE :q OR LOWER(descricao) LIKE :q OR LOWER(identificador) LIKE :q)"
        params["q"] = f"%{filtro.lower()}%"
    if data:
        sql += " AND DATE(timestamp) = :d"
        params["d"] = data  # formato YYYY-MM-DD
    sql += " ORDER BY id DESC LIMIT 200"
    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).all()
    return [dict(timestamp=r[0], status=r[1], objeto=r[2],
                 descricao=r[3], imagem=r[4], identificador=r[5]) for r in rows]

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Eventos Recebidos</title>
  <style>
    body { font-family: Arial; margin: 40px; background: #f4f4f4; }
    .evento { background: #fff; padding: 15px; margin: 10px 0; border-left: 5px solid #007bff; }
    .alerta { border-color: red; }
    img { max-width: 400px; margin-top: 10px; border: 1px solid #ccc; }
  </style>
</head>
<body>
  <h1>📡 Eventos Recebidos</h1>
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
        <img src="data:image/jpeg;base64,{{ e.imagem }}">
      {% endif %}
    </div>
  {% else %}
    <p>Nenhum evento encontrado.</p>
  {% endfor %}
</body>
</html>
"""

@app.route("/")
def index():
    return "Online. POST /evento | POST /resposta_ia | GET /historico?filtro=&data=YYYY-MM-DD"

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
    return render_template_string(HTML_TEMPLATE, eventos=evs, filtro=filtro, data=data)

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=10000)


