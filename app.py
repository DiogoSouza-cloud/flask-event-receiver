from flask import Flask, request, jsonify, render_template_string
from datetime import datetime
import mysql.connector

DB_CONFIG = {
    "host": "107.161.183.117",
    "port": 3306,
    "user": "vtigerbr_eventos",
    "password": "M@landr0",
    "database": "vtigerbr_eventos"
}

app = Flask(__name__)

def init_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eventos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME,
            status VARCHAR(20),
            objeto VARCHAR(100),
            descricao TEXT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

init_db()

def salvar_evento(evento):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO eventos (timestamp, status, objeto, descricao)
        VALUES (%s, %s, %s, %s)
    """, (evento["timestamp"], evento["status"], evento["objeto"], evento["descricao"]))
    conn.commit()
    cursor.close()
    conn.close()

def buscar_eventos(filtro=None, data=None):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM eventos"
    condicoes = []
    valores = []
    if filtro:
        condicoes.append("(LOWER(objeto) LIKE %s OR LOWER(descricao) LIKE %s)")
        valores += [f"%{filtro.lower()}%"] * 2
    if data:
        condicoes.append("DATE(timestamp) = %s")
        valores.append(data)
    if condicoes:
        query += " WHERE " + " AND ".join(condicoes)
    query += " ORDER BY timestamp DESC LIMIT 100"
    cursor.execute(query, valores)
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return resultados

HTML_TEMPLATE = '''<!DOCTYPE html>
<html>
<head>
    <title>Eventos Recebidos (MySQL)</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body { font-family: Arial; margin: 40px; background-color: #f4f4f4; }
        h1 { color: #333; }
        form { margin-bottom: 20px; }
        .evento { background: white; padding: 15px; margin-bottom: 10px; border-left: 5px solid #007bff; }
        .alerta { border-color: red; }
    </style>
</head>
<body>
    <h1>📡 Eventos Recebidos (Banco MySQL)</h1>
    <form method="get">
        <input type="text" name="filtro" placeholder="Palavra-chave" value="{{ filtro }}">
        <input type="date" name="data" value="{{ data }}">
        <button type="submit">Buscar</button>
    </form>
    {% for e in eventos %}
        <div class="evento {% if e.status == 'alerta' %}alerta{% endif %}">
            <strong>{{ e.timestamp }}</strong><br>
            <strong>Status:</strong> {{ e.status }}<br>
            <strong>Objeto:</strong> {{ e.objeto }}<br>
            <strong>Descrição:</strong> {{ e.descricao|safe }}
        </div>
    {% else %}
        <p>Nenhum evento encontrado.</p>
    {% endfor %}
</body>
</html>'''

@app.route("/")
def index():
    return "Servidor online com banco MySQL! Envie POST para /evento e veja /historico"

@app.route("/evento", methods=["POST"])
def receber():
    dados = request.json
    evento = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "alerta" if dados.get("detected") else "ok",
        "objeto": dados.get("object", ""),
        "descricao": dados.get("description", "").replace("\\n", "<br>")
    }
    salvar_evento(evento)
    return jsonify({"ok": True})

@app.route("/historico")
def historico():
    filtro = request.args.get("filtro", "")
    data = request.args.get("data", "")
    eventos = buscar_eventos(filtro, data)
    return render_template_string(HTML_TEMPLATE, eventos=eventos, filtro=filtro, data=data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
