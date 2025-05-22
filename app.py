from flask import Flask, request, jsonify, render_template_string
from datetime import datetime

app = Flask(__name__)
eventos = []

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Eventos Recebidos</title>
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
            <strong>Objeto:</strong> {{ e.objeto }}<br>
            <strong>Descrição:</strong> {{ e.descricao|safe }}
        </div>
    {% else %}
        <p>Nenhum evento encontrado.</p>
    {% endfor %}
</body>
</html>
'''

@app.route("/")
def index():
    return "Servidor online! Envie POST para /evento e veja /historico"

@app.route("/evento", methods=["POST"])
def receber():
    dados = request.json
    evento = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "alerta" if dados.get("detected") else "ok",
        "objeto": dados.get("object", ""),
        "descricao": dados.get("description", "").replace("\n", "<br>")
    }
    eventos.insert(0, evento)
    return jsonify({"ok": True})

@app.route("/historico")
def historico():
    filtro = request.args.get("filtro", "").lower()
    data = request.args.get("data", "")
    filtrados = eventos

    if filtro:
        filtrados = [e for e in filtrados if filtro in e["objeto"].lower() or filtro in e["descricao"].lower()]
    if data:
        filtrados = [e for e in filtrados if e["timestamp"].startswith(data)]

    return render_template_string(HTML_TEMPLATE, eventos=filtrados[:100], filtro=filtro, data=data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
