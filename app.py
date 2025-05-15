from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return "Servidor online! Envie eventos via POST para /evento"

@app.route("/evento", methods=["POST"])
def receber_evento():
    dados = request.json
    print("🔔 Evento recebido:", dados)

    if dados.get("detected") == True:
        return jsonify({
            "status": "alerta",
            "mensagem": f"Objeto perigoso detectado: {dados.get('object')}"
        })
    else:
        return jsonify({
            "status": "ok",
            "mensagem": "Evento recebido sem risco."
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
