import os
import base64
from io import BytesIO
from datetime import datetime, timedelta
from urllib.parse import urlparse
import re
import hashlib

from flask import Flask, request, jsonify, url_for, send_file, abort, redirect
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text
from sqlalchemy.sql import text
from sqlalchemy.pool import NullPool


# -------------------- Config --------------------
DB_URL = os.getenv("DATABASE_URL", "sqlite:///eventos.db")
ADMIN_KEY = os.getenv("ADMIN_KEY", "troque-isto")

PRUNE_THRESHOLD = float(os.getenv("PRUNE_THRESHOLD", "0.80"))
PRUNE_TARGET    = float(os.getenv("PRUNE_TARGET", "0.70"))
PRUNE_BATCH     = int(os.getenv("PRUNE_BATCH", "1000"))
MAX_ROWS        = int(os.getenv("MAX_ROWS", "500000"))
UPDATE_WINDOW_SEC = int(os.getenv("UPDATE_WINDOW_SEC", "15"))

CONFIRM_VALUE = "SIM"

# Pool pequeno para reduzir RAM em planos free (pode ajustar via env)
_DB_POOL_SIZE    = int(os.getenv("DB_POOL_SIZE", "2"))
_DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "0"))
_DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))  # segundos

_engine_kwargs = dict(pool_pre_ping=True, future=True)
# Em casos extremos, você pode forçar NullPool (sem pool) definindo SQLALCHEMY_NULLPOOL=1
if os.getenv("SQLALCHEMY_NULLPOOL", "0") == "1":
    _engine_kwargs["poolclass"] = NullPool
else:
    _engine_kwargs.update(pool_size=_DB_POOL_SIZE, max_overflow=_DB_MAX_OVERFLOW, pool_recycle=_DB_POOL_RECYCLE)

engine = create_engine(DB_URL, **_engine_kwargs)
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
    # flatten p/ Grafana (opcional)
    Column("tratamento_status", Text),
    Column("tratamento_resumo", Text),
    Column("tratamento_em", Text),
    # dados suplementares (estatística)
    Column("vitimas_aparentes", Text),
    Column("criancas_ou_idosos", Text),
    Column("em_andamento", Text),
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
        # cria/popula matriz de tratamento (tabelas + mapeamento fixo)
        _ensure_tratamento_tables_and_seed(conn)

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



# =========================
# Matriz de tratamento (Qualificação -> Gravidade/Protocolo/Meio/Órgão)
# =========================
TRATAMENTO_MATRIZ = {
    "Tentativa de acesso não autorizado": {
        "gravidade": "Média",
        "protocolo": "Monitorar, registrar evidências, verificar recorrência, acionar se persistente",
        "meio": "Telefone 21 99999-9999",
        "orgao": "Vigilância Privada",
    },
    "Furto": {
        "gravidade": "Média",
        "protocolo": "Confirmar ocorrência, registrar imagens, acionar patrulha",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
    "Dano ao patrimônio público ou privado": {
        "gravidade": "Baixa",
        "protocolo": "Registrar, avaliar extensão do dano, acionar a Guarda Municipal",
        "meio": "Automático Sistema",
        "orgao": "Guarda Municipal",
    },
    "Roubo": {
        "gravidade": "Alta",
        "protocolo": "Acionamento imediato, preservação de imagens, acompanhamento em tempo real",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
    "Ostentação de arma de fogo": {
        "gravidade": "Crítica",
        "protocolo": "Alerta imediato, monitoramento contínuo, despacho emergencial",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
    "Porte de arma oculta": {
        "gravidade": "Alta",
        "protocolo": "Monitoramento discreto, alerta preventivo, registro para inteligência",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
    "Acidente em via pública": {
        "gravidade": "Média",
        "protocolo": "Avaliar vítimas, acionar emergência conforme gravidade",
        "meio": "Automático Sistema",
        "orgao": "Guarda Municipal",
    },
    "Agressão física sem arma": {
        "gravidade": "Alta",
        "protocolo": "Acionamento imediato, monitoramento do conflito",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
    "Ataque com arma branca": {
        "gravidade": "Crítica",
        "protocolo": "Alerta máximo, despacho urgente, preservação de provas",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
    "Ataque com arma de fogo": {
        "gravidade": "Crítica",
        "protocolo": "Protocolo de emergência máxima, múltiplos acionamentos",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
    "Violência sexual": {
        "gravidade": "Crítica",
        "protocolo": "Acionamento imediato, preservação rigorosa de imagens",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
    "Conflito generalizado": {
        "gravidade": "Alta",
        "protocolo": "Monitoramento ampliado, acionamento preventivo ou repressivo",
        "meio": "Automático Sistema",
        "orgao": "Polícia Militar",
    },
}


def _ensure_tratamento_tables_and_seed(conn):
    """Cria (se necessário) as tabelas de tratamento e popula a matriz fixa.

    Importante:
    - Para permitir edição manual via UI (renomear gravidades/protocolos/meios/órgãos),
      o seed NÃO deve reintroduzir automaticamente textos antigos após você editar.
    - Portanto, o seed da matriz TRATAMENTO_MATRIZ só roda se as tabelas alvo ainda estiverem vazias.
    """

    # --- DDL (sempre) ---
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS gravidade (
            id SERIAL PRIMARY KEY,
            nome TEXT UNIQUE NOT NULL
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS protocolo_tratamento (
            id SERIAL PRIMARY KEY,
            descricao TEXT UNIQUE NOT NULL
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS meio_acionamento (
            id SERIAL PRIMARY KEY,
            nome TEXT UNIQUE NOT NULL
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS orgao_acionado (
            id SERIAL PRIMARY KEY,
            nome TEXT UNIQUE NOT NULL
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS qualificacao_tratamento (
            qualificacao_id INTEGER PRIMARY KEY
                REFERENCES qualificacao_incidente(id) ON DELETE CASCADE,
            gravidade_id INTEGER NOT NULL REFERENCES gravidade(id),
            protocolo_id INTEGER NOT NULL REFERENCES protocolo_tratamento(id),
            meio_id INTEGER NOT NULL REFERENCES meio_acionamento(id),
            orgao_id INTEGER NOT NULL REFERENCES orgao_acionado(id)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS evento_tratamento (
            evento_id INTEGER NOT NULL REFERENCES eventos(id) ON DELETE CASCADE,
            qualificacao_id INTEGER NOT NULL REFERENCES qualificacao_incidente(id) ON DELETE CASCADE,
            gravidade_id INTEGER NOT NULL REFERENCES gravidade(id),
            protocolo_id INTEGER NOT NULL REFERENCES protocolo_tratamento(id),
            meio_id INTEGER NOT NULL REFERENCES meio_acionamento(id),
            orgao_id INTEGER NOT NULL REFERENCES orgao_acionado(id),
            created_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (evento_id, qualificacao_id)
        );
    """))

    # --- Seed (somente se tabelas ainda estão vazias) ---
    try:
        has_any = (
            int(conn.execute(text("SELECT COUNT(*) FROM gravidade")).scalar() or 0) > 0 or
            int(conn.execute(text("SELECT COUNT(*) FROM protocolo_tratamento")).scalar() or 0) > 0 or
            int(conn.execute(text("SELECT COUNT(*) FROM meio_acionamento")).scalar() or 0) > 0 or
            int(conn.execute(text("SELECT COUNT(*) FROM orgao_acionado")).scalar() or 0) > 0
        )
    except Exception:
        has_any = False

    # Se já há dados (provavelmente editados), não re-semeia
    if has_any:
        return

    gravidades = {v["gravidade"] for v in TRATAMENTO_MATRIZ.values()}
    meios = {v["meio"] for v in TRATAMENTO_MATRIZ.values()}
    orgaos = {v["orgao"] for v in TRATAMENTO_MATRIZ.values()}
    protocolos = {v["protocolo"] for v in TRATAMENTO_MATRIZ.values()}

    for g in sorted(gravidades):
        conn.execute(text("INSERT INTO gravidade (nome) VALUES (:n) ON CONFLICT (nome) DO NOTHING"), {"n": g})
    for m in sorted(meios):
        conn.execute(text("INSERT INTO meio_acionamento (nome) VALUES (:n) ON CONFLICT (nome) DO NOTHING"), {"n": m})
    for o in sorted(orgaos):
        conn.execute(text("INSERT INTO orgao_acionado (nome) VALUES (:n) ON CONFLICT (nome) DO NOTHING"), {"n": o})
    for p in sorted(protocolos):
        conn.execute(text("INSERT INTO protocolo_tratamento (descricao) VALUES (:d) ON CONFLICT (descricao) DO NOTHING"), {"d": p})

    # Popula a matriz qualificacao_tratamento somente se estiver vazia
    try:
        qt_count = int(conn.execute(text("SELECT COUNT(*) FROM qualificacao_tratamento")).scalar() or 0)
    except Exception:
        qt_count = 0

    if qt_count <= 0:
        for qual_nome, meta in TRATAMENTO_MATRIZ.items():
            conn.execute(text("""
                INSERT INTO qualificacao_tratamento (qualificacao_id, gravidade_id, protocolo_id, meio_id, orgao_id)
                SELECT qi.id, g.id, p.id, m.id, o.id
                  FROM qualificacao_incidente qi
                  JOIN gravidade g ON g.nome = :grav
                  JOIN protocolo_tratamento p ON p.descricao = :prot
                  JOIN meio_acionamento m ON m.nome = :meio
                  JOIN orgao_acionado o ON o.nome = :org
                 WHERE qi.nome = :qual
                ON CONFLICT (qualificacao_id) DO NOTHING
            """), {
                "qual": qual_nome,
                "grav": meta["gravidade"],
                "prot": meta["protocolo"],
                "meio": meta["meio"],
                "org": meta["orgao"],
            })


def _salvar_tratamento_evento(conn, ev_id: int, qual_ids):
    """Aplica a matriz para o evento, gerando linhas em evento_tratamento."""
    qual_ids = [int(q) for q in (qual_ids or []) if str(q).strip().isdigit()]
    conn.execute(text("DELETE FROM evento_tratamento WHERE evento_id=:id"), {"id": ev_id})
    if not qual_ids:
        return

    for qid in dict.fromkeys(qual_ids):
        conn.execute(text("""
            INSERT INTO evento_tratamento (evento_id, qualificacao_id, gravidade_id, protocolo_id, meio_id, orgao_id)
            SELECT :ev_id, qt.qualificacao_id, qt.gravidade_id, qt.protocolo_id, qt.meio_id, qt.orgao_id
              FROM qualificacao_tratamento qt
             WHERE qt.qualificacao_id = :qid
            ON CONFLICT (evento_id, qualificacao_id) DO NOTHING
        """), {"ev_id": ev_id, "qid": qid})



def _refresh_tratamento_flat(conn, ev_id: int):
    """Atualiza colunas flatten (tratamento_*) na tabela eventos a partir de evento_tratamento.
    Objetivo: permitir Grafana/queries simples sem JOINs complexos.
    - tratamento_status: 'PENDENTE' | 'TRATADO' (quando confirmado='SIM')
    - tratamento_resumo: texto curto agregado (gravidade/meio/órgão + qualificação)
    - tratamento_em: timestamp de criação do tratamento (max created_at)
    """
    try:
        row = conn.execute(text("""
            SELECT
              COUNT(*) AS n,
              STRING_AGG(DISTINCT qi.nome, ', ' ORDER BY qi.nome) AS quals,
              STRING_AGG(DISTINCT g.nome,  ', ' ORDER BY g.nome)  AS gravs,
              STRING_AGG(DISTINCT m.nome,  ', ' ORDER BY m.nome)  AS meios,
              STRING_AGG(DISTINCT o.nome,  ', ' ORDER BY o.nome)  AS orgaos,
              MAX(t.created_at) AS last_at
            FROM evento_tratamento t
            JOIN qualificacao_incidente qi ON qi.id = t.qualificacao_id
            JOIN gravidade g ON g.id = t.gravidade_id
            JOIN meio_acionamento m ON m.id = t.meio_id
            JOIN orgao_acionado o ON o.id = t.orgao_id
            WHERE t.evento_id = :id
        """), {"id": int(ev_id)}).first()
        n = int(row[0] or 0) if row else 0
        quals, gravs, meios, orgaos, last_at = (row[1], row[2], row[3], row[4], row[5]) if row else (None,None,None,None,None)

        if n <= 0:
            # se confirmado, mas sem tratamento, marca como pendente
            conn.execute(text("""
                UPDATE eventos
                   SET tratamento_status = CASE WHEN confirmado = :c THEN 'PENDENTE' ELSE COALESCE(NULLIF(tratamento_status,''),'') END,
                       tratamento_resumo = CASE WHEN confirmado = :c THEN COALESCE(NULLIF(tratamento_resumo,''),'') ELSE COALESCE(NULLIF(tratamento_resumo,''),'') END,
                       tratamento_em = CASE WHEN confirmado = :c THEN COALESCE(NULLIF(tratamento_em,''),'') ELSE COALESCE(NULLIF(tratamento_em,''),'') END
                 WHERE id = :id
            """), {"id": int(ev_id), "c": CONFIRM_VALUE})
            return

        resumo_parts = []
        if gravs: resumo_parts.append(f"Gravidade: {gravs}")
        if orgaos: resumo_parts.append(f"Órgão: {orgaos}")
        if meios: resumo_parts.append(f"Meio: {meios}")
        if quals: resumo_parts.append(f"Qualificação: {quals}")
        resumo = " | ".join(resumo_parts)[:2000]

        conn.execute(text("""
            UPDATE eventos
               SET tratamento_status = 'TRATADO',
                   tratamento_resumo = :resumo,
                   tratamento_em = COALESCE(:tem, tratamento_em)
             WHERE id = :id
        """), {"id": int(ev_id), "resumo": resumo or '', "tem": (str(last_at) if last_at else '')})
    except Exception:
        # nunca derrubar confirmação por causa desse espelho
        app.logger.exception('Falha ao atualizar tratamento_* flatten para evento %s', ev_id)
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
            # NOVO (tratamento flatten p/ Grafana)
            if "tratamento_status" not in names: add("tratamento_status")
            if "tratamento_resumo" not in names: add("tratamento_resumo")
            if "tratamento_em"     not in names: add("tratamento_em")
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
            # NOVO (tratamento flatten p/ Grafana)
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS tratamento_status TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS tratamento_resumo TEXT",
            "ALTER TABLE eventos ADD COLUMN IF NOT EXISTS tratamento_em TEXT",
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


    # Backfill: se já houver eventos confirmados sem tratamento_status, marca como PENDENTE (não altera tratados).
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE eventos
                   SET tratamento_status = 'PENDENTE'
                 WHERE (tratamento_status IS NULL OR tratamento_status = '')
                   AND confirmado = 'SIM'
            """))
    except Exception as _e:
        try:
            app.logger.exception('Falha no backfill de tratamento_status')
        except Exception:
            print('WARN: backfill tratamento_status falhou:', _e)
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
        ", COALESCE(vitimas_aparentes,'') AS vitimas_aparentes"
        ", COALESCE(criancas_ou_idosos,'') AS criancas_ou_idosos"
        ", COALESCE(em_andamento,'') AS em_andamento"
        ", COALESCE(tratamento_status,'') AS tratamento_status"
        ", COALESCE(tratamento_resumo,'') AS tratamento_resumo"
        ", COALESCE(tratamento_em,'') AS tratamento_em"
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
            vitimas_aparentes=r[23] or "",
            criancas_ou_idosos=r[24] or "",
            em_andamento=r[25] or "",
            tratamento_status=r[26] or "",
            tratamento_resumo=r[27] or "",
            tratamento_em=r[28] or "",
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

# Garante schema/seed também em deploy via gunicorn (import app:app)
try:
    init_db()
except Exception as _e:
    # Não impede a subida do serviço; erros de migração/seed aparecerão nos logs
    print('WARN: init_db falhou:', _e)

# Pré-compila templates para reduzir alocações por request
_MAIN_TEMPLATE = app.jinja_env.from_string(HTML_TEMPLATE)

def _render_main(**ctx):
    return _MAIN_TEMPLATE.render(**ctx)

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
        "GET /historico | GET /indicios | GET /confirmados | GET /tratamentos | "
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

    # Se não veio hash, calcula uma vez por evento (evita varreduras caras no /confirmar)
    if not sha256 and img_b64:
        sha256 = _sha1_from_b64_image(img_b64)

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
            # UPDATE seguro: NÃO sobrescreve campos com string vazia.
            # Isso evita "apagar" sha256, llava_pt, img_url, imagem, etc., quando chegam eventos parciais.
            params = {k: ("" if v is None else v) for k, v in base_row.items() if k != "timestamp"}
            params["id"] = int(row[0])
            params["ts"] = _now_str()

            set_parts = []
            for k in base_row.keys():
                if k == "timestamp":
                    continue
                # mantém o valor existente se o payload vier vazio
                set_parts.append(f"{k}=COALESCE(NULLIF(:{k},''), {k})")

            conn.execute(
                text("UPDATE eventos SET " + ", ".join(set_parts) + ", timestamp=:ts WHERE id=:id"),
                params
            )
            ev_id = int(row[0])
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
<html lang="pt-br">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Confirmar caso real de Violência</title>
  <style>
    :root { --bg:#f6f7fb; --card:#fff; --text:#111827; --muted:#6b7280; --border:#e5e7eb; --ok:#16a34a; --danger:#dc2626; }
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,sans-serif;margin:0;background:var(--bg);color:var(--text);}
    .wrap{max-width:1200px;margin:24px auto;padding:0 16px;}
    .top{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;}
    h1{font-size:28px;margin:0;}
    a{color:#2563eb;text-decoration:none;}
    .card{background:var(--card);border:1px solid var(--border);border-radius:14px;box-shadow:0 1px 10px rgba(0,0,0,.05);padding:16px;}
    form{margin:0;}
    .grid{display:grid;grid-template-columns: 420px 1fr;gap:16px;align-items:start;}
    @media (max-width: 980px){ .grid{grid-template-columns:1fr;} }
    .imgbox{border:1px solid var(--border);border-radius:12px;overflow:hidden;background:#fff;}
    .imgbox img{width:100%;display:block;}
    .imgempty{padding:32px;color:var(--muted);text-align:center;}
    .qualbox{margin-top:12px;border:1px solid var(--border);border-radius:12px;padding:12px;background:#fff;}
    .qualbox h3{margin:0 0 8px 0;font-size:16px;}
    .qualhint{color:var(--muted);font-size:12px;margin:0 0 8px 0;}
    /* ===== Qualificação (layout melhorado) ===== */
    .qualgrid{
      display:grid;
      grid-template-columns:repeat(2, minmax(0, 1fr));
      gap:10px;
      max-height:280px;
      overflow:auto;
      padding-right:6px;
    }
    
    .qitem{
      display:grid;
      grid-template-columns:20px 1fr;   /* coluna do checkbox + coluna do texto */
      align-items:start;
      column-gap:10px;
      padding:8px 10px;
      border:1px solid #e5e7eb;
      border-radius:10px;
      background:#fff;
    }
    
    .qitem input{
      margin-top:3px;
    }
    
    .qitem span{
      display:block;
      text-align:left;
      line-height:1.2;
      word-break:break-word;
    }
    
    /* Responsivo: em telas estreitas, 1 coluna */
    @media (max-width: 860px){
      .qualgrid{grid-template-columns:1fr;}
    }

    .meta{display:grid;grid-template-columns:160px 1fr;gap:6px 12px;margin-bottom:10px;}
    .meta .k{color:var(--muted);font-weight:600;}
    .pill{display:inline-block;padding:2px 10px;border-radius:999px;border:1px solid var(--border);font-size:12px;margin-left:6px;background:#fff;}
    .section{border-top:1px solid var(--border);padding-top:12px;margin-top:12px;}
    .label{font-weight:700;margin:0 0 6px 0;}
    textarea,input{width:100%;border:1px solid var(--border);border-radius:10px;padding:10px;font-size:14px;box-sizing:border-box;}
    textarea{min-height:120px;resize:vertical;}
    .actions{display:flex;gap:10px;align-items:center;margin-top:12px;flex-wrap:wrap;}
    .oprow{display:grid;grid-template-columns:260px 1fr;gap:14px;align-items:start;}
    @media (max-width: 860px){ .oprow{grid-template-columns:1fr;} }
    .suppbox{border:1px solid var(--border);border-radius:12px;padding:10px;background:#fff;}
    .ck{display:flex;gap:10px;align-items:flex-start;margin:6px 0;color:var(--text);font-size:14px;}
    .ck input{margin-top:2px;}
    .btn{border:0;border-radius:10px;padding:10px 16px;font-weight:700;color:#fff;cursor:pointer;}
    .btn-ok{background:var(--ok);}
    .btn-danger{background:var(--danger);}
    .note{color:var(--muted);font-size:12px;}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Confirmar caso real de Violência</h1>
      <a href="{{ next_url }}">Voltar</a>
    </div>

    <div class="card">
      <form method="post">
        <input type="hidden" name="key" value="{{ key_value }}">
        <input type="hidden" name="id" value="{{ ev.id }}">
        <input type="hidden" name="next" value="{{ next_url }}">

        <div class="grid">
          <div>
            <div class="imgbox">
              {% if img_src %}
                <img src="{{ img_src }}" alt="Imagem do evento">
              {% else %}
                <div class="imgempty">Sem imagem disponível.</div>
              {% endif %}
            </div>

            <div class="qualbox">
              <h3>Qualificação do incidente</h3>
              <p class="qualhint">Selecione uma ou mais opções (opcional).</p>
              <div class="qualgrid">
                {% for q in qualificacoes %}
                  <label class="qitem">
                    <input type="checkbox" name="qualificacoes" value="{{ q['id'] }}"
                      {% if q['id'] in qual_sel %}checked{% endif %}>
                    <span>{{ q['nome'] }}</span>
                  </label>
                {% endfor %}
              </div>
            </div>
          </div>

          <div>
            <div class="meta">
              <div class="k">ID:</div><div>{{ ev.id }}</div>
              <div class="k">Timestamp:</div><div>{{ ev.timestamp }}</div>
              <div class="k">Status:</div><div>{{ ev.status }} <span class="pill">{{ ev.status }}</span></div>
              <div class="k">Identificador:</div><div>{{ ev.identificador }}</div>
              <div class="k">Câmera:</div><div>{{ ev.camera_name }}</div>
              <div class="k">Local:</div><div>{{ ev.local }}</div>
              <div class="k">Objeto:</div><div>{{ ev.objeto }}</div>
            </div>

            <div class="section">
              <p class="label">Análise objeto:</p>
              <div>{{ ev.yolo }}</div>
            </div>

            <div class="section">
              <p class="label">Diagnóstico:</p>
              <div style="white-space:pre-wrap">{{ ev.llava_pt }}</div>
            </div>

            <div class="section">
              <div class="oprow">
                <div>
                  <p class="label">Operador:</p>
                  <input name="operador" value="{{ ev.confirmado_por or '' }}" placeholder="ex.: op1" />
                </div>

                <div class="suppbox">
                  <p class="label" style="margin-top:0;">Dados Suplementares</p>
                  <label class="ck"><input type="checkbox" name="vitimas_aparentes" value="SIM" {% if ev.vitimas_aparentes == 'SIM' %}checked{% endif %}> Presença de vítimas aparentes</label>
                  <label class="ck"><input type="checkbox" name="criancas_ou_idosos" value="SIM" {% if ev.criancas_ou_idosos == 'SIM' %}checked{% endif %}> Envolvimento de crianças ou idosos</label>
                  <label class="ck"><input type="checkbox" name="em_andamento" value="SIM" {% if ev.em_andamento == 'SIM' %}checked{% endif %}> Em andamento</label>
                </div>
              </div>
            </div>

            <div class="section">
              <p class="label">Relato do operador:</p>
              <textarea name="relato" placeholder="Descreva o que foi visto na imagem...">{{ ev.relato_operador or '' }}</textarea>
            </div>

            <div class="actions">
              <button class="btn btn-ok" name="action" value="confirmar" type="submit">Confirmar</button>
              <button class="btn btn-danger" name="action" value="desconfirmar" type="submit">Desfazer</button>
              <span class="note">O relato é obrigatório para confirmar.</span>
            </div>
          </div>
        </div>
      </form>
    </div>
  </div>
</body>
</html>
"""
# Pré-compila template de confirmação para reduzir alocações por request
_CONFIRM_TEMPLATE = app.jinja_env.from_string(CONFIRM_UI_TEMPLATE)

def _render_confirm(**ctx):
    return _CONFIRM_TEMPLATE.render(**ctx)


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
        "relato_operador": r[12] or "",
        "confirmado_por": r[13] or "",
        "vitimas_aparentes": r[14] or "",
        "criancas_ou_idosos": r[15] or "",
        "em_andamento": r[16] or "",
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

    Otimização: itera em streaming (sem .all()) para não carregar dezenas/centenas de imagens base64 em RAM.
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
        result = conn.execution_options(stream_results=True).execute(
            text("""
                SELECT id, imagem
                  FROM eventos
                 WHERE (sha256 IS NULL OR sha256 = '')
                   AND (imagem IS NOT NULL AND imagem <> '')
                 ORDER BY id DESC
                 LIMIT :lim
            """),
            {"lim": limit},
        )

        for ev_id, img_b64 in result:
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
                continue

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

        vitimas_aparentes = "SIM" if request.form.get("vitimas_aparentes") == "SIM" else ""
        criancas_ou_idosos = "SIM" if request.form.get("criancas_ou_idosos") == "SIM" else ""
        em_andamento = "SIM" if request.form.get("em_andamento") == "SIM" else ""

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
                           confirmado_em='',
                           tratamento_status='',
                           tratamento_resumo='',
                           tratamento_em='',
                       vitimas_aparentes='',
                       criancas_ou_idosos='',
                       em_andamento=''
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
                           tratamento_status = 'PENDENTE',
                           tratamento_em = COALESCE(NULLIF(tratamento_em,''), :em),
                           sha256 = COALESCE(NULLIF(sha256,''), NULLIF(:sha,'')),
                           vitimas_aparentes=:va,
                           criancas_ou_idosos=:ci,
                           em_andamento=:ea
                     WHERE id=:id
                """), {
                    "c": CONFIRM_VALUE,
                    "r": relato,
                    "p": operador,
                    "em": _now_str(),
                    "sha": sha_calc,
                    "va": vitimas_aparentes,
                    "ci": criancas_ou_idosos,
                    "ea": em_andamento,
                    "id": ev_id
                })

                # Atualiza relação N:N com as qualificações escolhidas
                _salvar_qualificacoes_evento(conn, ev_id, qual_ids)

                

                # aplica matriz de tratamento (gravidade/protocolo/meio/órgão)
                try:
                    _salvar_tratamento_evento(conn, ev_id, qual_ids)
                    _refresh_tratamento_flat(conn, ev_id)
                except Exception:
                    app.logger.exception('Falha ao salvar tratamento do evento %s', ev_id)
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

    return _render_confirm(
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
    vitimas_aparentes = "SIM" if _trim(dados.get("vitimas_aparentes")) == "SIM" else ""
    criancas_ou_idosos = "SIM" if _trim(dados.get("criancas_ou_idosos")) == "SIM" else ""
    em_andamento = "SIM" if _trim(dados.get("em_andamento")) == "SIM" else ""
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
                       tratamento_status = 'PENDENTE',
                       tratamento_em = COALESCE(NULLIF(tratamento_em,''), :em),
                       sha256 = COALESCE(NULLIF(sha256,''), NULLIF(:sha,'')),
                       vitimas_aparentes=:va,
                       criancas_ou_idosos=:ci,
                       em_andamento=:ea
                 WHERE id=:id
            """),
            {
                "c": confirmado,
                "r": relato,
                "p": operador,
                "em": _now_str(),
                "sha": sha_calc,
                "va": vitimas_aparentes,
                "ci": criancas_ou_idosos,
                "ea": em_andamento,
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
                       confirmado_em='',
                       tratamento_status='',
                       tratamento_resumo='',
                       tratamento_em='',
                       vitimas_aparentes='',
                       criancas_ou_idosos='',
                       em_andamento=''
                 WHERE id=:id
            """),
            {"id": ev_id}
        )
    return jsonify({"ok": True})


# -------------------- UI: editar textos de tratamento (lookup) --------------------
TRATAMENTO_EDIT_TEMPLATE = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Editar textos de Tratamento</title>
  <style>
    :root { --bg:#f6f7fb; --card:#fff; --text:#111827; --muted:#6b7280; --border:#e5e7eb; --brand:#111827; }
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Arial;margin:0;background:var(--bg);color:var(--text);}
    .wrap{max-width:1100px;margin:22px auto;padding:0 16px;}
    .top{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px;}
    h1{font-size:22px;margin:0;color:var(--brand);}
    a{color:#2563eb;text-decoration:none;}
    .card{background:var(--card);border:1px solid var(--border);border-radius:14px;box-shadow:0 1px 10px rgba(0,0,0,.05);padding:14px;margin:14px 0;}
    .hint{color:var(--muted);font-size:12px;margin:6px 0 0 0;}
    table{width:100%;border-collapse:collapse;}
    th,td{border-bottom:1px solid var(--border);padding:10px 8px;text-align:left;vertical-align:top;}
    th{font-size:12px;color:var(--muted);font-weight:700;}
    input[type="text"]{width:100%;border:1px solid var(--border);border-radius:10px;padding:10px;font-size:14px;box-sizing:border-box;}
    .row-add{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:10px}
    .btn{border:0;border-radius:10px;padding:10px 14px;font-weight:700;cursor:pointer;}
    .btn-save{background:#111827;color:#fff;}
    .btn-add{background:#2563eb;color:#fff;}
    .ok{background:#dcfce7;border:1px solid #bbf7d0;color:#166534;padding:10px 12px;border-radius:12px;margin:12px 0;}
    .err{background:#fee2e2;border:1px solid #fecaca;color:#991b1b;padding:10px 12px;border-radius:12px;margin:12px 0;}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Editar textos de tratamento (lookup)</h1>
      <a href="{{ back_url }}">Voltar</a>
    </div>

    {% if ok %}
      <div class="ok">Atualizado com sucesso.</div>
    {% endif %}
    {% if err %}
      <div class="err">{{ err }}</div>
    {% endif %}

    <form method="post">
      <input type="hidden" name="key" value="{{ key_value }}">

      <div class="card">
        <h2 style="margin:0 0 6px 0;font-size:16px;">Gravidade</h2>
        <p class="hint">Edite os textos e clique em “Salvar tudo”.</p>
        <table>
          <thead><tr><th style="width:70px;">ID</th><th>Texto</th></tr></thead>
          <tbody>
          {% for r in gravidades %}
            <tr>
              <td>{{ r.id }}</td>
              <td><input type="text" name="g_{{ r.id }}" value="{{ r.nome }}"></td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        <div class="row-add">
          <input type="text" name="add_gravidade" placeholder="Adicionar nova gravidade...">
          <button class="btn btn-add" name="action" value="add_gravidade" type="submit">Adicionar</button>
        </div>
      </div>

      <div class="card">
        <h2 style="margin:0 0 6px 0;font-size:16px;">Protocolo de Tratamento</h2>
        <p class="hint">Edite o texto do protocolo. (Pode ser longo.)</p>
        <table>
          <thead><tr><th style="width:70px;">ID</th><th>Texto</th></tr></thead>
          <tbody>
          {% for r in protocolos %}
            <tr>
              <td>{{ r.id }}</td>
              <td><input type="text" name="p_{{ r.id }}" value="{{ r.descricao }}"></td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        <div class="row-add">
          <input type="text" name="add_protocolo" placeholder="Adicionar novo protocolo...">
          <button class="btn btn-add" name="action" value="add_protocolo" type="submit">Adicionar</button>
        </div>
      </div>

      <div class="card">
        <h2 style="margin:0 0 6px 0;font-size:16px;">Meio de Acionamento</h2>
        <table>
          <thead><tr><th style="width:70px;">ID</th><th>Texto</th></tr></thead>
          <tbody>
          {% for r in meios %}
            <tr>
              <td>{{ r.id }}</td>
              <td><input type="text" name="m_{{ r.id }}" value="{{ r.nome }}"></td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        <div class="row-add">
          <input type="text" name="add_meio" placeholder="Adicionar novo meio...">
          <button class="btn btn-add" name="action" value="add_meio" type="submit">Adicionar</button>
        </div>
      </div>

      <div class="card">
        <h2 style="margin:0 0 6px 0;font-size:16px;">Órgão Acionado</h2>
        <table>
          <thead><tr><th style="width:70px;">ID</th><th>Texto</th></tr></thead>
          <tbody>
          {% for r in orgaos %}
            <tr>
              <td>{{ r.id }}</td>
              <td><input type="text" name="o_{{ r.id }}" value="{{ r.nome }}"></td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        <div class="row-add">
          <input type="text" name="add_orgao" placeholder="Adicionar novo órgão...">
          <button class="btn btn-add" name="action" value="add_orgao" type="submit">Adicionar</button>
        </div>
      </div>

      <div class="card" style="display:flex;justify-content:flex-end;">
        <button class="btn btn-save" name="action" value="save_all" type="submit">Salvar tudo</button>
      </div>
    </form>
  </div>
</body>
</html>
"""
_TRAT_EDIT_TEMPLATE = app.jinja_env.from_string(TRATAMENTO_EDIT_TEMPLATE)

def _render_trat_edit(**ctx):
    return _TRAT_EDIT_TEMPLATE.render(**ctx)

def _listar_lookup(table: str, col: str):
    with engine.begin() as conn:
        rows = conn.execute(text(f"SELECT id, {col} FROM {table} ORDER BY id")).fetchall()
    out = []
    for r in rows:
        if col == "descricao":
            out.append({"id": int(r[0]), "descricao": r[1] or ""})
        else:
            out.append({"id": int(r[0]), "nome": r[1] or ""})
    return out

@app.route("/tratamentos", methods=["GET", "POST"])
def tratamentos_ui():
    if not _admin_ok():
        abort(403)

    key_value = (request.values.get("key") or "").strip()
    back_url = (request.values.get("next") or "/").strip()
    ok = (request.args.get("ok") or "").strip() == "1"
    err = ""

    def _safe_update(conn, sql_txt: str, params: dict):
        nonlocal err
        try:
            conn.execute(text(sql_txt), params)
        except Exception as e:
            # mantém o app vivo e mostra mensagem no UI
            err = str(e)[:500]

    if request.method == "POST":
        action = (request.form.get("action") or "save_all").strip()

        with engine.begin() as conn:
            # Atualizações (renomear)
            for k, v in request.form.items():
                if k.startswith("g_"):
                    try:
                        _id = int(k[2:])
                        _val = _trim(v)
                        if _val:
                            _safe_update(conn, "UPDATE gravidade SET nome=:v WHERE id=:id", {"v": _val, "id": _id})
                    except Exception:
                        pass
                elif k.startswith("p_"):
                    try:
                        _id = int(k[2:])
                        _val = _trim(v)
                        if _val:
                            _safe_update(conn, "UPDATE protocolo_tratamento SET descricao=:v WHERE id=:id", {"v": _val, "id": _id})
                    except Exception:
                        pass
                elif k.startswith("m_"):
                    try:
                        _id = int(k[2:])
                        _val = _trim(v)
                        if _val:
                            _safe_update(conn, "UPDATE meio_acionamento SET nome=:v WHERE id=:id", {"v": _val, "id": _id})
                    except Exception:
                        pass
                elif k.startswith("o_"):
                    try:
                        _id = int(k[2:])
                        _val = _trim(v)
                        if _val:
                            _safe_update(conn, "UPDATE orgao_acionado SET nome=:v WHERE id=:id", {"v": _val, "id": _id})
                    except Exception:
                        pass

            # Inserções (novos itens)
            if action == "add_gravidade":
                nv = _trim(request.form.get("add_gravidade"))
                if nv:
                    _safe_update(conn, "INSERT INTO gravidade (nome) VALUES (:v) ON CONFLICT (nome) DO NOTHING", {"v": nv})
            elif action == "add_protocolo":
                nv = _trim(request.form.get("add_protocolo"))
                if nv:
                    _safe_update(conn, "INSERT INTO protocolo_tratamento (descricao) VALUES (:v) ON CONFLICT (descricao) DO NOTHING", {"v": nv})
            elif action == "add_meio":
                nv = _trim(request.form.get("add_meio"))
                if nv:
                    _safe_update(conn, "INSERT INTO meio_acionamento (nome) VALUES (:v) ON CONFLICT (nome) DO NOTHING", {"v": nv})
            elif action == "add_orgao":
                nv = _trim(request.form.get("add_orgao"))
                if nv:
                    _safe_update(conn, "INSERT INTO orgao_acionado (nome) VALUES (:v) ON CONFLICT (nome) DO NOTHING", {"v": nv})

        # Redirect (evita re-POST)
        if err:
            return _render_trat_edit(
                key_value=key_value,
                back_url=back_url,
                ok=False,
                err=err,
                gravidades=_listar_lookup("gravidade", "nome"),
                protocolos=_listar_lookup("protocolo_tratamento", "descricao"),
                meios=_listar_lookup("meio_acionamento", "nome"),
                orgaos=_listar_lookup("orgao_acionado", "nome"),
            )

        return redirect(url_for("tratamentos_ui", key=key_value, next=back_url, ok="1"))

    # GET
    return _render_trat_edit(
        key_value=key_value,
        back_url=back_url,
        ok=ok,
        err=err,
        gravidades=_listar_lookup("gravidade", "nome"),
        protocolos=_listar_lookup("protocolo_tratamento", "descricao"),
        meios=_listar_lookup("meio_acionamento", "nome"),
        orgaos=_listar_lookup("orgao_acionado", "nome"),
    )


# -------------------- Painéis Flask (BD) --------------------
@app.route("/historico")
def historico():
    filtro = (request.args.get("filtro") or "").strip()
    data = (request.args.get("data") or "").strip()
    page = max(int(request.args.get("page") or 1), 1)
    evs = buscar_eventos(filtro if filtro else None, data if data else None,
                         status=None, confirmado=None, limit=50, offset=(page-1)*50)
    return _render_main(
        page_title="Painel Histórico",
        eventos=evs,
        filtro=filtro,
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

    return _render_main(
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

    return _render_main(
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
           COALESCE(vitimas_aparentes,''), COALESCE(criancas_ou_idosos,''), COALESCE(em_andamento,''),
           COALESCE(tratamento_status,''), COALESCE(tratamento_resumo,''), COALESCE(tratamento_em,''),
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
            "vitimas_aparentes": r[20] or "",
            "criancas_ou_idosos": r[21] or "",
            "em_andamento": r[22] or "",
            "tratamento_status": r[23] or "",
            "tratamento_resumo": r[24] or "",
            "tratamento_em": r[25] or "",
            "has_img": bool(r[26]),
            "image_url": url_for("img", ev_id=r[0], _external=True) if r[26] else ""
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
