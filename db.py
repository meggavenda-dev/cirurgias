
# -*- coding: utf-8 -*-
"""
db.py — Camada de acesso ao SQLite (persistência do app)

Principais recursos:
- Caminho estável e gravável (DB_DIR via env -> ./data -> /tmp).
- PRAGMAs úteis (FK, WAL, synchronous).
- VACUUM robusto (checkpoint + optimize) usando sqlite3 e dispose_engine() para evitar locks.
- Índices únicos idempotentes para ON CONFLICT confiável.
- Reset/Manutenção (hard_reset_local_db, vacuum, etc).
- Base (pacientes): upsert_dataframe, upsert_paciente_single, delete_paciente_by_key, leituras.
- Catálogos: listar, upsert, ativar/inativar.
- Cirurgias: upsert, listar (com filtro Ano/Mês), excluir por id/chave, excluir por filtros.
"""

from __future__ import annotations

import os
import math
import tempfile
import sqlite3
from typing import Dict, Any, Optional, Sequence, Tuple, List
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =============================================================================
# CONFIGURAÇÃO DO BANCO (caminho gravável)
# =============================================================================

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Preferir variável de ambiente; senão ./data; se falhar, fallback para /tmp
DB_DIR = os.environ.get("DB_DIR") or os.environ.get("STREAMLIT_DB_DIR")
if not DB_DIR:
    candidate = os.path.join(BASE_DIR, "data")
    try:
        os.makedirs(candidate, exist_ok=True)
        DB_DIR = candidate
    except Exception:
        DB_DIR = os.path.join(tempfile.gettempdir(), "acompanhamento_db")
        os.makedirs(DB_DIR, exist_ok=True)

DB_PATH = os.path.join(DB_DIR, "exemplo.db")
DB_URI = f"sqlite:///{DB_PATH}"

_ENGINE: Optional[Engine] = None


def ensure_db_writable() -> None:
    """Garante que o diretório e o arquivo do DB são graváveis; ajusta permissões quando possível."""
    dir_path = os.path.dirname(DB_PATH) or "."
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    if not os.access(dir_path, os.W_OK):
        raise PermissionError(f"Diretório do DB não é gravável: {dir_path}")

    if os.path.exists(DB_PATH):
        try:
            os.chmod(DB_PATH, 0o664)  # rw-rw-r--
        except Exception:
            # Se não conseguir, segue — o erro real aparecerá na escrita
            pass


def get_engine() -> Engine:
    """Retorna (e cria se necessário) a engine do SQLAlchemy."""
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(
            DB_URI,
            future=True,
            echo=False,
            connect_args={"check_same_thread": False},  # útil em Streamlit
        )
    return _ENGINE


def dispose_engine() -> None:
    """Fecha e descarta a engine atual (útil para reset/manutenção)."""
    global _ENGINE
    if _ENGINE:
        try:
            _ENGINE.dispose()
        finally:
            _ENGINE = None


# =============================================================================
# HELPERS
# =============================================================================

def _safe_int(v, default=0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, float) and math.isnan(v):
            return default
        return int(float(str(v).strip()))
    except Exception:
        return default


def _safe_str(v, default: str = "") -> str:
    if v is None:
        return default
    try:
        if isinstance(v, float) and math.isnan(v):
            return default
    except Exception:
        pass
    return str(v).strip()


# =============================================================================
# GARANTIA DE ÍNDICES ÚNICOS
# =============================================================================

def ensure_unique_indexes() -> None:
    """Cria índices únicos idempotentes (garante ON CONFLICT funcionando)."""
    eng = get_engine()
    with eng.begin() as conn:
        # Índice único para a base de pacientes (chave resiliente)
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_pacientes_unicos
            ON pacientes_unicos_por_dia_prestador (Hospital, Atendimento, Paciente, Prestador, Data);
        """))
        # Índice único para cirurgias
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_cirurgias
            ON cirurgias (Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia);
        """))


# =============================================================================
# INIT DB (com UNIQUE constraints + PRAGMAs)
# =============================================================================

def init_db() -> None:
    """Cria tabelas caso não existam e aplica índices únicos e PRAGMAs."""
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    eng = get_engine()
    with eng.begin() as conn:
        # PRAGMAs úteis
        conn.execute(text("PRAGMA foreign_keys=ON"))
        conn.execute(text("PRAGMA journal_mode=WAL"))
        conn.execute(text("PRAGMA synchronous=NORMAL"))

        # Tabela base
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pacientes_unicos_por_dia_prestador (
                Hospital    TEXT,
                Ano         INTEGER,
                Mes         INTEGER,
                Dia         INTEGER,
                Data        TEXT,
                Atendimento TEXT,
                Paciente    TEXT,
                Aviso       TEXT,
                Convenio    TEXT,
                Prestador   TEXT,
                Quarto      TEXT,
                UNIQUE(Hospital, Atendimento, Paciente, Prestador, Data)
            );
        """))

        # Cirurgias
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cirurgias (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Hospital    TEXT,
                Atendimento TEXT,
                Paciente    TEXT,
                Prestador   TEXT,
                Data_Cirurgia TEXT,
                Convenio    TEXT,
                Procedimento_Tipo_ID INTEGER,
                Situacao_ID INTEGER,
                Guia_AMHPTISS TEXT,
                Guia_AMHPTISS_Complemento TEXT,
                Fatura TEXT,
                Observacoes TEXT,
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
            );
        """))

        # Catálogos
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS procedimento_tipos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome  TEXT UNIQUE,
                ativo INTEGER,
                ordem INTEGER
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cirurgia_situacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome  TEXT UNIQUE,
                ativo INTEGER,
                ordem INTEGER
            );
        """))

    ensure_unique_indexes()  # garante ON CONFLICT confiável


# =============================================================================
# RESET / MANUTENÇÃO
# =============================================================================

def vacuum() -> None:
    """
    Executa manutenção fora de transação:
      - PRAGMA wal_checkpoint(TRUNCATE)
      - VACUUM (requer write/exclusividade)
      - PRAGMA optimize
    Usa sqlite3 com conexão curta e sem transação.
    """
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(DB_PATH)

    ensure_db_writable()

    # descarte do engine atual para reduzir chance de "database is locked"
    dispose_engine()

    try:
        with sqlite3.connect(DB_PATH) as conn:
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
            conn.execute("VACUUM")
            try:
                conn.execute("PRAGMA optimize")
            except Exception:
                pass
    except sqlite3.OperationalError:
        # Propaga; o app decide ignorar se for read-only
        raise


def reset_db_file() -> None:
    """
    Remove o arquivo .db e recria o schema vazio.
    Útil para 'RESET TOTAL' na UI (versão simples).
    """
    dispose_engine()
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()


def hard_reset_local_db() -> None:
    """
    Fecha a engine, remove o arquivo .db e recria o schema vazio.
    Versão robusta do reset total.
    """
    dispose_engine()
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except Exception as e:
            raise RuntimeError(f"Falha ao remover {DB_PATH}: {e}")
    init_db()


def hard_reset_and_upload_to_github(upload_fn) -> bool:
    """
    Reset total local e upload da versão vazia para o GitHub.
    'upload_fn' é uma função que recebe (commit_msg) e executa o upload do DB_PATH.
    Retorna True se o upload foi OK, False caso contrário.
    """
    hard_reset_local_db()
    try:
        ok = upload_fn("Reset total: recria .db vazio")
        return bool(ok)
    except Exception:
        return False


def delete_all_pacientes() -> int:
    """Apaga todos os registros da base de pacientes; retorna a quantidade apagada."""
    ensure_db_writable()
    eng = get_engine()
    with eng.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM pacientes_unicos_por_dia_prestador")).scalar_one()
        conn.execute(text("DELETE FROM pacientes_unicos_por_dia_prestador"))
    return int(total or 0)


def delete_all_catalogos() -> int:
    """Apaga todos os catálogos (tipos e situações); retorna a soma das quantidades apagadas."""
    ensure_db_writable()
    eng = get_engine()
    with eng.begin() as conn:
        t1 = conn.execute(text("SELECT COUNT(*) FROM procedimento_tipos")).scalar_one() or 0
        t2 = conn.execute(text("SELECT COUNT(*) FROM cirurgia_situacoes")).scalar_one() or 0
        conn.execute(text("DELETE FROM procedimento_tipos"))
        conn.execute(text("DELETE FROM cirurgia_situacoes"))
    return int(t1) + int(t2)


def delete_all_cirurgias() -> int:
    """Apaga todas as cirurgias; retorna a quantidade apagada."""
    ensure_db_writable()
    eng = get_engine()
    with eng.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM cirurgias")).scalar_one()
        conn.execute(text("DELETE FROM cirurgias"))
    return int(total or 0)


# =============================================================================
# PACIENTES (UPSERT / LEITURAS)
# =============================================================================

def upsert_dataframe(df: pd.DataFrame) -> Tuple[int, int]:
    """
    Salva DataFrame na tabela base (pacientes_unicos_por_dia_prestador).
    Usa ON CONFLICT para atualizar Aviso, Convenio, Quarto.

    Regras:
    - Aceita Atendimento vazio se Paciente vier preenchido (e vice-versa).
    - Ignora linhas onde ambos (Atendimento e Paciente) estão vazios.
    - Retorna (linhas_salvas, linhas_ignoradas).
    """
    if df is None:
        return (0, 0)

    df = pd.DataFrame(df).copy()
    if df.empty:
        return (0, 0)

    ensure_unique_indexes()
    ensure_db_writable()

    # Normaliza colunas esperadas (defasadas viram vazias)
    for col in ["Hospital", "Data", "Atendimento", "Paciente", "Aviso", "Convenio", "Prestador", "Quarto"]:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").str.strip()
        else:
            df[col] = ""

    # Chave mínima: (Atendimento OU Paciente) + Hospital + Prestador + Data
    mask_key_missing = (df["Atendimento"] == "") & (df["Paciente"] == "")
    ignoradas = int(mask_key_missing.sum())
    df_valid = df[~mask_key_missing]

    if df_valid.empty:
        return (0, ignoradas)

    eng = get_engine()
    with eng.begin() as conn:
        for _, r in df_valid.iterrows():
            conn.execute(text("""
                INSERT INTO pacientes_unicos_por_dia_prestador
                (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
                VALUES
                (:Hospital, :Ano, :Mes, :Dia, :Data, :Atendimento, :Paciente, :Aviso, :Convenio, :Prestador, :Quarto)
                ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data)
                DO UPDATE SET
                    Aviso    = excluded.Aviso,
                    Convenio = excluded.Convenio,
                    Quarto   = excluded.Quarto
            """), {
                "Hospital": _safe_str(r.get("Hospital")),
                "Ano": _safe_int(r.get("Ano")),
                "Mes": _safe_int(r.get("Mes")),
                "Dia": _safe_int(r.get("Dia")),
                "Data": _safe_str(r.get("Data")),
                "Atendimento": _safe_str(r.get("Atendimento")),
                "Paciente": _safe_str(r.get("Paciente")),
                "Aviso": _safe_str(r.get("Aviso")),
                "Convenio": _safe_str(r.get("Convenio")),
                "Prestador": _safe_str(r.get("Prestador")),
                "Quarto": _safe_str(r.get("Quarto")),
            })

    return (len(df_valid), ignoradas)


def upsert_paciente_single(row: Dict[str, Any]) -> Tuple[int, int]:
    """
    UPSERT de 1 registro em pacientes_unicos_por_dia_prestador.
    Espera chaves: Hospital, Data (dd/MM/yyyy ou YYYY-MM-DD), Prestador
    e pelo menos um entre Atendimento/Paciente.

    Retorna (1, 0) se salvo; (0, 1) se ignorado por chave incompleta.
    """
    df = pd.DataFrame([row])

    # Deriva Ano/Mes/Dia de Data
    try:
        dt = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
        df["Ano"] = dt.dt.year
        df["Mes"] = dt.dt.month
        df["Dia"] = dt.dt.day
    except Exception:
        df["Ano"] = None
        df["Mes"] = None
        df["Dia"] = None

    return upsert_dataframe(df)


def delete_paciente_by_key(
    hospital: str,
    atendimento: str,
    paciente: str,
    prestador: str,
    data: str
) -> int:
    """
    Exclui 1 registro da base 'pacientes_unicos_por_dia_prestador' usando a chave única composta.
    Retorna o número de linhas afetadas (0 ou 1).
    """
    ensure_db_writable()
    eng = get_engine()
    with eng.begin() as conn:
        res = conn.execute(text("""
            DELETE FROM pacientes_unicos_por_dia_prestador
            WHERE Hospital=:h AND Atendimento=:a AND Paciente=:p AND Prestador=:pr AND Data=:d
        """), {
            "h": hospital.strip(), "a": atendimento.strip(), "p": paciente.strip(),
            "pr": prestador.strip(), "d": data.strip()
        })
        return res.rowcount or 0


def read_all() -> List[Tuple]:
    """Lê todos os registros da base, ordenados para exibição no app."""
    eng = get_engine()
    with eng.connect() as conn:
        return conn.execute(text("""
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento,
                   Paciente, Aviso, Convenio, Prestador, Quarto
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Hospital, Ano, Mes, Dia, Paciente, Prestador
        """)).fetchall()


def count_all() -> int:
    """Conta todas as linhas da base de pacientes."""
    eng = get_engine()
    with eng.connect() as conn:
        return int(conn.execute(text("SELECT COUNT(*) FROM pacientes_unicos_por_dia_prestador")).scalar_one() or 0)


# =============================================================================
# LEITURAS P/ ABA CIRURGIAS (BASE)
# =============================================================================

def _date_filter_clause(colname: str, ano: Optional[int], mes: Optional[int]) -> Tuple[str, dict]:
    """
    Monta filtro por ano/mês tolerante a 'dd/MM/yyyy' ou 'YYYY-MM'.
    """
    params = {}
    parts = []
    if ano is not None and mes is not None:
        parts.append(f"(({colname} LIKE :p1) OR ({colname} LIKE :p2))")
        params["p1"] = f"%/{mes:02d}/{ano}"
        params["p2"] = f"{ano}-{mes:02d}-%"
    elif ano is not None:
        parts.append(f"(({colname} LIKE :p3) OR ({colname} LIKE :p4))")
        params["p3"] = f"%/{ano}"
        params["p4"] = f"{ano}-%"
    clause = (" AND " + " AND ".join(parts)) if parts else ""
    return clause, params


def find_registros_para_prefill(
    hospital: str,
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    prestadores: Optional[Sequence[str]] = None
) -> List[Tuple]:
    """
    Retorna registros da tabela base para pré-preencher a Aba Cirurgias.
    Filtros opcionais: hospital (obrigatório), ano/mês, lista de prestadores.
    """
    if not hospital:
        return []

    where = ["Hospital = :h"]
    params = {"h": hospital}

    # Filtro por Data (aceita 'dd/MM/yyyy' e 'YYYY-MM')
    clause, p = _date_filter_clause("Data", ano, mes)
    if clause:
        where.append(clause[5:] if clause.startswith(" AND ") else clause)
        params.update(p)

    # Prestadores
    if prestadores:
        tokens = [str(p).strip() for p in prestadores if str(p).strip()]
        if tokens:
            in_params = {}
            placeholders = []
            for i, val in enumerate(tokens):
                key = f"pp{i}"
                in_params[key] = val
                placeholders.append(f":{key}")
            where.append(f"Prestador IN ({', '.join(placeholders)})")
            params.update(in_params)

    sql = f"""
        SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
        FROM pacientes_unicos_por_dia_prestador
        WHERE {' AND '.join(where)}
        ORDER BY Data, Prestador, Atendimento, Paciente
    """
    eng = get_engine()
    with eng.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


def list_registros_base_all(limit: int = 500) -> List[Tuple]:
    """
    Lista registros da base para diagnóstico rápido (limite configurável).
    """
    limit = int(limit or 500)
    eng = get_engine()
    with eng.connect() as conn:
        return conn.execute(text(f"""
            SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Data DESC, Prestador, Atendimento, Paciente
            LIMIT {limit}
        """)).fetchall()


# =============================================================================
# CATÁLOGOS (Tipos e Situações)
# =============================================================================

def list_procedimento_tipos(only_active: bool = True) -> List[Tuple]:
    eng = get_engine()
    sql = "SELECT id, nome, ativo, ordem FROM procedimento_tipos"
    if only_active:
        sql += " WHERE ativo=1"
    sql += " ORDER BY ordem, nome"
    with eng.connect() as conn:
        return conn.execute(text(sql)).fetchall()


def upsert_procedimento_tipo(nome: str, ativo: int = 1, ordem: int = 1) -> int:
    ensure_unique_indexes()
    ensure_db_writable()
    eng = get_engine()
    nome = _safe_str(nome)
    with eng.begin() as conn:
        conn.execute(text("""
            INSERT INTO procedimento_tipos (nome, ativo, ordem)
            VALUES (:nome, :ativo, :ordem)
            ON CONFLICT(nome) DO UPDATE SET ativo=excluded.ativo, ordem=excluded.ordem
        """), {"nome": nome, "ativo": int(ativo), "ordem": int(ordem)})
        row = conn.execute(text("SELECT id FROM procedimento_tipos WHERE nome=:n"), {"n": nome}).fetchone()
        return int(row[0]) if row else 0


def set_procedimento_tipo_status(tid: int, ativo: int) -> None:
    ensure_db_writable()
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("UPDATE procedimento_tipos SET ativo=:a WHERE id=:i"), {"a": int(ativo), "i": int(tid)})


def list_cirurgia_situacoes(only_active: bool = True) -> List[Tuple]:
    eng = get_engine()
    sql = "SELECT id, nome, ativo, ordem FROM cirurgia_situacoes"
    if only_active:
        sql += " WHERE ativo=1"
    sql += " ORDER BY ordem, nome"
    with eng.connect() as conn:
        return conn.execute(text(sql)).fetchall()


def upsert_cirurgia_situacao(nome: str, ativo: int = 1, ordem: int = 1) -> int:
    ensure_unique_indexes()
    ensure_db_writable()
    eng = get_engine()
    nome = _safe_str(nome)
    with eng.begin() as conn:
        conn.execute(text("""
            INSERT INTO cirurgia_situacoes (nome, ativo, ordem)
            VALUES (:nome, :ativo, :ordem)
            ON CONFLICT(nome) DO UPDATE SET ativo=excluded.ativo, ordem=excluded.ordem
        """), {"nome": nome, "ativo": int(ativo), "ordem": int(ordem)})
        row = conn.execute(text("SELECT id FROM cirurgia_situacoes WHERE nome=:n"), {"n": nome}).fetchone()
        return int(row[0]) if row else 0


def set_cirurgia_situacao_status(sid: int, ativo: int) -> None:
    ensure_db_writable()
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("UPDATE cirurgia_situacoes SET ativo=:a WHERE id=:i"), {"a": int(ativo), "i": int(sid)})


# =============================================================================
# CIRURGIAS (UPSERT / LISTA / DELETE)
# =============================================================================

def insert_or_update_cirurgia(payload: Dict[str, Any]) -> int:
    """
    UPSERT de cirurgia. A chave é:
    (Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
    Observação: Aceita Atendimento vazio se Paciente vier preenchido (e vice-versa).
    """
    ensure_unique_indexes()
    ensure_db_writable()

    h = _safe_str(payload.get("Hospital"))
    att = _safe_str(payload.get("Atendimento"), "")
    pac = _safe_str(payload.get("Paciente"), "")
    p = _safe_str(payload.get("Prestador"))
    d = _safe_str(payload.get("Data_Cirurgia"))
    if not h or not p or not d or (not att and not pac):
        raise ValueError("Chave mínima inválida para cirurgia.")

    now = datetime.now().isoformat(timespec="seconds")
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("""
            INSERT INTO cirurgias (
                Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
                Convenio, Procedimento_Tipo_ID, Situacao_ID,
                Guia_AMHPTISS, Guia_AMHPTISS_Complemento,
                Fatura, Observacoes, created_at, updated_at
            )
            VALUES (
                :Hospital, :Atendimento, :Paciente, :Prestador, :Data,
                :Convenio, :TipoID, :SitID,
                :Guia, :GuiaC, :Fatura, :Obs, :created, :updated
            )
            ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
            DO UPDATE SET
                Convenio=excluded.Convenio,
                Procedimento_Tipo_ID=excluded.Procedimento_Tipo_ID,
                Situacao_ID=excluded.Situacao_ID,
                Guia_AMHPTISS=excluded.Guia_AMHPTISS,
                Guia_AMHPTISS_Complemento=excluded.Guia_AMHPTISS_Complemento,
                Fatura=excluded.Fatura,
                Observacoes=excluded.Observacoes,
                updated_at=excluded.updated_at
        """), {
            "Hospital": h, "Atendimento": att, "Paciente": pac, "Prestador": p, "Data": d,
            "Convenio": _safe_str(payload.get("Convenio")),
            "TipoID": payload.get("Procedimento_Tipo_ID"),
            "SitID": payload.get("Situacao_ID"),
            "Guia": _safe_str(payload.get("Guia_AMHPTISS")),
            "GuiaC": _safe_str(payload.get("Guia_AMHPTISS_Complemento")),
            "Fatura": _safe_str(payload.get("Fatura")),
            "Obs": _safe_str(payload.get("Observacoes")),
            "created": now, "updated": now
        })
        row = conn.execute(text("""
            SELECT id FROM cirurgias
            WHERE Hospital=:h AND Atendimento=:a AND Paciente=:p AND Prestador=:pr AND Data_Cirurgia=:d
        """), {"h": h, "a": att, "p": pac, "pr": p, "d": d}).fetchone()
        return int(row[0]) if row else 0


def _ano_mes_clause_for_cirurgias(ano_mes: Optional[str]) -> Tuple[str, dict]:
    """
    Monta filtro por Ano/Mês para 'Data_Cirurgia' aceitando:
      - 'YYYY-MM%' (ISO-like)
      - '%/MM/YYYY' (brasileiro com dia primeiro)
    """
    if not ano_mes:
        return "", {}
    try:
        year_str, month_str = ano_mes.split("-", 1)
        year = int(year_str)
        month = int(month_str)
    except Exception:
        # formato inesperado; usa LIKE direto
        return " AND Data_Cirurgia LIKE :dmx", {"dmx": f"{ano_mes}%"}

    return " AND ((Data_Cirurgia LIKE :dm1) OR (Data_Cirurgia LIKE :dm2))", {
        "dm1": f"{year}-{month:02d}-%",    # ISO-like
        "dm2": f"%/{month:02d}/{year}"     # BR-like
    }


def list_cirurgias(
    hospital: Optional[str] = None,
    ano_mes: Optional[str] = None,
    prestador: Optional[str] = None
) -> List[Tuple]:
    """
    Lista cirurgias com filtros opcionais:
      - hospital: exato
      - ano_mes: 'YYYY-MM' (aceita ambas formas ao persistir)
      - prestador: exato (se informado)
    """
    clauses = []
    params: Dict[str, Any] = {}

    if hospital:
        clauses.append("Hospital=:h")
        params["h"] = hospital

    if prestador:
        clauses.append("Prestador=:p")
        params["p"] = prestador

    # Filtro Ano/Mês para Data_Cirurgia
    ano_mes_clause, ano_mes_params = _ano_mes_clause_for_cirurgias(ano_mes)
    where = " AND ".join(clauses)
    if where:
        where = " WHERE " + where
        if ano_mes_clause:
            where += ano_mes_clause
            params.update(ano_mes_params)
    else:
        if ano_mes_clause:
            where = " WHERE 1=1 " + ano_mes_clause
            params.update(ano_mes_params)

    sql = f"""
        SELECT id, Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
               Convenio, Procedimento_Tipo_ID, Situacao_ID,
               Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura,
               Observacoes, created_at, updated_at
        FROM cirurgias {where}
        ORDER BY Data_Cirurgia, Prestador, Atendimento, Paciente
    """
    eng = get_engine()
    with eng.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


def delete_cirurgia(cirurgia_id: int) -> int:
    ensure_db_writable()
    eng = get_engine()
    with eng.begin() as conn:
        res = conn.execute(text("DELETE FROM cirurgias WHERE id=:i"), {"i": int(cirurgia_id)})
        return res.rowcount or 0


def delete_cirurgia_by_key(
    hospital: str,
    atendimento: str,
    paciente: str,
    prestador: str,
    data_cirurgia: str
) -> int:
    """
    Exclui 1 registro de 'cirurgias' usando a chave única composta.
    Retorna o número de linhas afetadas (0 ou 1).
    """
    ensure_db_writable()
    eng = get_engine()
    with eng.begin() as conn:
        res = conn.execute(text("""
            DELETE FROM cirurgias
            WHERE Hospital=:h AND Atendimento=:a AND Paciente=:p AND Prestador=:pr AND Data_Cirurgia=:d
        """), {
            "h": hospital.strip(), "a": atendimento.strip(), "p": paciente.strip(),
            "pr": prestador.strip(), "d": data_cirurgia.strip()
        })
        return res.rowcount or 0


def delete_cirurgias_by_filter(
    hospital: str,
    atendimentos: Optional[Sequence[str]] = None,
    prestadores: Optional[Sequence[str]] = None,
    datas: Optional[Sequence[str]] = None
) -> int:
    """
    Exclui em lote usando filtros (Hospital obrigatório; Atendimento/Prestador/Data opcionais).
    Datas aceitam formato livre, mas devem bater com o que está persistido (ex.: 'dd/MM/yyyy').
    Retorna total de linhas apagadas.
    """
    ensure_db_writable()

    clauses = ["Hospital=:h"]
    params: Dict[str, Any] = {"h": hospital.strip()}

    def _add_in(field: str, values: Optional[Sequence[str]], key_prefix: str):
        if values:
            vals = [str(v).strip() for v in values if str(v).strip()]
            if vals:
                phs, dct = [], {}
                for i, val in enumerate(vals):
                    k = f"{key_prefix}{i}"
                    dct[k] = val
                    phs.append(f":{k}")
                clauses.append(f"{field} IN ({', '.join(phs)})")
                params.update(dct)

    _add_in("Atendimento", atendimentos, "att")
    _add_in("Prestador", prestadores, "pr")
    _add_in("Data_Cirurgia", datas, "dt")

    where = " AND ".join(clauses)
    sql = f"DELETE FROM cirurgias WHERE {where}"
    eng = get_engine()
    with eng.begin() as conn:
        res = conn.execute(text(sql), params)
        return res.rowcount or 0
