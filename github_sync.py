
# -*- coding: utf-8 -*-
"""
github_sync.py — Sincronização do arquivo SQLite (.db) com o GitHub (Contents API),
com controle de versão via 'sha', preflight GET (create vs update) e
merge automático em caso de conflito (409) com retorno detalhado.

Funcionalidades:
- download_db_from_github(..., return_sha=False) -> bool ou (bool, sha)
- upload_db_to_github(..., prev_sha=None, _return_details=False) -> bool ou (ok, new_sha, status, message)
- safe_upload_with_merge(..., _return_details=False) -> bool ou (ok, status, message)

PATCHES:
- ✅ Checkpoint do WAL antes de ler/enviar o arquivo (garante que o .db reflita o estado atual).
- ✅ Função get_remote_sha(...) para atualizar o SHA remoto no app após upload bem-sucedido.
"""

import base64
import json
import os
import shutil
import tempfile
import sqlite3  # ✅ novo
from typing import Optional, Tuple, Union

# Importa a função de merge do módulo externo
from db_merge import merge_sqlite_dbs

# HTTP: usa 'requests' se disponível; senão, 'urllib'
try:
    import requests
    _HAS_REQUESTS = True
except Exception:
    _HAS_REQUESTS = False
    import urllib.request
    import urllib.error


# =========================
# Helpers de Token/Headers
# =========================

def _resolve_token(token: Optional[str]) -> Optional[str]:
    """
    Resolve token a partir de:
    - parâmetro explícito
    - st.secrets["GITHUB_TOKEN"] (se streamlit presente e segredo definido)
    - os.environ["GITHUB_TOKEN"]
    """
    if token:
        return token
    try:
        import streamlit as st
        tok = st.secrets.get("GITHUB_TOKEN")
        if tok:
            return tok
    except Exception:
        pass
    return os.environ.get("GITHUB_TOKEN")


def _gh_headers(token: Optional[str]) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "github-sync-sqlite/1.2",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _http_get(url: str, headers: dict) -> Tuple[int, bytes]:
    if _HAS_REQUESTS:
        resp = requests.get(url, headers=headers)
        return resp.status_code, resp.content
    # urllib fallback
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.getcode(), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError:
        return 0, b""


def _http_put_json(url: str, headers: dict, payload: dict) -> Tuple[int, bytes]:
    body = json.dumps(payload).encode("utf-8")
    hdrs = dict(headers)
    hdrs["Content-Type"] = "application/json"
    if _HAS_REQUESTS:
        resp = requests.put(url, headers=hdrs, data=json.dumps(payload))
        return resp.status_code, resp.content
    # urllib fallback
    req = urllib.request.Request(url, headers=hdrs, data=body, method="PUT")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.getcode(), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError:
        return 0, b""


# =========================
# WAL checkpoint helper (✅ novo)
# =========================

def _checkpoint_sqlite(path: str) -> None:
    """
    Garante que todas as transações em WAL sejam descarregadas no arquivo principal .db.
    Usa TRUNCATE para limpar o .wal após aplicar. Tenta descartar conexões do SQLAlchemy.
    """
    try:
        # Tenta descartar conexões do SQLAlchemy para evitar "database is locked"
        try:
            from db import dispose_engine  # depende do seu próprio módulo db.py
            dispose_engine()
        except Exception:
            pass

        with sqlite3.connect(path) as conn:
            # FULL garantiria aplicar tudo; TRUNCATE aplica e limpa o .wal
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            try:
                conn.execute("PRAGMA optimize")
            except Exception:
                pass
    except Exception:
        # Não bloqueia o upload se checkpoint falhar; apenas evita crash
        pass


# =========================
# Download do .db (Contents API)
# =========================

def download_db_from_github(
    owner: str,
    repo: str,
    path_in_repo: str,
    branch: str,
    local_db_path: str,
    token: Optional[str] = None,
    return_sha: bool = False
) -> Union[bool, Tuple[bool, Optional[str]]]:
    """
    Baixa um arquivo binário do repositório GitHub (Contents API) e salva em 'local_db_path'.
    Se o arquivo não existir no repo/branch, retorna False (e None se return_sha=True).
    """
    token = _resolve_token(token)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_in_repo}?ref={branch}"
    status, content = _http_get(url, _gh_headers(token))
    if status == 404:
        return (False, None) if return_sha else False
    if status != 200:
        return (False, None) if return_sha else False

    try:
        data = json.loads(content.decode("utf-8"))
    except Exception:
        return (False, None) if return_sha else False

    # 'content' vem base64; 'sha' contém a versão atual do blob
    content_b64 = data.get("content")
    if not content_b64:
        return (False, None) if return_sha else False

    blob = base64.b64decode(content_b64)
    os.makedirs(os.path.dirname(local_db_path), exist_ok=True)
    with open(local_db_path, "wb") as f:
        f.write(blob)

    sha = data.get("sha")
    return (True, sha) if return_sha else True


# =========================
# Upload do .db (Contents API) com preflight GET
# =========================

def upload_db_to_github(
    owner: str,
    repo: str,
    path_in_repo: str,
    branch: str,
    local_db_path: str,
    commit_message: str,
    token: Optional[str] = None,
    prev_sha: Optional[str] = None,
    _return_details: bool = False
) -> Union[bool, Tuple[bool, Optional[str], int, str]]:
    """
    Faz upload (PUT) do arquivo local para o GitHub (Contents API).
    - Se 'prev_sha' for informado, tenta update diretamente com esse sha.
    - Se 'prev_sha' for None, primeiro faz GET para descobrir se o arquivo existe:
        * 200: arquivo existe -> usa sha do remoto no payload (update)
        * 404: arquivo não existe -> cria (sem sha)
    - Retorna:
        * _return_details=False: bool
        * _return_details=True: (ok, new_sha, status_code, message)
    """
    token = _resolve_token(token)
    url_put = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_in_repo}"

    if not os.path.exists(local_db_path):
        msg = "Local db file not found"
        return (False, None, 0, msg) if _return_details else False

    # ✅ Força checkpoint do WAL antes de ler o arquivo
    _checkpoint_sqlite(local_db_path)

    # Lê arquivo local
    with open(local_db_path, "rb") as f:
        raw = f.read()
    if len(raw) == 0:
        msg = "Local db file is empty (0 bytes)"
        return (False, None, 422, msg) if _return_details else False

    b64 = base64.b64encode(raw).decode("utf-8")

    # Decide entre create/update
    sha_to_use = prev_sha
    if not sha_to_use:
        # Descobre se o arquivo existe
        url_get = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_in_repo}?ref={branch}"
        status_get, content_get = _http_get(url_get, _gh_headers(token))
        if status_get == 200:
            try:
                data_get = json.loads(content_get.decode("utf-8"))
                sha_to_use = data_get.get("sha")
            except Exception:
                sha_to_use = None
        elif status_get == 404:
            sha_to_use = None
        else:
            # Falha em descobrir; retorne erro detalhado
            msg = f"Preflight GET failed (status={status_get})"
            return (False, None, status_get, msg) if _return_details else False

    payload = {
        "message": commit_message,
        "content": b64,
        "branch": branch,
    }
    if sha_to_use:
        payload["sha"] = sha_to_use  # update
    # else: create

    status_put, content_put = _http_put_json(url_put, _gh_headers(token), payload)
    if status_put in (200, 201):
        try:
            data_put = json.loads(content_put.decode("utf-8"))
            new_sha = (data_put.get("content") or {}).get("sha")
        except Exception:
            new_sha = None
        return (True, new_sha, status_put, "OK") if _return_details else True

    # Retorna a mensagem do GitHub (body) para diagnóstico
    try:
        msg = content_put.decode("utf-8")
    except Exception:
        msg = ""
    return (False, None, status_put, msg) if _return_details else False


# =========================
# Helper para recuperar SHA remoto (✅ novo)
# =========================

def get_remote_sha(
    owner: str,
    repo: str,
    path_in_repo: str,
    branch: str,
    token: Optional[str] = None
) -> Optional[str]:
    """
    Retorna o SHA atual do blob no GitHub sem baixar o arquivo.
    Útil para atualizar o st.session_state['gh_sha'] após upload bem-sucedido.
    """
    token = _resolve_token(token)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_in_repo}?ref={branch}"
    status, content = _http_get(url, _gh_headers(token))
    if status != 200:
        return None
    try:
        data = json.loads(content.decode("utf-8"))
        return data.get("sha")
    except Exception:
        return None


# =========================
# Upload seguro com merge automático e retorno detalhado
# =========================

def safe_upload_with_merge(
    owner: str,
    repo: str,
    path_in_repo: str,
    branch: str,
    local_db_path: str,
    commit_message: str,
    token: Optional[str] = None,
    prev_sha: Optional[str] = None,
    _return_details: bool = False
) -> Union[bool, Tuple[bool, int, str]]:
    """
    Tenta upload com 'prev_sha'. Em conflito (409):
      1) Baixa remoto (pega 'remote_sha2')
      2) Mescla local+remoto (merge_sqlite_dbs)
      3) Substitui local pelo mesclado
      4) Reenvia com prev_sha atualizado

    Retorno:
      - se _return_details=False: bool
      - se _return_details=True: (ok: bool, status: int, message: str)
    """
    # Tentativa inicial (já inclui checkpoint dentro do upload_db_to_github)
    ok, new_sha, status, msg = upload_db_to_github(
        owner, repo, path_in_repo, branch, local_db_path, commit_message,
        token=token, prev_sha=prev_sha, _return_details=True
    )
    if ok and new_sha:
        return (True, status, "Upload OK") if _return_details else True

    if status == 409:
        # Conflito → baixar remoto e mesclar
        tmp_remote = tempfile.NamedTemporaryFile(prefix="remote_", suffix=".db", delete=False)
        tmp_remote_path = tmp_remote.name
        tmp_remote.close()

        downloaded, remote_sha2 = download_db_from_github(
            owner, repo, path_in_repo, branch, tmp_remote_path, token=token, return_sha=True
        )
        if not downloaded:
            # não conseguiu baixar remoto
            try: os.unlink(tmp_remote_path)
            except Exception: pass
            msg2 = "Conflito 409, mas falha ao baixar remoto"
            return (False, 409, msg2) if _return_details else False

        tmp_merged = tempfile.NamedTemporaryFile(prefix="merged_", suffix=".db", delete=False)
        tmp_merged_path = tmp_merged.name
        tmp_merged.close()

        try:
            merge_sqlite_dbs(local_db_path, tmp_remote_path, tmp_merged_path)
            # substitui local
            shutil.move(tmp_merged_path, local_db_path)
        except Exception as e:
            # Falha no merge
            try: os.unlink(tmp_merged_path)
            except Exception: pass
            try: os.unlink(tmp_remote_path)
            except Exception: pass
            msg2 = f"Falha no merge: {e}"
            return (False, 409, msg2) if _return_details else False

        # Reenvia com sha atualizado do remoto
        ok2, new_sha2, status2, msg2 = upload_db_to_github(
            owner, repo, path_in_repo, branch, local_db_path,
            f"{commit_message} (merge automático)", token=token, prev_sha=remote_sha2,
            _return_details=True
        )

        try: os.unlink(tmp_remote_path)
        except Exception: pass

        if ok2 and new_sha2:
            return (True, status2, "Upload após merge OK") if _return_details else True

        return (False, status2, f"Falha ao subir após merge: {msg2}") if _return_details else False

    # Outros erros (inclui 422) 
    return (False, status, f"Falha inicial de upload: {msg}") if _return_details else False
