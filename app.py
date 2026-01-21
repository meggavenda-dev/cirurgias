
# -*- coding: utf-8 -*-
import os
from datetime import datetime
from io import BytesIO
import streamlit as st
import pandas as pd

from db import (
    # Básico / manutenção
    init_db, ensure_db_writable, vacuum, DB_PATH,

    # Base (pacientes)
    upsert_dataframe, read_all, count_all,
    find_registros_para_prefill, list_registros_base_all,
    delete_all_pacientes, upsert_paciente_single, delete_paciente_by_key,

    # Catálogos
    list_procedimento_tipos, set_procedimento_tipo_status, upsert_procedimento_tipo,
    list_cirurgia_situacoes, set_cirurgia_situacao_status, upsert_cirurgia_situacao,

    # Cirurgias
    list_cirurgias, insert_or_update_cirurgia, delete_cirurgia,
    delete_cirurgia_by_key, delete_cirurgias_by_filter,

    # Resets
    delete_all_cirurgias, delete_all_catalogos, hard_reset_local_db,
)
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital, to_formatted_excel_cirurgias

# --- GitHub sync (baixar/subir o .db) ---
try:
    from github_sync import (
        download_db_from_github,
        safe_upload_with_merge,
        upload_db_to_github,
        get_remote_sha,  # usado para atualizar SHA após upload
    )
    GITHUB_SYNC_AVAILABLE = True
except Exception:
    GITHUB_SYNC_AVAILABLE = False

# ---- Config GitHub (usa st.secrets; sem UI) ----
GH_OWNER = st.secrets.get("GH_OWNER", "seu-usuario-ou-org")
GH_REPO  = st.secrets.get("GH_REPO", "seu-repo")
GH_BRANCH = st.secrets.get("GH_BRANCH", "main")
GH_PATH_IN_REPO = st.secrets.get("GH_DB_PATH", "data/exemplo.db")
GITHUB_TOKEN_OK = bool(st.secrets.get("GITHUB_TOKEN", ""))

# ---- Versão de cache do app (mude quando alterar a lógica de processamento) ----
APP_CACHE_VERSION = "v1.0.0"

st.set_page_config(page_title="Gestão de Pacientes e Cirurgias", layout="wide")

# ---------------------------
# Boot cleanup do cache de dados (uma vez por sessão)
# ---------------------------
def _boot_session_cleanup():
    st.cache_data.clear()

if not st.session_state.get("_boot_cleanup_done"):
    _boot_session_cleanup()
    st.session_state["_boot_cleanup_done"] = True

# ---------------------------
# Helper: VACUUM seguro + limpeza de cache pós-manutenção
# ---------------------------
def try_vacuum_safely():
    """Tenta executar VACUUM; se DB estiver read-only, não interrompe a UI. Limpa cache de dados após sucesso."""
    try:
        ensure_db_writable()
        vacuum()
        st.cache_data.clear()
        st.caption("VACUUM + checkpoint executados.")
    except Exception as e:
        msg = str(e).lower()
        if "readonly" in msg or "read-only" in msg:
            st.warning("VACUUM não pôde ser executado (banco read-only agora). Prosseguindo sem VACUUM.")
        else:
            st.info("Não foi possível executar VACUUM agora.")
            st.exception(e)

# =========================
# Startup: baixar .db se não existir ou se parecer "vazio" (apenas schema)
# =========================
def _should_bootstrap_from_github(db_path: str, size_threshold_bytes: int = 10_000) -> bool:
    if not os.path.exists(db_path):
        return True
    try:
        return os.path.getsize(db_path) < size_threshold_bytes
    except Exception:
        return True

if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
    needs_bootstrap = _should_bootstrap_from_github(DB_PATH, size_threshold_bytes=10_000)
    if needs_bootstrap and not st.session_state.get("gh_db_fetched"):
        try:
            downloaded, remote_sha = download_db_from_github(
                owner=GH_OWNER,
                repo=GH_REPO,
                path_in_repo=GH_PATH_IN_REPO,
                branch=GH_BRANCH,
                local_db_path=DB_PATH,
                return_sha=True
            )
            if downloaded:
                st.cache_data.clear()
                st.session_state["gh_sha"] = remote_sha
                st.session_state["gh_db_fetched"] = True
                st.success("Banco baixado do GitHub na inicialização (bootstrap).")
                st.rerun()
            else:
                st.info("Banco ainda não existe no GitHub. Um novo será criado localmente ao salvar.")
        except Exception as e:
            st.error("Erro ao sincronizar inicialização com GitHub.")
            st.exception(e)
            st.session_state["gh_db_fetched"] = True

# =======================
# Sidebar: Sincronização + Diagnóstico
# =======================
with st.sidebar:
    st.markdown("### Sincronização GitHub")
    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
        if st.button("🔽 Baixar banco do GitHub (manual)"):
            try:
                downloaded, remote_sha = download_db_from_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    return_sha=True
                )
                if downloaded:
                    st.cache_data.clear()
                    st.success("Banco baixado do GitHub (manual).")
                    st.session_state["gh_sha"] = remote_sha
                    st.session_state["gh_db_fetched"] = True
                    st.rerun()
                else:
                    st.info("Arquivo não existe no repositório.")
            except Exception as e:
                st.error("Falha ao baixar do GitHub.")
                st.exception(e)
    else:
        st.info("GitHub sync desativado (sem token).")

    st.markdown("---")
    st.caption("Diagnóstico de versão")
    st.write(f"Versão atual (SHA): `{st.session_state.get('gh_sha', 'desconhecida')}`")
    if os.path.exists(DB_PATH):
        try:
            st.write(f"Tamanho do .db: {os.path.getsize(DB_PATH)} bytes")
            db_dir = os.path.dirname(DB_PATH) or "."
            st.caption(f"DB_DIR: {db_dir}")
            st.caption(f"Diretório gravável: {os.access(db_dir, os.W_OK)}")
        except Exception:
            pass

# =======================
# 🧨 Área de risco (Reset)
# =======================
with st.sidebar:
    st.markdown("---")
    st.markdown("### 🧨 Área de risco (Reset)")
    st.caption("Atenção: ações destrutivas. Exporte o Excel para backup antes.")

    confirmar = st.checkbox("Eu entendo que isso **não pode ser desfeito**.")
    confirma_texto = st.text_input("Digite **RESET** para confirmar:", value="")

    def _sync_after_reset(commit_message: str):
        """Para resets: sobe a versão local para o GitHub com detalhes."""
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            try:
                ok, new_sha, status, msg = upload_db_to_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_message,
                    prev_sha=st.session_state.get("gh_sha"),
                    _return_details=True
                )
                if ok:
                    st.session_state["gh_sha"] = new_sha or st.session_state.get("gh_sha")
                    st.success("Sincronização automática com GitHub concluída.")
                else:
                    st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
            except Exception as e:
                st.error("Falha ao sincronizar com GitHub.")
                st.exception(e)

    can_execute = confirmar and (confirma_texto.strip().upper() == "RESET")

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("Apagar **PACIENTES** (tabela base)", type="secondary", disabled=not can_execute):
            try:
                ensure_db_writable()
                apagados = delete_all_pacientes()
                st.cache_data.clear()
                try_vacuum_safely()
                st.success(f"✅ {apagados} paciente(s) apagado(s) do banco.")
                _sync_after_reset(f"Reset: apaga {apagados} pacientes")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar pacientes.")
                st.exception(e)

    with col_r2:
        if st.button("Apagar **CIRURGIAS**", type="secondary", disabled=not can_execute):
            try:
                ensure_db_writable()
                apagadas = delete_all_cirurgias()
                st.cache_data.clear()
                try_vacuum_safely()
                st.session_state.pop("editor_lista_cirurgias_union", None)
                st.success(f"✅ {apagadas} cirurgia(s) apagada(s) do banco.")
                _sync_after_reset(f"Reset: apaga {apagadas} cirurgias")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar cirurgias.")
                st.exception(e)

    col_r3, col_r4 = st.columns(2)
    with col_r3:
        if st.button("Apagar **CATÁLOGOS** (Tipos/Situações)", type="secondary", disabled=not can_execute):
            try:
                ensure_db_writable()
                apagados = delete_all_catalogos()
                st.cache_data.clear()
                try_vacuum_safely()
                st.success(f"✅ {apagados} registro(s) apagado(s) dos catálogos.")
                _sync_after_reset(f"Reset: apaga {apagados} catálogos")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar catálogos.")
                st.exception(e)

    with col_r4:
        if st.button("🗑️ **RESET TOTAL** (apaga arquivo .db)", type="primary", disabled=not can_execute):
            try:
                hard_reset_local_db()
                st.cache_data.clear()
                st.success("Banco recriado vazio (local).")
                _sync_after_reset("Reset total: recria .db vazio")
                st.session_state["gh_db_fetched"] = True
                st.rerun()
            except Exception as e:
                st.error("Falha no reset total.")
                st.exception(e)

# Inicializa DB
init_db()

# -----------------------------------------------------------------
# Cache de dados (com TTL + versão) para processamento de arquivo
# -----------------------------------------------------------------
@st.cache_data(ttl=900, show_spinner=False)
def _process_file_cached(file_bytes: bytes, file_name: str, prestadores_lista: list, hospital: str,
                         upload_id: str, app_cache_version: str) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    try:
        bio.name = file_name or "upload.bin"
    except Exception:
        pass
    df = process_uploaded_file(bio, prestadores_lista, hospital)
    return pd.DataFrame(df) if df is not None else pd.DataFrame()

# Diagnóstico rápido do arquivo .db
with st.expander("🔎 Diagnóstico do arquivo .db (local)", expanded=False):
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else 0
    st.caption(f"Caminho: `{DB_PATH}` | Existe: {exists} | Tamanho: {size} bytes | Linhas (pacientes): {count_all()}")

# Lista única de hospitais
HOSPITAL_OPCOES = [
    "Hospital Santa Lucia Sul",
    "Hospital Santa Lucia Norte",
    "Hospital Maria Auxiliadora",
]

# ---------------- Abas ----------------
tabs = st.tabs([
    "📥 Importação & Pacientes",
    "🩺 Cirurgias",
    "📚 Cadastro (Tipos & Situações)",
    "📄 Tipos (Lista)"
])

# ====================================================================================
# 📥 Aba 1: Importação & Pacientes
# ====================================================================================
with tabs[0]:
    st.subheader("Pacientes únicos por data, prestador e hospital")
    st.caption("Upload → processamento cacheado (TTL) → Revisar/Editar/Selecionar → Salvar apenas selecionados → Exportar → sync GitHub")

    st.markdown("#### Prestadores alvo")
    prestadores_default = ["JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"]
    prestadores_text = st.text_area(
        "Informe os prestadores (um por linha)",
        value="\n".join(prestadores_default),
        height=120,
        help="A lista é usada para filtrar os registros. A comparação é case-insensitive."
    )
    prestadores_lista = [p.strip() for p in prestadores_text.splitlines() if p.strip()]

    st.markdown("#### Hospital deste arquivo")
    selected_hospital = st.selectbox(
        "Selecione o Hospital referente à planilha enviada",
        options=HOSPITAL_OPCOES,
        index=0,
        help="Aplicado a todas as linhas processadas deste arquivo."
    )

    st.markdown("#### Upload de planilha (CSV ou Excel)")
    uploaded_file = st.file_uploader(
        "Escolha o arquivo",
        type=["csv", "xlsx", "xls"],
        help="Aceita CSV 'bruto' ou planilhas estruturadas."
    )

    if "pacientes_select_default" not in st.session_state:
        st.session_state["pacientes_select_default"] = True

    if "df_final" not in st.session_state:
        st.session_state.df_final = None
    if "last_upload_id" not in st.session_state:
        st.session_state.last_upload_id = None
    if "editor_key" not in st.session_state:
        st.session_state.editor_key = "editor_pacientes_initial"

    def _make_upload_id(file, hospital: str) -> str:
        name = getattr(file, "name", "sem_nome")
        size = getattr(file, "size", 0)
        return f"{name}-{size}-{hospital.strip()}"

    col_reset1, col_reset2 = st.columns(2)
    with col_reset1:
        if st.button("🧹 Limpar tabela / reset"):
            st.session_state.df_final = None
            st.session_state.last_upload_id = None
            st.session_state.editor_key = "editor_pacientes_reset"
            st.success("Tabela limpa. Faça novo upload para reprocessar.")
    with col_reset2:
        col_sel_all, col_sel_none = st.columns(2)
        with col_sel_all:
            if st.button("✅ Selecionar todos"):
                st.session_state["pacientes_select_default"] = True
                st.success("Todos marcados para importar.")
                st.rerun()
        with col_sel_none:
            if st.button("❌ Desmarcar todos"):
                st.session_state["pacientes_select_default"] = False
                st.info("Todos desmarcados; escolha manualmente quem importar.")
                st.rerun()

    if uploaded_file is not None:
        current_upload_id = _make_upload_id(uploaded_file, selected_hospital)
        if st.session_state.last_upload_id != current_upload_id:
            st.cache_data.clear()
            st.session_state.df_final = None
            st.session_state.editor_key = f"editor_pacientes_{current_upload_id}"
            st.session_state.last_upload_id = current_upload_id

        with st.spinner("Processando arquivo (cacheado com TTL)..."):
            try:
                file_name = getattr(uploaded_file, "name", "upload.bin")
                file_bytes = uploaded_file.getvalue()
                df_final = _process_file_cached(
                    file_bytes=file_bytes,
                    file_name=file_name,
                    prestadores_lista=prestadores_lista,
                    hospital=selected_hospital.strip(),
                    upload_id=current_upload_id,
                    app_cache_version=APP_CACHE_VERSION
                )
                if df_final is None or len(df_final) == 0:
                    st.warning("Nenhuma linha após processamento. Verifique a lista de prestadores e o conteúdo do arquivo.")
                    st.session_state.df_final = None
                else:
                    st.session_state.df_final = df_final
            except Exception as e:
                st.error("Falha ao processar o arquivo.")
                st.exception(e)

    if st.session_state.df_final is not None and len(st.session_state.df_final) > 0:
        st.success(f"Processamento concluído! Linhas: {len(st.session_state.df_final)}")

        st.markdown("#### Revisar, editar e selecionar pacientes")
        df_to_edit = st.session_state.df_final.sort_values(
            ["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]
        ).reset_index(drop=True)

        default_select = bool(st.session_state.get("pacientes_select_default", True))
        if "Selecionar" not in df_to_edit.columns:
            df_to_edit["Selecionar"] = default_select
        else:
            df_to_edit["Selecionar"] = df_to_edit["Selecionar"].fillna(default_select).astype(bool)

        edited_df = st.data_editor(
            df_to_edit,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "Selecionar": st.column_config.CheckboxColumn(help="Marque para importar"),
                "Hospital": st.column_config.TextColumn(disabled=True),
                "Ano": st.column_config.NumberColumn(disabled=True),
                "Mes": st.column_config.NumberColumn(disabled=True),
                "Dia": st.column_config.NumberColumn(disabled=True),
                "Data": st.column_config.TextColumn(disabled=True),
                "Atendimento": st.column_config.TextColumn(disabled=True),
                "Aviso": st.column_config.TextColumn(disabled=True),
                "Convenio": st.column_config.TextColumn(disabled=True),
                "Prestador": st.column_config.TextColumn(disabled=True),
                "Quarto": st.column_config.TextColumn(disabled=True),
                "Paciente": st.column_config.TextColumn(help="Clique para editar o nome do paciente."),
            },
            hide_index=True,
            key=st.session_state.editor_key
        )
        edited_df = pd.DataFrame(edited_df)

        df_selecionados = edited_df[edited_df["Selecionar"] == True].copy()

        st.markdown("#### Persistência (apenas selecionados)")
        if st.button("💾 Salvar apenas selecionados no banco"):
            try:
                if df_selecionados.empty:
                    st.warning("Nenhum paciente selecionado para importação.")
                else:
                    ensure_db_writable()
                    salvos, ignoradas = upsert_dataframe(df_selecionados.drop(columns=["Selecionar"], errors="ignore"))
                    st.cache_data.clear()
                    total = count_all()
                    st.success(f"Importação concluída: {salvos} salvos, {ignoradas} ignorados (chave incompleta). Total no banco: {total}")

                    try_vacuum_safely()

                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        try:
                            ok, status, msg = safe_upload_with_merge(
                                owner=GH_OWNER,
                                repo=GH_REPO,
                                path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH,
                                local_db_path=DB_PATH,
                                commit_message=f"Atualiza banco SQLite via app (salvar pacientes selecionados: {salvos})",
                                prev_sha=st.session_state.get("gh_sha"),
                                _return_details=True
                            )
                            if ok:
                                new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                                if new_sha:
                                    st.session_state["gh_sha"] = new_sha
                                st.success("Sincronização automática com GitHub concluída.")
                            else:
                                st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
                        except Exception as e:
                            st.error("Falha ao sincronizar com GitHub.")
                            st.exception(e)

                    st.session_state.editor_key = "editor_pacientes_after_save"

            except PermissionError as pe:
                st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
            except Exception as e:
                st.error("Falha ao salvar no banco.")
                st.exception(e)

        st.markdown("#### Exportar Excel (multi-aba por Hospital)")
        df_for_export = pd.DataFrame(edited_df.drop(columns=["Selecionar"], errors="ignore"))
        excel_bytes = to_formatted_excel_by_hospital(df_for_export)
        st.download_button(
            label="Baixar Excel por Hospital (arquivo atual)",
            data=excel_bytes,
            file_name="Pacientes_por_dia_prestador_hospital.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # ==========================================================
    # ➕ Cadastro manual de paciente (mesmos campos da importação)
    # ==========================================================
    with st.expander("➕ Cadastro manual de paciente (campos iguais à importação)", expanded=False):
        st.caption("Preencha os dados e clique em **Salvar**. É obrigatório informar: Hospital, Data, Prestador e (Atendimento **ou** Paciente).")

        colM1, colM2, colM3 = st.columns([1, 1, 1])
        with colM1:
            cad_hospital = st.selectbox("Hospital", options=HOSPITAL_OPCOES, index=0, key="cad_hosp_manual")
        with colM2:
            cad_data_date = st.date_input("Data (dia/mês/ano)", value=datetime.today().date(), key="cad_data_manual")
            cad_data_str = cad_data_date.strftime("%d/%m/%Y")  # dd/MM/yyyy
        with colM3:
            cad_prestador = st.text_input("Prestador", value="", key="cad_prest_manual", placeholder="Ex.: JOSE.ADORNO")

        colM4, colM5, colM6 = st.columns([1, 1, 1])
        with colM4:
            cad_atendimento = st.text_input("Atendimento", value="", key="cad_att_manual", placeholder="Código do atendimento (opcional se informar Paciente)")
        with colM5:
            cad_paciente = st.text_input("Paciente", value="", key="cad_pac_manual", placeholder="Nome do paciente (opcional se informar Atendimento)")
        with colM6:
            cad_convenio = st.text_input("Convênio", value="", key="cad_conv_manual", placeholder="Ex.: Particular, Unimed...")

        colM7, colM8 = st.columns([1, 1])
        with colM7:
            cad_aviso = st.text_input("Aviso", value="", key="cad_aviso_manual", placeholder="Observação breve (opcional)")
        with colM8:
            cad_quarto = st.text_input("Quarto", value="", key="cad_quarto_manual", placeholder="Ex.: 301-B (opcional)")

        salvar_cad_manual = st.button("💾 Salvar paciente (manual)")

        if salvar_cad_manual:
            try:
                # Validações mínimas
                errors = []
                if not cad_hospital:
                    errors.append("Hospital é obrigatório.")
                if not cad_data_str:
                    errors.append("Data é obrigatória.")
                if not cad_prestador.strip():
                    errors.append("Prestador é obrigatório.")
                if (not cad_atendimento.strip()) and (not cad_paciente.strip()):
                    errors.append("Informe **Atendimento** ou **Paciente** (pelo menos um).")

                if errors:
                    for e in errors:
                        st.error(e)
                else:
                    ensure_db_writable()
                    base_row = {
                        "Hospital": cad_hospital.strip(),
                        "Data": cad_data_str.strip(),
                        "Atendimento": cad_atendimento.strip(),
                        "Paciente": cad_paciente.strip(),
                        "Prestador": cad_prestador.strip(),
                        "Convenio": cad_convenio.strip(),
                        "Aviso": cad_aviso.strip(),
                        "Quarto": cad_quarto.strip(),
                    }
                    salvos, ignoradas = upsert_paciente_single(base_row)

                    if salvos == 1 and ignoradas == 0:
                        st.success("Paciente cadastrado/atualizado com sucesso na base.")
                    elif ignoradas == 1:
                        st.warning("Cadastro ignorado: chave mínima incompleta (Atendimento e Paciente vazios).")
                    else:
                        st.info(f"Ação concluída. (salvos={salvos}, ignoradas={ignoradas})")

                    # Limpa cache de dados e manutenção
                    st.cache_data.clear()
                    try_vacuum_safely()

                    # Sincroniza com GitHub
                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        try:
                            ok, status, msg = safe_upload_with_merge(
                                owner=GH_OWNER,
                                repo=GH_REPO,
                                path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH,
                                local_db_path=DB_PATH,
                                commit_message="Cadastro manual de paciente (Aba Importação)",
                                prev_sha=st.session_state.get("gh_sha"),
                                _return_details=True
                            )
                            if ok:
                                new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                                if new_sha:
                                    st.session_state["gh_sha"] = new_sha
                                st.success("Sincronização automática com GitHub concluída.")
                            else:
                                st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
                        except Exception as e:
                            st.error("Falha ao sincronizar com GitHub.")
                            st.exception(e)

                    # Recarrega a aba para refletir o novo cadastro
                    st.rerun()

            except PermissionError as pe:
                st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
            except Exception as e:
                st.error("Falha ao salvar cadastro manual.")
                st.exception(e)

    st.divider()
    st.markdown("#### Conteúdo atual do banco (exemplo.db)")
    rows = read_all()
    if rows:
        cols = ["Hospital", "Ano", "Mes", "Dia", "Data", "Atendimento", "Paciente", "Aviso", "Convenio", "Prestador", "Quarto"]
        db_df = pd.DataFrame(rows, columns=cols)
        st.dataframe(
            db_df.sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]),
            use_container_width=True
        )
        st.markdown("##### Exportar Excel (dados do banco)")
        excel_bytes_db = to_formatted_excel_by_hospital(db_df)
        st.download_button(
            label="Baixar Excel (Banco)",
            data=excel_bytes_db,
            file_name="Pacientes_por_dia_prestador_hospital_banco.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Banco ainda sem dados. Faça o upload, selecione e clique em 'Salvar apenas selecionados no banco'.")

# ====================================================================================
# 🩺 Aba 2: Cirurgias — União (ocultando colunas técnicas) + salvar mover/atualizar
# ====================================================================================
with tabs[1]:
    st.subheader("Cadastrar / Editar Cirurgias (compartilha o mesmo banco)")

    st.markdown("#### Filtros para carregar pacientes na Lista de Cirurgias")
    colF0, colF1, colF2, colF3 = st.columns([1, 1, 1, 1])
    with colF0:
        usar_periodo = st.checkbox(
            "Filtrar por Ano/Mês",
            value=True,
            help="Desmarque para carregar todos os pacientes do hospital, independente do período."
        )
    with colF1:
        hosp_cad = st.selectbox("Filtro Hospital (lista)", options=HOSPITAL_OPCOES, index=0)
    now = datetime.now()
    with colF2:
        ano_cad = st.number_input(
            "Ano (filtro base)", min_value=2000, max_value=2100,
            value=now.year, step=1, disabled=not usar_periodo
        )
    with colF3:
        mes_cad = st.number_input(
            "Mês (filtro base)", min_value=1, max_value=12,
            value=now.month, step=1, disabled=not usar_periodo
        )

    prestadores_filtro = st.text_input(
        "Prestadores (filtro base, separar por ; ) — deixe vazio para não filtrar",
        value=""
    )
    prestadores_lista_filtro = [p.strip() for p in prestadores_filtro.split(";") if p.strip()]

    # ---- Recarregar catálogos (Tipos/Situações) ----
    col_refresh, col_refresh_info = st.columns([1.5, 2.5])
    with col_refresh:
        if st.button("🔄 Recarregar catálogos (Tipos/Situações)"):
            st.session_state["catalog_refresh_ts"] = datetime.now().isoformat(timespec="seconds")
            st.success(f"Catálogos recarregados às {st.session_state['catalog_refresh_ts']}")
    with col_refresh_info:
        ts = st.session_state.get("catalog_refresh_ts")
        if ts:
            st.caption(f"Último recarregamento: {ts}")

    # -------- Carregar catálogos (para dropdowns do grid) --------
    tipos_rows = list_procedimento_tipos(only_active=True)
    df_tipos_cat = pd.DataFrame(tipos_rows, columns=["id", "nome", "ativo", "ordem"]) if tipos_rows else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
    if not df_tipos_cat.empty:
        df_tipos_cat = df_tipos_cat.sort_values(["ordem", "nome"], kind="mergesort")
        tipo_nome_list = df_tipos_cat["nome"].tolist()
        tipo_nome2id = dict(zip(df_tipos_cat["nome"], df_tipos_cat["id"]))  # nome -> id
        tipo_id2nome = dict(zip(df_tipos_cat["id"], df_tipos_cat["nome"]))  # id -> nome
    else:
        tipo_nome_list = []
        tipo_nome2id = {}
        tipo_id2nome = {}

    sits_rows = list_cirurgia_situacoes(only_active=True)
    df_sits_cat = pd.DataFrame(sits_rows, columns=["id", "nome", "ativo", "ordem"]) if sits_rows else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
    if not df_sits_cat.empty:
        df_sits_cat = df_sits_cat.sort_values(["ordem", "nome"], kind="mergesort")
        sit_nome_list = df_sits_cat["nome"].tolist()
        sit_nome2id = dict(zip(df_sits_cat["nome"], df_sits_cat["id"]))
        sit_id2nome = dict(zip(df_sits_cat["id"], df_sits_cat["nome"]))
    else:
        sit_nome_list = []
        sit_nome2id = {}
        sit_id2nome = {}

    if not tipo_nome_list:
        st.warning("Nenhum **Tipo de Procedimento** ativo encontrado. Cadastre na aba **📚 Cadastro (Tipos & Situações)** e marque como **Ativo**.")
    if not sit_nome_list:
        st.warning("Nenhuma **Situação da Cirurgia** ativa encontrada. Cadastre na aba **📚 Cadastro (Tipos & Situações)** e marque como **Ativo**.")

    # --- Filtro de Situação (multiselect) ---
    st.markdown("#### Filtros complementares")
    col_sit, col_hint = st.columns([2, 2])
    with col_sit:
        sit_filter_nomes = st.multiselect(
            "Filtrar por Situação (se selecionar, ignora Ano/Mês)",
            options=sit_nome_list,
            default=[],
            help="Selecione uma ou mais Situações. Se usar este filtro, o filtro de Ano/Mês será ignorado."
        )
    sit_filter_ids = [sit_nome2id[n] for n in sit_filter_nomes if n in sit_nome2id]

    # Se o filtro de Situação estiver ativo, devemos ignorar Ano/Mês
    ignorar_periodo_por_situacao = len(sit_filter_ids) > 0
    if ignorar_periodo_por_situacao:
        st.info("Filtro por Situação **ativo** — o filtro de Ano/Mês foi ignorado.")

    # -------- Montar a Lista de Cirurgias com união (Cirurgias + Base) --------
    try:
        # ✅ Ignorar Ano/Mês quando houver filtro por Situação
        if ignorar_periodo_por_situacao:
            ano_mes_str = None
            ano_base = None
            mes_base = None
        else:
            ano_mes_str = f"{int(ano_cad)}-{int(mes_cad):02d}" if usar_periodo else None
            ano_base = int(ano_cad) if usar_periodo else None
            mes_base = int(mes_cad) if usar_periodo else None

        rows_cir = list_cirurgias(hospital=hosp_cad, ano_mes=ano_mes_str, prestador=None)
        df_cir = pd.DataFrame(rows_cir, columns=[
            "id", "Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
            "Convenio", "Procedimento_Tipo_ID", "Situacao_ID",
            "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura",
            "Observacoes", "created_at", "updated_at"
        ])
        if df_cir.empty:
            df_cir = pd.DataFrame(columns=[
                "id", "Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
                "Convenio", "Procedimento_Tipo_ID", "Situacao_ID",
                "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura",
                "Observacoes", "created_at", "updated_at"
            ])

        # 🔎 Filtro de prestadores sobre cirurgias (quando informado)
        if prestadores_lista_filtro:
            prest_set = {p.strip().lower() for p in prestadores_lista_filtro if p.strip()}
            df_cir = df_cir[df_cir["Prestador"].astype(str).str.lower().isin(prest_set)]

        # 🔎 Filtro de situação sobre cirurgias (quando informado)
        if ignorar_periodo_por_situacao and sit_filter_ids:
            df_cir = df_cir[df_cir["Situacao_ID"].isin(sit_filter_ids)]

        # Labels legíveis
        df_cir["Fonte"] = "Cirurgia"
        df_cir["Tipo (nome)"] = df_cir["Procedimento_Tipo_ID"].map(tipo_id2nome).fillna("")
        df_cir["Situação (nome)"] = df_cir["Situacao_ID"].map(sit_id2nome).fillna("")

        # metadados para mover/atualizar
        df_cir["_old_source"] = "Cirurgia"
        df_cir["_old_h"] = df_cir["Hospital"].astype(str)
        df_cir["_old_att"] = df_cir["Atendimento"].astype(str)
        df_cir["_old_pac"] = df_cir["Paciente"].astype(str)
        df_cir["_old_pre"] = df_cir["Prestador"].astype(str)
        df_cir["_old_data"] = df_cir["Data_Cirurgia"].astype(str)

        # ✅ Base de candidatos (ignora período se filtro de Situação estiver ativo)
        base_rows = find_registros_para_prefill(
            hosp_cad,
            ano=ano_base,
            mes=mes_base,
            prestadores=prestadores_lista_filtro
        )
        df_base = pd.DataFrame(base_rows, columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
        if df_base.empty:
            df_base = pd.DataFrame(columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
        else:
            for col in ["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"]:
                df_base[col] = df_base[col].fillna("").astype(str)

        # Mapeia candidatos da base para o esquema de cirurgias
        df_base_mapped = pd.DataFrame({
            "id": [None]*len(df_base),
            "Hospital": df_base["Hospital"],
            "Atendimento": df_base["Atendimento"],
            "Paciente": df_base["Paciente"],
            "Prestador": df_base["Prestador"],
            "Data_Cirurgia": df_base["Data"],
            "Convenio": df_base["Convenio"],
            "Procedimento_Tipo_ID": [None]*len(df_base),
            "Situacao_ID": [None]*len(df_base),
            "Guia_AMHPTISS": ["" for _ in range(len(df_base))],
            "Guia_AMHPTISS_Complemento": ["" for _ in range(len(df_base))],
            "Fatura": ["" for _ in range(len(df_base))],
            "Observacoes": ["" for _ in range(len(df_base))],
            "created_at": [None]*len(df_base),
            "updated_at": [None]*len(df_base),
            "Fonte": ["Base"]*len(df_base),
            "Tipo (nome)": ["" for _ in range(len(df_base))],
            "Situação (nome)": ["" for _ in range(len(df_base))],

            # metadados chave original
            "_old_source": ["Base"]*len(df_base),
            "_old_h": df_base["Hospital"].astype(str),
            "_old_att": df_base["Atendimento"].astype(str),
            "_old_pac": df_base["Paciente"].astype(str),
            "_old_pre": df_base["Prestador"].astype(str),
            "_old_data": df_base["Data"].astype(str),
        })

        st.info(f"Cirurgias já salvas: {len(df_cir)} | Candidatos da base: {len(df_base)}")

        if df_base.empty and not ignorar_periodo_por_situacao:
            st.warning("Nenhum candidato carregado da base com os filtros atuais.")
            with st.expander("Diagnóstico do filtro", expanded=False):
                st.markdown("- Verifique o **Hospital** (coincide com Aba 1?).")
                st.markdown("- Ajuste **Ano/Mês** ou desmarque **Filtrar por Ano/Mês**.")
                st.markdown("- Deixe **Prestadores** vazio para não filtrar.")
                st.markdown("- O filtro aceita datas em `dd/MM/yyyy` e `YYYY-MM-DD`.")

        # União preferindo registros já existentes (evita duplicar mesma chave)
        df_union = pd.concat([df_cir, df_base_mapped], ignore_index=True)
        df_union["_has_id"] = df_union["id"].notna().astype(int)

        # Chave de dedup (usa Atendimento quando presente, senão Paciente)
        df_union["_AttOrPac"] = df_union["Atendimento"].fillna("").astype(str).str.strip()
        empty_mask = df_union["_AttOrPac"] == ""
        df_union.loc[empty_mask, "_AttOrPac"] = df_union.loc[empty_mask, "Paciente"].fillna("").astype(str).str.strip()

        KEY_COLS = ["Hospital", "_AttOrPac", "Prestador", "Data_Cirurgia"]
        df_union = df_union.sort_values(KEY_COLS + ["_has_id"], ascending=[True, True, True, True, False])
        df_union = df_union.drop_duplicates(subset=KEY_COLS, keep="first")
        df_union.drop(columns=["_has_id"], inplace=True)

        st.markdown("#### Lista de Cirurgias (com pacientes carregados da base)")
        st.caption("Edite diretamente no grid. Selecione **Tipo (nome)** e **Situação (nome)**; ao salvar, IDs são resolvidos automaticamente.")

        # ---------- Preparar visão editável ocultando colunas técnicas ----------
        df_edit_view = df_union.copy()
        cols_to_hide = [
            "_old_source", "_old_h", "_old_att", "_old_pac", "_old_pre", "_old_data",
            "_AttOrPac", "_attorpac",
            "created_at", "updated_at",
            "Fonte", "fonte",
            "Procedimento_Tipo_ID", "Situacao_ID",
            "id"  # remova se quiser manter o id visível
        ]
        df_edit_view = df_edit_view.drop(columns=[c for c in cols_to_hide if c in df_edit_view.columns], errors="ignore")

        edited_df = st.data_editor(
            df_edit_view,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "Hospital": st.column_config.TextColumn(),
                "Atendimento": st.column_config.TextColumn(),
                "Paciente": st.column_config.TextColumn(),
                "Prestador": st.column_config.TextColumn(),
                "Data_Cirurgia": st.column_config.TextColumn(help="Formato livre, ex.: dd/MM/yyyy ou YYYY-MM-DD."),
                "Convenio": st.column_config.TextColumn(),
                "Tipo (nome)": st.column_config.SelectboxColumn(options=[""] + tipo_nome_list),
                "Situação (nome)": st.column_config.SelectboxColumn(options=[""] + sit_nome_list),
                "Guia_AMHPTISS": st.column_config.TextColumn(),
                "Guia_AMHPTISS_Complemento": st.column_config.TextColumn(),
                "Fatura": st.column_config.TextColumn(),
                "Observacoes": st.column_config.TextColumn(),
            },
            key="editor_lista_cirurgias_union"
        )
        edited_df = pd.DataFrame(edited_df)

        # --- Salvar com atualização/movimentação correta ---
        if st.button("💾 Salvar alterações da Lista (atualizar/mover)"):
            try:
                ensure_db_writable()

                # ✅ Reintroduz metadados _old_* no edited_df antes do loop de salvar
                meta_cols = ["_old_source", "_old_h", "_old_att", "_old_pac", "_old_pre", "_old_data"]
                merge_keys = ["Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia"]
                df_meta = df_union[meta_cols + merge_keys].copy()
                edited_df = edited_df.merge(df_meta, on=merge_keys, how="left")

                num_updated, num_moved, num_skip = 0, 0, 0

                # Mapear nomes -> IDs
                def _nome_to_id(nome: str, mapa: dict):
                    nome = (nome or "").strip()
                    return mapa.get(nome) if nome else None

                for _, r in edited_df.iterrows():
                    tipo_id = _nome_to_id(r.get("Tipo (nome)"), tipo_nome2id)
                    sit_id  = _nome_to_id(r.get("Situação (nome)"), sit_nome2id)

                    # Nova chave (editada pelo usuário)
                    h   = str(r.get("Hospital", "")).strip()
                    att = str(r.get("Atendimento", "")).strip()
                    pac = str(r.get("Paciente", "")).strip()
                    p   = str(r.get("Prestador", "")).strip()
                    d   = str(r.get("Data_Cirurgia", "")).strip()
                    if not h or not p or not d or (not att and not pac):
                        num_skip += 1
                        continue

                    # Chave original (como veio da união)
                    src  = str(r.get("_old_source", "")).strip()
                    oh   = str(r.get("_old_h", "")).strip()
                    oatt = str(r.get("_old_att", "")).strip()
                    opac = str(r.get("_old_pac", "")).strip()
                    op   = str(r.get("_old_pre", "")).strip()
                    od   = str(r.get("_old_data", "")).strip()

                    key_changed = (h != oh) or (p != op) or (d != od) or (att != oatt) or (pac != opac)

                    payload = {
                        "Hospital": h,
                        "Atendimento": att,
                        "Paciente": pac,
                        "Prestador": p,
                        "Data_Cirurgia": d,
                        "Convenio": str(r.get("Convenio", "")).strip(),
                        "Procedimento_Tipo_ID": tipo_id,
                        "Situacao_ID": sit_id,
                        "Guia_AMHPTISS": str(r.get("Guia_AMHPTISS", "")).strip(),
                        "Guia_AMHPTISS_Complemento": str(r.get("Guia_AMHPTISS_Complemento", "")).strip(),
                        "Fatura": str(r.get("Fatura", "")).strip(),
                        "Observacoes": str(r.get("Observacoes", "")).strip(),
                    }

                    # ---- Cirurgias: mover se chave mudou e origem era 'Cirurgia'
                    if key_changed and src == "Cirurgia":
                        delete_cirurgia_by_key(oh, oatt, opac, op, od)
                        insert_or_update_cirurgia(payload)
                        num_moved += 1
                    else:
                        # mesma chave (ou veio da base) → upsert resolve atualização
                        insert_or_update_cirurgia(payload)
                        num_updated += 1

                    # ---- Base: se origem 'Base' e chave mudou, apaga antigo
                    if key_changed and src == "Base":
                        delete_paciente_by_key(oh, oatt, opac, op, od)

                    # Sempre upsert do novo estado na base (garante espelho)
                    base_row = {
                        "Hospital": h,
                        "Data": d,
                        "Atendimento": att,
                        "Paciente": pac,
                        "Prestador": p,
                        "Convenio": str(r.get("Convenio", "")).strip(),
                        "Aviso": "",
                        "Quarto": "",
                    }
                    upsert_paciente_single(base_row)

                st.success(f"UPSERT concluído. Atualizados={num_updated}; Movidos={num_moved}; Ignorados={num_skip}")
                st.cache_data.clear()

                try_vacuum_safely()

                if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                    try:
                        ok, status, msg = safe_upload_with_merge(
                            owner=GH_OWNER,
                            repo=GH_REPO,
                            path_in_repo=GH_PATH_IN_REPO,
                            branch=GH_BRANCH,
                            local_db_path=DB_PATH,
                            commit_message=f"Atualiza banco SQLite via app (cirurgias: upd={num_updated}, move={num_moved})",
                            prev_sha=st.session_state.get("gh_sha"),
                            _return_details=True
                        )
                        if ok:
                            new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                            if new_sha:
                                st.session_state["gh_sha"] = new_sha
                            st.success("Sincronização automática com GitHub concluída.")
                        else:
                            st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
                    except Exception as e:
                        st.error("Falha ao sincronizar com GitHub.")
                        st.exception(e)

                # ✅ Recalcula a aba com filtros aplicados imediatamente
                st.rerun()

            except PermissionError as pe:
                st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
            except Exception as e:
                st.error("Falha ao salvar alterações da lista.")
                st.exception(e)

        # Exportar Excel (lista atual) — também sem colunas ocultas
        colG1, colG2 = st.columns([1, 1])
        with colG2:
            try:
                export_hide_cols = cols_to_hide
                export_df = edited_df.drop(columns=[c for c in export_hide_cols if c in edited_df.columns], errors="ignore")
                excel_bytes = to_formatted_excel_cirurgias(export_df)
                st.download_button(
                    label="⬇️ Baixar Cirurgias.xlsx",
                    data=excel_bytes,
                    file_name="Cirurgias.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception as e:
                st.error("Falha ao exportar Excel.")
                st.exception(e)

        # Exclusões rápidas
        with st.expander("🗑️ Excluir por ID / por chave / por filtros", expanded=False):
            colD1, colD2 = st.columns(2)
            with colD1:
                del_id = st.number_input("Excluir cirurgia por id", min_value=0, step=1, value=0)
                if st.button("🗑️ Excluir cirurgia (id)"):
                    try:
                        ensure_db_writable()
                        delete_cirurgia(int(del_id))
                        st.cache_data.clear()
                        st.success(f"Cirurgia id={int(del_id)} excluída.")
                        try_vacuum_safely()
                        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                            ok, status, msg = safe_upload_with_merge(
                                owner=GH_OWNER, repo=GH_REPO, path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH, local_db_path=DB_PATH,
                                commit_message="Atualiza banco SQLite via app (excluir cirurgia id)",
                                prev_sha=st.session_state.get("gh_sha"),
                                _return_details=True
                            )
                            if ok:
                                new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                                if new_sha: st.session_state["gh_sha"] = new_sha
                                st.success("Sincronização com GitHub concluída.")
                            else:
                                st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
                    except Exception as e:
                        st.error("Falha ao excluir.")
                        st.exception(e)

            with colD2:
                with st.form("form_excluir_chave"):
                    key_h  = st.selectbox("Hospital", options=HOSPITAL_OPCOES, index=0, key="key_hosp")
                    key_att = st.text_input("Atendimento", value="", key="key_att")
                    key_pac = st.text_input("Paciente", value="", key="key_pac")
                    key_pre = st.text_input("Prestador", value="", key="key_pre")
                    key_dt  = st.text_input("Data da Cirurgia (ex.: 10/10/2025)", value="", key="key_dt")
                    submitted = st.form_submit_button("Apagar por chave (1 registro)")
                if submitted:
                    try:
                        ensure_db_writable()
                        removed = delete_cirurgia_by_key(key_h, key_att, key_pac, key_pre, key_dt)
                        st.cache_data.clear()
                        try_vacuum_safely()
                        if removed:
                            st.success("Registro apagado com sucesso.")
                        else:
                            st.info("Nenhum registro encontrado para a chave informada.")
                        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                            ok, status, msg = safe_upload_with_merge(
                                owner=GH_OWNER, repo=GH_REPO, path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH, local_db_path=DB_PATH,
                                commit_message="Exclusão por chave composta (1 registro)",
                                prev_sha=st.session_state.get("gh_sha"),
                                _return_details=True
                            )
                            if ok:
                                new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                                if new_sha: st.session_state["gh_sha"] = new_sha
                                st.success("Sincronização com GitHub concluída.")
                            else:
                                st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
                        st.rerun()
                    except Exception as e:
                        st.error("Falha na exclusão por chave.")
                        st.exception(e)

        with st.expander("🗑️ Exclusão em lote (por filtros)", expanded=False):
            st.caption("Hospital é obrigatório. Demais filtros opcionais; se todos vazios, nada será apagado.")
            hosp_del = st.selectbox("Hospital", options=HOSPITAL_OPCOES, index=0, key="del_hosp")
            atts_raw = st.text_area("Atendimentos (um por linha)", value="", height=120, key="del_atts")
            prests_raw = st.text_area("Prestadores (um por linha)", value="", height=120, key="del_prests")
            datas_raw = st.text_area("Datas de Cirurgia (um por linha, ex.: 10/10/2025)", value="", height=120, key="del_datas")

            def _to_list(raw: str):
                return [ln.strip() for ln in raw.splitlines() if ln.strip()]

            if st.button("Apagar por filtros (lote)"):
                try:
                    ensure_db_writable()
                    total_apagadas = delete_cirurgias_by_filter(
                        hospital=hosp_del,
                        atendimentos=_to_list(atts_raw),
                        prestadores=_to_list(prests_raw),
                        datas=_to_list(datas_raw)
                    )
                    st.cache_data.clear()
                    try_vacuum_safely()
                    st.success(f"{total_apagadas} cirurgia(s) apagada(s) com sucesso.")

                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        ok, status, msg = safe_upload_with_merge(
                            owner=GH_OWNER, repo=GH_REPO, path_in_repo=GH_PATH_IN_REPO,
                            branch=GH_BRANCH, local_db_path=DB_PATH,
                            commit_message=f"Exclusão em lote de cirurgias ({total_apagadas} apagadas)",
                            prev_sha=st.session_state.get("gh_sha"),
                            _return_details=True
                        )
                        if ok:
                            new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                            if new_sha: st.session_state["gh_sha"] = new_sha
                            st.success("Sincronização automática com GitHub concluída.")
                        else:
                            st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")

                    st.rerun()
                except Exception as e:
                    st.error("Falha na exclusão em lote.")
                    st.exception(e)

        with st.expander("🔎 Diagnóstico rápido (ver primeiros registros da base)", expanded=False):
            if st.button("Ver todos (limite 500)"):
                try:
                    rows_all = list_registros_base_all(500)
                    df_all = pd.DataFrame(rows_all, columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
                    st.dataframe(df_all, use_container_width=True, height=300)
                except Exception as e:
                    st.error("Erro ao listar registros base.")
                    st.exception(e)

    except Exception as e:
        st.error("Erro ao montar a lista de cirurgias.")
        st.exception(e)

# ====================================================================================
# 📚 Aba 3: Cadastro (Tipos & Situações)
# ====================================================================================
with tabs[2]:
    st.subheader("Catálogos de Tipos de Procedimento e Situações da Cirurgia")

    st.markdown("#### Tipos de Procedimento")
    colA, colB = st.columns([2, 1])

    if "tipo_form_reset" not in st.session_state:
        st.session_state["tipo_form_reset"] = 0
    if "tipo_bulk_reset" not in st.session_state:
        st.session_state["tipo_bulk_reset"] = 0

    df_tipos_cached = st.session_state.get("df_tipos_cached")
    if df_tipos_cached is None:
        tipos_all = list_procedimento_tipos(only_active=False)
        if tipos_all:
            df_tipos_cached = pd.DataFrame(tipos_all, columns=["id", "nome", "ativo", "ordem"])
        else:
            df_tipos_cached = pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
        st.session_state["df_tipos_cached"] = df_tipos_cached

    def _next_ordem_from_cache(df: pd.DataFrame) -> int:
        if df.empty or "ordem" not in df.columns:
            return 1
        try:
            return int(pd.to_numeric(df["ordem"], errors="coerce").max() or 0) + 1
        except Exception:
            return 1

    next_tipo_ordem = _next_ordem_from_cache(df_tipos_cached)

    def _upload_db_catalogo(commit_msg: str):
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            try:
                try_vacuum_safely()
                ok, status, msg = safe_upload_with_merge(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_msg,
                    prev_sha=st.session_state.get("gh_sha"),
                    _return_details=True
                )
                if ok:
                    new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                    if new_sha:
                        st.session_state["gh_sha"] = new_sha
                    st.success("Sincronização automática com GitHub concluída.")
                else:
                    st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
            except Exception as e:
                st.error("Falha ao sincronizar com GitHub.")
                st.exception(e)

    def _save_tipo_and_reset():
        try:
            suffix = st.session_state["tipo_form_reset"]
            tipo_nome = (st.session_state.get(f"tipo_nome_input_{suffix}") or "").strip()
            if not tipo_nome:
                st.warning("Informe um nome de Tipo antes de salvar.")
                return
            tipo_ordem = int(st.session_state.get(f"tipo_ordem_input_{suffix}", next_tipo_ordem))
            tipo_ativo = bool(st.session_state.get(f"tipo_ativo_input_{suffix}", True))

            ensure_db_writable()
            tid = upsert_procedimento_tipo(tipo_nome, int(tipo_ativo), int(tipo_ordem))
            st.cache_data.clear()
            st.success(f"Tipo salvo (id={tid}).")

            tipos_all2 = list_procedimento_tipos(only_active=False)
            df2 = pd.DataFrame(tipos_all2, columns=["id", "nome", "ativo", "ordem"]) if tipos_all2 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_tipos_cached"] = df2

            prox_id = (df2["id"].max() + 1) if not df2.empty else 1
            st.info(f"Próximo ID previsto: {prox_id}")

            _upload_db_catalogo("Atualiza catálogo de Tipos (salvar individual)")
        except PermissionError as pe:
            st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
        except Exception as e:
            msg = str(e).lower()
            if "readonly" in msg or "read-only" in msg:
                st.error("Banco está em modo somente leitura. Mova o .db para diretório gravável (DB_DIR) ou ajuste permissões.")
            else:
                st.error("Falha ao salvar tipo.")
                st.exception(e)
        finally:
            st.session_state["tipo_form_reset"] += 1

    with colA:
        suffix = st.session_state["tipo_form_reset"]
        st.text_input("Novo tipo / atualizar por nome", placeholder="Ex.: Colecistectomia", key=f"tipo_nome_input_{suffix}")
        st.number_input("Ordem (para ordenar listagem)", min_value=0, value=next_tipo_ordem, step=1, key=f"tipo_ordem_input_{suffix}")
        st.checkbox("Ativo", value=True, key=f"tipo_ativo_input_{suffix}")
        st.button("Salvar tipo de procedimento", on_click=_save_tipo_and_reset)

        st.markdown("##### Cadastrar vários tipos (em lote)")
        bulk_suffix = st.session_state["tipo_bulk_reset"]
        st.caption("Informe um tipo por linha. Ex.: Consulta\nECG\nRaio-X")
        st.text_area("Tipos (um por linha)", height=120, key=f"tipo_bulk_input_{bulk_suffix}")
        st.number_input("Ordem inicial (auto-incrementa)", min_value=0, value=next_tipo_ordem, step=1, key=f"tipo_bulk_ordem_{bulk_suffix}")
        st.checkbox("Ativo (padrão)", value=True, key=f"tipo_bulk_ativo_{bulk_suffix}")

        def _save_tipos_bulk_and_reset():
            try:
                suffix = st.session_state["tipo_bulk_reset"]
                raw_text = st.session_state.get(f"tipo_bulk_input_{suffix}", "") or ""
                start_ordem = int(st.session_state.get(f"tipo_bulk_ordem_{suffix}", next_tipo_ordem))
                ativo_padrao = bool(st.session_state.get(f"tipo_bulk_ativo_{suffix}", True))

                linhas = [ln.strip() for ln in raw_text.splitlines()]
                nomes = [ln for ln in linhas if ln]
                if not nomes:
                    st.warning("Nada a cadastrar: informe ao menos um nome de tipo.")
                    return

                ensure_db_writable()
                num_new, num_skip = 0, 0
                vistos = set()
                for i, nome in enumerate(nomes):
                    if nome.lower() in vistos:
                        num_skip += 1
                        continue
                    vistos.add(nome.lower())
                    try:
                        upsert_procedimento_tipo(nome, int(ativo_padrao), start_ordem + i)
                        num_new += 1
                    except Exception:
                        num_skip += 1

                st.cache_data.clear()

                tipos_all3 = list_procedimento_tipos(only_active=False)
                df3 = pd.DataFrame(tipos_all3, columns=["id", "nome", "ativo", "ordem"]) if tipos_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                st.session_state["df_tipos_cached"] = df3

                st.success(f"Cadastro em lote concluído. Criados/atualizados: {num_new} | ignorados: {num_skip}")
                prox_id = (df3["id"].max() + 1) if not df3.empty else 1
                st.info(f"Próximo ID previsto: {prox_id}")

                _upload_db_catalogo("Atualiza catálogo de Tipos (cadastro em lote)")
            except PermissionError as pe:
                st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
            except Exception as e:
                st.error("Falha no cadastro em lote de tipos.")
                st.exception(e)
            finally:
                st.session_state["tipo_bulk_reset"] += 1

        st.button("Salvar tipos em lote", on_click=_save_tipos_bulk_and_reset)

    with colB:
        st.markdown("##### Ações rápidas (Tipos)")
        col_btn_tipos, _ = st.columns([1.5, 2.5])
        with col_btn_tipos:
            if st.button("🔄 Recarregar catálogos de Tipos"):
                try:
                    tipos_allX = list_procedimento_tipos(only_active=False)
                    dfX = pd.DataFrame(tipos_allX, columns=["id", "nome", "ativo", "ordem"]) if tipos_allX else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                    st.session_state["df_tipos_cached"] = dfX
                    st.success("Tipos recarregados com sucesso.")
                except Exception as e:
                    st.error("Falha ao recarregar tipos.")
                    st.exception(e)

        try:
            df_tipos = st.session_state.get("df_tipos_cached", pd.DataFrame(columns=["id", "nome", "ativo", "ordem"]))
            if not df_tipos.empty:
                st.data_editor(
                    df_tipos,
                    use_container_width=True,
                    column_config={
                        "id": st.column_config.NumberColumn(disabled=True),
                        "nome": st.column_config.TextColumn(disabled=True),
                        "ordem": st.column_config.NumberColumn(),
                        "ativo": st.column_config.CheckboxColumn(),
                    },
                    key="editor_tipos_proc"
                )
                if st.button("Aplicar alterações nos tipos"):
                    try:
                        ensure_db_writable()
                        for _, r in df_tipos.iterrows():
                            set_procedimento_tipo_status(int(r["id"]), int(r["ativo"]))
                        st.cache_data.clear()

                        st.success("Tipos atualizados.")

                        tipos_all3 = list_procedimento_tipos(only_active=False)
                        df3 = pd.DataFrame(tipos_all3, columns=["id", "nome", "ativo", "ordem"]) if tipos_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_tipos_cached"] = df3

                        prox_id = (df3["id"].max() + 1) if not df3.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id}")

                        _upload_db_catalogo("Atualiza catálogo de Tipos (aplicar alterações)")
                    except PermissionError as pe:
                        st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
                    except Exception as e:
                        st.error("Falha ao aplicar alterações nos tipos.")
                        st.exception(e)
            else:
                st.info("Nenhum tipo cadastrado ainda.")
        except Exception as e:
            st.error("Erro ao listar/editar tipos.")
            st.exception(e)

    # --------- Situações da Cirurgia -----------
    st.markdown("#### Situações da Cirurgia")
    colC, colD = st.columns([2, 1])

    if "sit_form_reset" not in st.session_state:
        st.session_state["sit_form_reset"] = 0

    df_sits_cached = st.session_state.get("df_sits_cached")
    if df_sits_cached is None:
        sits_all = list_cirurgia_situacoes(only_active=False)
        if sits_all:
            df_sits_cached = pd.DataFrame(sits_all, columns=["id", "nome", "ativo", "ordem"])
        else:
            df_sits_cached = pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
        st.session_state["df_sits_cached"] = df_sits_cached

    def _next_sit_ordem_from_cache(df: pd.DataFrame) -> int:
        if df.empty or "ordem" not in df.columns:
            return 1
        try:
            return int(pd.to_numeric(df["ordem"], errors="coerce").max() or 0) + 1
        except Exception:
            return 1

    next_sit_ordem = _next_sit_ordem_from_cache(df_sits_cached)

    def _upload_db_situacao(commit_msg: str):
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            try_vacuum_safely()
            try:
                ok, status, msg = safe_upload_with_merge(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_msg,
                    prev_sha=st.session_state.get("gh_sha"),
                    _return_details=True
                )
                if ok:
                    new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                    if new_sha:
                        st.session_state["gh_sha"] = new_sha
                    st.success("Sincronização automática com GitHub concluída.")
                else:
                    st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
            except Exception as e:
                st.error("Falha ao sincronizar com GitHub.")
                st.exception(e)

    def _save_sit_and_reset():
        try:
            suffix = st.session_state["sit_form_reset"]
            sit_nome = (st.session_state.get(f"sit_nome_input_{suffix}") or "").strip()
            if not sit_nome:
                st.warning("Informe um nome de Situação antes de salvar.")
                return
            sit_ordem = int(st.session_state.get(f"sit_ordem_input_{suffix}", next_sit_ordem))
            sit_ativo = bool(st.session_state.get(f"sit_ativo_input_{suffix}", True))

            ensure_db_writable()
            sid = upsert_cirurgia_situacao(sit_nome, int(sit_ativo), int(sit_ordem))
            st.cache_data.clear()
            st.success(f"Situação salva (id={sid}).")

            sits_all2 = list_cirurgia_situacoes(only_active=False)
            df2 = pd.DataFrame(sits_all2, columns=["id", "nome", "ativo", "ordem"]) if sits_all2 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_sits_cached"] = df2

            prox_id_s = (df2["id"].max() + 1) if not df2.empty else 1
            st.info(f"Próximo ID previsto: {prox_id_s}")

            _upload_db_situacao("Atualiza catálogo de Situações (salvar individual)")
        except PermissionError as pe:
            st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
        except Exception as e:
            msg = str(e).lower()
            if "readonly" in msg or "read-only" in msg:
                st.error("Banco está em modo somente leitura. Mova o .db para diretório gravável (DB_DIR) ou ajuste permissões.")
            else:
                st.error("Falha ao salvar situação.")
                st.exception(e)
        finally:
            st.session_state["sit_form_reset"] += 1

    with colC:
        suffix = st.session_state["sit_form_reset"]
        st.text_input("Nova situação / atualizar por nome", placeholder="Ex.: Realizada, Cancelada, Adiada", key=f"sit_nome_input_{suffix}")
        st.number_input("Ordem (para ordenar listagem)", min_value=0, value=next_sit_ordem, step=1, key=f"sit_ordem_input_{suffix}")
        st.checkbox("Ativo", value=True, key=f"sit_ativo_input_{suffix}")
        st.button("Salvar situação", on_click=_save_sit_and_reset)

    with colD:
        st.markdown("##### Ações rápidas (Situações)")
        col_btn_sits, _ = st.columns([1.5, 2.5])
        with col_btn_sits:
            if st.button("🔄 Recarregar catálogos de Situações"):
                try:
                    sits_allX = list_cirurgia_situacoes(only_active=False)
                    dfX = pd.DataFrame(sits_allX, columns=["id", "nome", "ativo", "ordem"]) if sits_allX else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                    st.session_state["df_sits_cached"] = dfX
                    st.success("Situações recarregadas com sucesso.")
                except Exception as e:
                    st.error("Falha ao recarregar situações.")
                    st.exception(e)

        try:
            df_sits = st.session_state.get("df_sits_cached", pd.DataFrame(columns=["id", "nome", "ativo", "ordem"]))
            if not df_sits.empty:
                st.data_editor(
                    df_sits,
                    use_container_width=True,
                    column_config={
                        "id": st.column_config.NumberColumn(disabled=True),
                        "nome": st.column_config.TextColumn(disabled=True),
                        "ordem": st.column_config.NumberColumn(),
                        "ativo": st.column_config.CheckboxColumn(),
                    },
                    key="editor_situacoes"
                )
                if st.button("Aplicar alterações nas situações"):
                    try:
                        ensure_db_writable()
                        for _, r in df_sits.iterrows():
                            set_cirurgia_situacao_status(int(r["id"]), int(r["ativo"]))
                        st.cache_data.clear()

                        st.success("Situações atualizadas.")

                        sits_all3 = list_cirurgia_situacoes(only_active=False)
                        df3 = pd.DataFrame(sits_all3, columns=["id", "nome", "ativo", "ordem"]) if sits_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_sits_cached"] = df3

                        prox_id_s = (df3["id"].max() + 1) if not df3.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id_s}")

                        _upload_db_situacao("Atualiza catálogo de Situações (aplicar alterações)")
                    except PermissionError as pe:
                        st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
                    except Exception as e:
                        st.error("Falha ao aplicar alterações nas situações.")
                        st.exception(e)
            else:
                st.info("Nenhuma situação cadastrada ainda.")
        except Exception as e:
            st.error("Erro ao listar/editar situações.")
            st.exception(e)

# ====================================================================================
# 📄 Aba 4: Tipos (Lista)
# ====================================================================================
with tabs[3]:
    st.subheader("Lista de Tipos de Procedimento")
    st.caption("Visualize, filtre, busque, ordene e exporte todos os tipos (ativos e inativos).")

    try:
        tipos_all = list_procedimento_tipos(only_active=False)
        df_tipos_full = pd.DataFrame(tipos_all, columns=["id", "nome", "ativo", "ordem"])
    except Exception as e:
        st.error("Erro ao carregar tipos do banco.")
        st.exception(e)
        df_tipos_full = pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])

    colF1, colF2, colF3, colF4 = st.columns([1, 1, 1, 2])
    with colF1:
        filtro_status = st.selectbox("Status", options=["Todos", "Ativos", "Inativos"], index=0)
    with colF2:
        ordenar_por = st.selectbox("Ordenar por", options=["id", "nome", "ativo", "ordem"], index=3)
    with colF3:
        ordem_cresc = st.checkbox("Ordem crescente", value=True)
    with colF4:
        busca_nome = st.text_input("Buscar por nome (contém)", value="", placeholder="Ex.: ECG, Consulta...")

    df_view = df_tipos_full.copy()
    if filtro_status == "Ativos":
        df_view = df_view[df_view["ativo"] == 1]
    elif filtro_status == "Inativos":
        df_view = df_view[df_view["ativo"] == 0]
    if busca_nome.strip():
        termo = busca_nome.strip().lower()
        df_view = df_view[df_view["nome"].astype(str).str.lower().str.contains(termo)]
    df_view = df_view.sort_values(by=[ordenar_por], ascending=ordem_cresc, kind="mergesort")

    st.divider()
    st.markdown("#### Resultado")
    total_rows = len(df_view)
    per_page = st.number_input("Linhas por página", min_value=10, max_value=200, value=25, step=5)
    max_page = max(1, (total_rows + per_page - 1) // per_page)
    page = st.number_input("Página", min_value=1, max_value=max_page, value=1, step=1)
    start, end = (page - 1) * per_page, (page - 1) * per_page + per_page
    df_page = df_view.iloc[start:end].copy()
    st.caption(f"Exibindo {len(df_page)} de {total_rows} registro(s) — página {page}/{max_page}")
    st.dataframe(df_page, use_container_width=True)

    st.markdown("#### Exportar")
    colE1, colE2 = st.columns(2)
    with colE1:
        csv_bytes = df_view.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Baixar CSV (filtros aplicados)",
            data=csv_bytes,
            file_name="tipos_de_procedimento.csv",
            mime="text/csv"
        )
    with colE2:
        try:
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_view.to_excel(writer, sheet_name="Tipos", index=False)
                wb = writer.book
                ws = writer.sheets("Tipos") if hasattr(writer, "sheets") else writer.sheets["Tipos"]
                header_fmt = wb.add_format({"bold": True, "bg_color": "#DCE6F1", "border": 1})
                # Segurança: garante cabeçalho com formato
                for col_num, value in enumerate(df_view.columns):
                    ws.write(0, col_num, value, header_fmt)
                last_row = max(len(df_view), 1)
                ws.autofilter(0, 0, last_row, max(0, len(df_view.columns) - 1))
                for i, col in enumerate(df_view.columns):
                    values = [str(x) for x in df_view[col].tolist()]
                    maxlen = max([len(str(col))] + [len(v) for v in values]) + 2
                    ws.set_column(i, i, max(14, min(maxlen, 60)))
            output.seek(0)
            st.download_button(
                label="⬇️ Baixar Excel (filtros aplicados)",
                data=output.getvalue(),
                file_name="tipos_de_procedimento.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error("Falha ao gerar Excel.")
            st.exception(e)

    with st.expander("ℹ️ Ajuda / Diagnóstico", expanded=False):
        st.markdown("""
        - **Status**: escolha **Ativos** para ver apenas os que aparecem na Aba **Cirurgias**.
        - **Ordenação**: por padrão ordenamos por **ordem** e depois por **nome**.
        - **Busca**: digite parte do nome e pressione Enter.
        - **Paginação**: ajuste conforme necessário.
        - **Exportar**: baixa exatamente o que está filtrado/ordenado.
        """)
