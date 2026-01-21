
# -*- coding: utf-8 -*-
import os
from datetime import datetime
import pandas as pd
import streamlit as st

from db import init_db, upsert_dataframe, read_all, DB_PATH, count_all
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital

# --- GitHub sync (baixar/subir o .db) ---
try:
    from github_sync import download_db_from_github, upload_db_to_github
    GITHUB_SYNC_AVAILABLE = True
except Exception:
    GITHUB_SYNC_AVAILABLE = False

# ---- Config GitHub (usa st.secrets; sem UI) ----
GH_OWNER = st.secrets.get("GH_OWNER", "seu-usuario-ou-org")
GH_REPO = st.secrets.get("GH_REPO", "seu-repo")
GH_BRANCH = st.secrets.get("GH_BRANCH", "main")
GH_PATH_IN_REPO = st.secrets.get("GH_DB_PATH", "data/exemplo.db")  # deve coincidir com DB_PATH em db.py
GITHUB_TOKEN_OK = bool(st.secrets.get("GITHUB_TOKEN", ""))

st.set_page_config(page_title="Gestão de Pacientes e Cirurgias", layout="wide")

# --- Header ---
st.title("Gestão de Pacientes e Cirurgias")
st.caption("Download do banco no GitHub (1x) → Importar/Processar → Revisar/Salvar → Exportar → Cirurgias (com catálogos) → Cadastro/Lista")

# Baixar DB do GitHub apenas 1x por sessão (ou se não existir localmente)
if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
    if ("gh_db_fetched" not in st.session_state) or (not st.session_state["gh_db_fetched"]):
        if not os.path.exists(DB_PATH):
            try:
                downloaded = download_db_from_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH
                )
                if downloaded:
                    st.success("Banco baixado do GitHub (primeira carga na sessão).")
                else:
                    st.info("Banco não encontrado no GitHub (primeiro uso). Será criado localmente ao salvar.")
            except Exception as e:
                st.warning("Não foi possível baixar o banco do GitHub. Verifique token/permissões em st.secrets.")
                st.exception(e)
        st.session_state["gh_db_fetched"] = True

# Botão opcional (sidebar) para re-download manual
with st.sidebar:
    st.markdown("### Sincronização GitHub")
    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
        if st.button("🔽 Baixar banco do GitHub (manual)"):
            try:
                downloaded = download_db_from_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH
                )
                if downloaded:
                    st.success("Banco baixado do GitHub (manual).")
                else:
                    st.info("Arquivo não existe no repositório.")
            except Exception as e:
                st.error("Falha ao baixar do GitHub.")
                st.exception(e)
    else:
        st.info("GitHub sync desativado (sem token).")

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
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            try:
                ok = upload_db_to_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_message
                )
                if ok:
                    st.success("Sincronização automática com GitHub concluída.")
            except Exception as e:
                st.error("Falha ao sincronizar com GitHub.")
                st.exception(e)

    can_execute = confirmar and (confirma_texto.strip().upper() == "RESET")

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("Apagar **PACIENTES** (tabela base)", type="secondary", disabled=not can_execute):
            try:
                from db import delete_all_pacientes, vacuum
                apagados = delete_all_pacientes()
                vacuum()
                st.success(f"✅ {apagados} paciente(s) apagado(s) do banco.")
                _sync_after_reset(f"Reset: apaga {apagados} pacientes")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar pacientes.")
                st.exception(e)

    with col_r2:
        if st.button("Apagar **CIRURGIAS**", type="secondary", disabled=not can_execute):
            try:
                from db import delete_all_cirurgias, vacuum
                apagadas = delete_all_cirurgias()  # retorna quantas foram removidas
                vacuum()
                st.session_state.pop("editor_lista_cirurgias_union", None)  # limpa cache do grid
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
                from db import delete_all_catalogos, vacuum
                apagados = delete_all_catalogos()
                vacuum()
                st.success(f"✅ {apagados} registro(s) apagado(s) dos catálogos.")
                _sync_after_reset(f"Reset: apaga {apagados} catálogos")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar catálogos.")
                st.exception(e)

    with col_r4:
        if st.button("🗑️ **RESET TOTAL** (apaga arquivo .db)", type="primary", disabled=not can_execute):
            try:
                from db import dispose_engine, reset_db_file
                dispose_engine()
                reset_db_file()
                st.success("Banco recriado vazio.")
                _sync_after_reset("Reset total: recria .db vazio")
                st.rerun()
            except Exception as e:
                st.error("Falha no reset total.")
                st.exception(e)

# Inicializa DB
init_db()

# Lista única de hospitais (ajuste conforme necessário)
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
    st.caption("Upload → herança/filtragem/deduplicação → Hospital → editar Paciente → salvar → exportar → commit automático no GitHub")

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

    col_reset1, _ = st.columns(2)
    with col_reset1:
        if st.button("🧹 Limpar tabela / reset"):
            st.session_state.df_final = None
            st.session_state.last_upload_id = None
            st.session_state.editor_key = "editor_pacientes_reset"
            st.success("Tabela limpa. Faça novo upload para reprocessar.")

    if uploaded_file is not None:
        current_upload_id = _make_upload_id(uploaded_file, selected_hospital)
        if st.session_state.last_upload_id != current_upload_id:
            st.session_state.df_final = None
            st.session_state.editor_key = f"editor_pacientes_{current_upload_id}"
            st.session_state.last_upload_id = current_upload_id

        with st.spinner("Processando arquivo..."):
            try:
                df_final = process_uploaded_file(uploaded_file, prestadores_lista, selected_hospital.strip())
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

        st.markdown("#### Revisar e editar nomes de Paciente (opcional)")
        df_to_edit = st.session_state.df_final.sort_values(
            ["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]
        ).reset_index(drop=True)

        edited_df = st.data_editor(
            df_to_edit,
            use_container_width=True,
            num_rows="fixed",
            column_config={
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
        edited_df = pd.DataFrame(edited_df)  # tipo correto após editor
        st.session_state.df_final = edited_df

        st.markdown("#### Persistência")
        if st.button("Salvar no banco (exemplo.db)"):
            try:
                upsert_dataframe(st.session_state.df_final)
                total = count_all()
                st.success(f"Dados salvos com sucesso. Total de linhas no banco: {total}")

                if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                    try:
                        ok = upload_db_to_github(
                            owner=GH_OWNER,
                            repo=GH_REPO,
                            path_in_repo=GH_PATH_IN_REPO,
                            branch=GH_BRANCH,
                            local_db_path=DB_PATH,
                            commit_message="Atualiza banco SQLite via app (salvar pacientes)"
                        )
                        if ok:
                            st.success("Sincronização automática com GitHub concluída.")
                    except Exception as e:
                        st.error("Falha ao sincronizar com GitHub.")
                        st.exception(e)

                st.session_state.df_final = None
                st.session_state.editor_key = "editor_pacientes_after_save"

            except Exception as e:
                st.error("Falha ao salvar no banco.")
                st.exception(e)

        st.markdown("#### Exportar Excel (multi-aba por Hospital)")
        df_for_export = pd.DataFrame(st.session_state.df_final)
        excel_bytes = to_formatted_excel_by_hospital(df_for_export)
        st.download_button(
            label="Baixar Excel por Hospital (arquivo atual)",
            data=excel_bytes,
            file_name="Pacientes_por_dia_prestador_hospital.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

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
        st.info("Banco ainda sem dados. Faça o upload e clique em 'Salvar no banco'.")

# ====================================================================================
# 🩺 Aba 2: Cirurgias
# ====================================================================================
with tabs[1]:
    st.subheader("Cadastrar / Editar Cirurgias (compartilha o mesmo banco)")
    from db import (
        find_registros_para_prefill,
        insert_or_update_cirurgia,
        list_procedimento_tipos,
        list_cirurgia_situacoes,
        list_cirurgias,
        delete_cirurgia,
        list_registros_base_all
    )
    from export import to_formatted_excel_cirurgias

    # Filtros principais
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

    # Filtros adicionais (opcionais)
    st.markdown("##### Filtros adicionais (opcionais)")
    col_extra1, col_extra2, col_extra3, col_extra4 = st.columns([1, 1, 1, 1])
    with col_extra1:
        filtro_convenio = st.text_input("Convênio contém", value="")
    with col_extra2:
        filtro_paciente = st.text_input("Paciente contém", value="")
    with col_extra3:
        filtro_data_ini = st.text_input("Data início (dd/MM/yyyy)", value="")
    with col_extra4:
        filtro_data_fim = st.text_input("Data fim (dd/MM/yyyy)", value="")

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

    # -------- Montar a Lista de Cirurgias com união (Cirurgias + Base) --------
    try:
        # Carrega cirurgias já salvas
        rows_cir = list_cirurgias(hospital=hosp_cad, ano_mes=None, prestador=None)
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

        # Prepara nomes legíveis a partir dos IDs para linhas existentes
        df_cir["Fonte"] = "Cirurgia"
        df_cir["Tipo (nome)"] = df_cir["Procedimento_Tipo_ID"].map(tipo_id2nome).fillna("")
        df_cir["Situação (nome)"] = df_cir["Situacao_ID"].map(sit_id2nome).fillna("")

        # Carrega candidatos da base
        base_rows = find_registros_para_prefill(
            hosp_cad,
            ano=int(ano_cad) if usar_periodo else None,
            mes=int(mes_cad) if usar_periodo else None,
            prestadores=prestadores_lista_filtro
        )
        df_base = pd.DataFrame(base_rows, columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
        if df_base.empty:
            df_base = pd.DataFrame(columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
        else:
            for col in ["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"]:
                df_base[col] = df_base[col].fillna("").astype(str)

        # Aplica filtros adicionais aos candidatos da base
        if filtro_convenio.strip():
            df_base = df_base[df_base["Convenio"].astype(str).str.contains(filtro_convenio.strip(), case=False, na=False)]
        if filtro_paciente.strip():
            df_base = df_base[df_base["Paciente"].astype(str).str.contains(filtro_paciente.strip(), case=False, na=False)]
        # Filtro por intervalo de data (dd/MM/yyyy)
        def _parse_br_date(s: str):
            try:
                return pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
            except Exception:
                return pd.NaT
        if filtro_data_ini.strip() or filtro_data_fim.strip():
            df_base["_Data"] = pd.to_datetime(df_base["Data"], format="%d/%m/%Y", errors="coerce")
            if filtro_data_ini.strip():
                di = _parse_br_date(filtro_data_ini.strip())
                if pd.notna(di):
                    df_base = df_base[df_base["_Data"] >= di]
            if filtro_data_fim.strip():
                df = _parse_br_date(filtro_data_fim.strip())
                if pd.notna(df):
                    df_base = df_base[df_base["_Data"] <= df]
            df_base.drop(columns=["_Data"], inplace=True, errors="ignore")

        st.info(f"Cirurgias já salvas: {len(df_cir)} | Candidatos da base (após filtros): {len(df_base)}")

        if df_base.empty:
            st.warning("Nenhum candidato carregado da base com os filtros atuais.")
            with st.expander("Diagnóstico do filtro", expanded=False):
                st.markdown("- Verifique o **Hospital** (coincide com Aba 1?).")
                st.markdown("- Ajuste **Ano/Mês** ou desmarque **Filtrar por Ano/Mês**.")
                st.markdown("- Deixe **Prestadores** vazio para não filtrar.")
                st.markdown("- Use filtros adicionais com termos exatos (Convênio/Paciente).")
                st.markdown("- O filtro de datas aceita formato `dd/MM/yyyy`.")

        # Mapeia candidatos da base para o esquema de cirurgias (com colunas legíveis)
        df_base_mapped = pd.DataFrame({
            "id": [None]*len(df_base),
            "Hospital": df_base["Hospital"],
            "Atendimento": df_base["Atendimento"],
            "Paciente": df_base["Paciente"],
            "Prestador": df_base["Prestador"],
            "Data_Cirurgia": df_base["Data"],
            "Convenio": df_base["Convenio"],
            "Procedimento_Tipo_ID": [None]*len(df_base),  # será preenchido ao salvar
            "Situacao_ID": [None]*len(df_base),           # idem
            "Guia_AMHPTISS": ["" for _ in range(len(df_base))],
            "Guia_AMHPTISS_Complemento": ["" for _ in range(len(df_base))],
            "Fatura": ["" for _ in range(len(df_base))],
            "Observacoes": ["" for _ in range(len(df_base))],
            "created_at": [None]*len(df_base),
            "updated_at": [None]*len(df_base],
            "Fonte": ["Base"]*len(df_base),
            "Tipo (nome)": ["" for _ in range(len(df_base))],  # edição por nome
            "Situação (nome)": ["" for _ in range(len(df_base))]  # edição por nome
        })

        # --- Seleção manual dos candidatos para importar ---
        st.markdown("#### Selecione os candidatos da base que deseja importar como Cirurgias")
        st.caption("Marque apenas os que devem virar registros de cirurgia. O tipo e situação podem ser preenchidos depois.")

        df_select_view = df_base_mapped.copy()
        if "Selecionar" not in df_select_view.columns:
            df_select_view["Selecionar"] = False

        edited_select_df = st.data_editor(
            df_select_view[
                [
                    "Selecionar",
                    "Hospital", "Atendimento", "Paciente", "Prestador",
                    "Data_Cirurgia", "Convenio"
                ]
            ],
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "Selecionar": st.column_config.CheckboxColumn(),
                "Hospital": st.column_config.TextColumn(disabled=True),
                "Atendimento": st.column_config.TextColumn(),
                "Paciente": st.column_config.TextColumn(),
                "Prestador": st.column_config.TextColumn(disabled=True),
                "Data_Cirurgia": st.column_config.TextColumn(disabled=True),
                "Convenio": st.column_config.TextColumn(),
            },
            key="editor_candidatos_base_select"
        )
        edited_select_df = pd.DataFrame(edited_select_df)

        col_imp1, col_imp2 = st.columns([1, 3])
        with col_imp1:
            if st.button("📥 Importar selecionados (somente marcados)"):
                try:
                    df_sel = edited_select_df[edited_select_df["Selecionar"] == True].copy()
                    if df_sel.empty:
                        st.warning("Nenhum candidato marcado. Selecione ao menos uma linha.")
                    else:
                        num_ok, num_skip = 0, 0
                        for _, r in df_sel.iterrows():
                            h = str(r.get("Hospital", "")).strip()
                            p = str(r.get("Prestador", "")).strip()
                            d = str(r.get("Data_Cirurgia", "")).strip()
                            att = str(r.get("Atendimento", "")).strip()
                            pac = str(r.get("Paciente", "")).strip()

                            if h and p and d and (att or pac):
                                payload = {
                                    "Hospital": h,
                                    "Atendimento": att,
                                    "Paciente": pac,
                                    "Prestador": p,
                                    "Data_Cirurgia": d,
                                    "Convenio": str(r.get("Convenio", "")).strip(),
                                    "Procedimento_Tipo_ID": None,
                                    "Situacao_ID": None,
                                    "Guia_AMHPTISS": "",
                                    "Guia_AMHPTISS_Complemento": "",
                                    "Fatura": "",
                                    "Observacoes": "",
                                }
                                try:
                                    insert_or_update_cirurgia(payload)
                                    num_ok += 1
                                except Exception:
                                    num_skip += 1
                            else:
                                num_skip += 1

                        st.success(f"Importação concluída. {num_ok} linha(s) importadas; {num_skip} ignorada(s).")
                        # Opcional: recarregar a aba para refletir as cirurgias recém-importadas
                        # st.rerun()
                except Exception as e:
                    st.error("Falha ao importar selecionados.")
                    st.exception(e)

        with col_imp2:
            st.info("Dica: depois de importar, use o grid de **Lista de Cirurgias** para preencher Tipo e Situação.")

        st.divider()
        st.markdown("#### Lista de Cirurgias (com pacientes carregados da base)")
        st.caption("Edite diretamente no grid. Selecione **Tipo (nome)** e **Situação (nome)**; ao salvar, o app preenche os IDs correspondentes.")

        # União preferindo registros já existentes (evita duplicar mesma chave)
        df_union = pd.concat([df_cir, df_base_mapped], ignore_index=True)
        df_union["_has_id"] = df_union["id"].notna().astype(int)

        # Chave resiliente: usa Atendimento; se vazio, usa Paciente
        df_union["_AttOrPac"] = df_union["Atendimento"].fillna("").astype(str).str.strip()
        empty_mask = df_union["_AttOrPac"] == ""
        df_union.loc[empty_mask, "_AttOrPac"] = df_union.loc[empty_mask, "Paciente"].fillna("").astype(str).str.strip()

        KEY_COLS = ["Hospital", "_AttOrPac", "Prestador", "Data_Cirurgia"]
        df_union = df_union.sort_values(KEY_COLS + ["_has_id"], ascending=[True, True, True, True, False])
        df_union = df_union.drop_duplicates(subset=KEY_COLS, keep="first")
        df_union.drop(columns=["_has_id", "_AttOrPac"], inplace=True)

        # 👇 Oculta colunas ID/Fonte, numéricas e auditoria na visão do editor
        df_edit_view = df_union.drop(
            columns=["id", "Fonte", "Procedimento_Tipo_ID", "Situacao_ID", "created_at", "updated_at"],
            errors="ignore"
        )

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

                "Tipo (nome)": st.column_config.SelectboxColumn(
                    options=[""] + tipo_nome_list,
                    help="Selecione o tipo de serviço cadastrado (apenas ativos)."
                ),
                "Situação (nome)": st.column_config.SelectboxColumn(
                    options=[""] + sit_nome_list,
                    help="Selecione a situação da cirurgia (apenas ativas)."
                ),

                "Guia_AMHPTISS": st.column_config.TextColumn(),
                "Guia_AMHPTISS_Complemento": st.column_config.TextColumn(),
                "Fatura": st.column_config.TextColumn(),
                "Observacoes": st.column_config.TextColumn(),
            },
            key="editor_lista_cirurgias_union"
        )
        edited_df = pd.DataFrame(edited_df)

        colG1, colG2, colG3 = st.columns([1.2, 1, 1.8])
        with colG1:
            if st.button("💾 Salvar alterações da Lista (UPSERT em massa)"):
                try:
                    edited_df = edited_df.copy()
                    # Reconstroi IDs a partir dos nomes escolhidos
                    edited_df["Procedimento_Tipo_ID"] = edited_df["Tipo (nome)"].map(lambda n: tipo_nome2id.get(n) if n else None)
                    edited_df["Situacao_ID"] = edited_df["Situação (nome)"].map(lambda n: sit_nome2id.get(n) if n else None)

                    num_ok, num_skip = 0, 0
                    for _, r in edited_df.iterrows():
                        h = str(r.get("Hospital", "")).strip()
                        att = str(r.get("Atendimento", "")).strip()
                        pac = str(r.get("Paciente", "")).strip()
                        p = str(r.get("Prestador", "")).strip()
                        d = str(r.get("Data_Cirurgia", "")).strip()
                        if h and p and d and (att or pac):
                            payload = {
                                "Hospital": h,
                                "Atendimento": att,
                                "Paciente": pac,
                                "Prestador": p,
                                "Data_Cirurgia": d,
                                "Convenio": str(r.get("Convenio", "")).strip(),
                                "Procedimento_Tipo_ID": r.get("Procedimento_Tipo_ID"),
                                "Situacao_ID": r.get("Situacao_ID"),
                                "Guia_AMHPTISS": str(r.get("Guia_AMHPTISS", "")).strip(),
                                "Guia_AMHPTISS_Complemento": str(r.get("Guia_AMHPTISS_Complemento", "")).strip(),
                                "Fatura": str(r.get("Fatura", "")).strip(),
                                "Observacoes": str(r.get("Observacoes", "")).strip(),
                            }
                            insert_or_update_cirurgia(payload)
                            num_ok += 1
                        else:
                            num_skip += 1
                    st.success(f"UPSERT concluído. {num_ok} linha(s) salvas; {num_skip} ignorada(s) (chave incompleta).")

                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        try:
                            ok = upload_db_to_github(
                                owner=GH_OWNER,
                                repo=GH_REPO,
                                path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH,
                                local_db_path=DB_PATH,
                                commit_message="Atualiza banco SQLite via app (salvar lista de cirurgias)"
                            )
                            if ok:
                                st.success("Sincronização automática com GitHub concluída.")
                        except Exception as e:
                            st.error("Falha ao sincronizar com GitHub.")
                            st.exception(e)

                    # Opcional: st.rerun()

                except Exception as e:
                    st.error("Falha ao salvar alterações da lista.")
                    st.exception(e)

        with colG2:
            if st.button("⬇️ Exportar Excel (Lista atual)"):
                try:
                    from export import to_formatted_excel_cirurgias
                    export_df = edited_df.drop(columns=["Tipo (nome)", "Situação (nome)"], errors="ignore")
                    export_df = pd.DataFrame(export_df)
                    excel_bytes = to_formatted_excel_cirurgias(export_df)
                    st.download_button(
                        label="Baixar Cirurgias.xlsx",
                        data=excel_bytes,
                        file_name="Cirurgias.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                except Exception as e:
                    st.error("Falha ao exportar Excel.")
                    st.exception(e)

        with colG3:
            del_id = st.number_input("Excluir cirurgia por id", min_value=0, step=1, value=0)
            if st.button("🗑️ Excluir cirurgia"):
                try:
                    delete_cirurgia(int(del_id))
                    st.success(f"Cirurgia id={int(del_id)} excluída.")
                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        try:
                            ok = upload_db_to_github(
                                owner=GH_OWNER,
                                repo=GH_REPO,
                                path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH,
                                local_db_path=DB_PATH,
                                commit_message="Atualiza banco SQLite via app (excluir cirurgia)"
                            )
                            if ok:
                                st.success("Sincronização automática com GitHub concluída.")
                        except Exception as e:
                            st.error("Falha ao sincronizar com GitHub.")
                            st.exception(e)
                except Exception as e:
                    st.error("Falha ao excluir.")
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

    from db import list_procedimento_tipos
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
                ok = upload_db_to_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_msg
                )
                if ok:
                    st.success("Sincronização automática com GitHub concluída.")
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

            from db import upsert_procedimento_tipo, list_procedimento_tipos
            tid = upsert_procedimento_tipo(tipo_nome, int(tipo_ativo), int(tipo_ordem))
            st.success(f"Tipo salvo (id={tid}).")

            tipos_all2 = list_procedimento_tipos(only_active=False)
            df2 = pd.DataFrame(tipos_all2, columns=["id", "nome", "ativo", "ordem"]) if tipos_all2 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_tipos_cached"] = df2

            prox_id = (df2["id"].max() + 1) if not df2.empty else 1
            st.info(f"Próximo ID previsto: {prox_id}")

            _upload_db_catalogo("Atualiza catálogo de Tipos (salvar individual)")
        except Exception as e:
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

                from db import upsert_procedimento_tipo, list_procedimento_tipos
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

                tipos_all3 = list_procedimento_tipos(only_active=False)
                df3 = pd.DataFrame(tipos_all3, columns=["id", "nome", "ativo", "ordem"]) if tipos_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                st.session_state["df_tipos_cached"] = df3

                st.success(f"Cadastro em lote concluído. Criados/atualizados: {num_new} | ignorados: {num_skip}")
                prox_id = (df3["id"].max() + 1) if not df3.empty else 1
                st.info(f"Próximo ID previsto: {prox_id}")

                _upload_db_catalogo("Atualiza catálogo de Tipos (cadastro em lote)")
            except Exception as e:
                st.error("Falha no cadastro em lote de tipos.")
                st.exception(e)
            finally:
                st.session_state["tipo_bulk_reset"] += 1

        st.button("Salvar tipos em lote", on_click=_save_tipos_bulk_and_reset)

    with colB:
        # Botão de recarregar tipos (cache do grid)
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

        from db import set_procedimento_tipo_status
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
                        for _, r in df_tipos.iterrows():
                            set_procedimento_tipo_status(int(r["id"]), int(r["ativo"]))
                        st.success("Tipos atualizados.")

                        tipos_all3 = list_procedimento_tipos(only_active=False)
                        df3 = pd.DataFrame(tipos_all3, columns=["id", "nome", "ativo", "ordem"]) if tipos_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_tipos_cached"] = df3

                        prox_id = (df3["id"].max() + 1) if not df3.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id}")

                        _upload_db_catalogo("Atualiza catálogo de Tipos (aplicar alterações)")
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

    from db import list_cirurgia_situacoes
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
            try:
                ok = upload_db_to_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_msg
                )
                if ok:
                    st.success("Sincronização automática com GitHub concluída.")
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

            from db import upsert_cirurgia_situacao, list_cirurgia_situacoes
            sid = upsert_cirurgia_situacao(sit_nome, int(sit_ativo), int(sit_ordem))
            st.success(f"Situação salva (id={sid}).")

            sits_all2 = list_cirurgia_situacoes(only_active=False)
            df2 = pd.DataFrame(sits_all2, columns=["id", "nome", "ativo", "ordem"]) if sits_all2 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_sits_cached"] = df2

            prox_id_s = (df2["id"].max() + 1) if not df2.empty else 1
            st.info(f"Próximo ID previsto: {prox_id_s}")

            _upload_db_situacao("Atualiza catálogo de Situações (salvar individual)")
        except Exception as e:
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

        from db import set_cirurgia_situacao_status
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
                        for _, r in df_sits.iterrows():
                            set_cirurgia_situacao_status(int(r["id"]), int(r["ativo"]))
                        st.success("Situações atualizadas.")

                        sits_all3 = list_cirurgia_situacoes(only_active=False)
                        df3 = pd.DataFrame(sits_all3, columns=["id", "nome", "ativo", "ordem"]) if sits_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_sits_cached"] = df3

                        prox_id_s = (df3["id"].max() + 1) if not df3.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id_s}")

                        _upload_db_situacao("Atualiza catálogo de Situações (aplicar alterações)")
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

    from db import list_procedimento_tipos

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
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_view.to_excel(writer, sheet_name="Tipos", index=False)
                wb = writer.book
                ws = writer.sheets["Tipos"]
                header_fmt = wb.add_format({"bold": True, "bg_color": "#DCE6F1", "border": 1})
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
        - **Status**: escolha **Ativos** para ver apenas os que aparecem na Aba **Cirurgias** (dropdown “Tipo (nome)”).
        - **Ordenação**: por padrão ordenamos por **ordem** e depois por **nome**.
        - **Busca**: digite parte do nome e pressione Enter.
        - **Paginação**: ajuste conforme necessário.
        - **Exportar**: baixa exatamente o que está filtrado/ordenado.
        """)
