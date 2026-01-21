
# -*- coding: utf-8 -*-
"""
processing.py — Parser e pipeline de normalização/deduplicação
para a aba 'Importação & Pacientes'.

Alterações-chave:
- Normaliza Aviso (somente dígitos) e resolve conflitos por (Data, Atendimento)
- Deduplica por (Data, Prestador, Atendimento) preservando ordem original
"""

import io
import csv
import re
import unicodedata
import numpy as np
import pandas as pd

# =========================
# Regex / Constantes
# =========================

TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
HAS_LETTER_RE = re.compile(r"[A-Za-zÁÉÍÓÚÃÕÇáéíóúãõç]")
SECTION_KEYWORDS = ["CENTRO CIRURGICO", "HEMODINAMICA", "CENTRO OBSTETRICO"]

EXPECTED_COLS = [
    "Centro", "Data", "Atendimento", "Paciente", "Aviso",
    "Hora_Inicio", "Hora_Fim", "Cirurgia", "Convenio", "Prestador",
    "Anestesista", "Tipo_Anestesia", "Quarto"
]

PROCEDURE_HINTS = {
    "HERNIA", "HERNIORRAFIA", "COLECISTECTOMIA", "APENDICECTOMIA",
    "ENDOMETRIOSE", "SINOVECTOMIA", "OSTEOCONDROPLASTIA", "ARTROPLASTIA",
    "ADENOIDECTOMIA", "AMIGDALECTOMIA", "ETMOIDECTOMIA", "SEPTOPLASTIA",
    "TURBINECTOMIA", "MIOMECTOMIA", "HISTEROSCOPIA", "HISTERECTOMIA",
    "ENXERTO", "TENOLISE", "MICRONEUROLISE", "URETERO", "NEFRECTOMIA",
    "LAPAROTOMIA", "LAPAROSCOPICA", "ROBOTICA", "BIOPSIA", "CRANIOTOMIA",
    "RETIRADA", "DRENAGEM", "FISTULECTOMIA", "HEMOSTA", "ARTRODESE",
    "OSTEOTOMIA", "SEPTOPLASTA", "CIRURGIA", "EXERESE", "RESSECCAO",
    "URETEROLITOTRIPSIA", "URETEROSCOPIA", "ENDOSCOPICA", "ENDOSCOPIA",
    "CATETER", "CERVICOTOMIA", "TIREOIDECTOMIA", "LINFADENECTOMIA",
    "RECONSTRUÇÃO", "RETOSSIGMOIDECTOMIA", "PLEUROSCOPIA",
}

# =========================
# Funções Auxiliares
# =========================

def _is_probably_procedure_token(tok) -> bool:
    if tok is None or pd.isna(tok): 
        return False
    T = str(tok).upper().strip()
    if any(h in T for h in PROCEDURE_HINTS): 
        return True
    if any(c in T for c in [",", "/", "(", ")", "%", "  ", "-"]): 
        return True
    if len(T) > 50: 
        return True
    return False

def _strip_accents(s: str) -> str:
    if s is None or pd.isna(s): 
        return ""
    s = str(s)
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: 
        return df
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    col_map = {
        "Convênio": "Convenio", "Convênio*": "Convenio",
        "Tipo Anestesia": "Tipo_Anestesia", "Hora Inicio": "Hora_Inicio",
        "Hora Início": "Hora_Inicio", "Hora Fim": "Hora_Fim",
        "Centro Cirurgico": "Centro", "Centro Cirúrgico": "Centro",
    }
    df.rename(columns=col_map, inplace=True)
    return df

# =========================
# Parser de texto bruto (robusto p/ cabeçalhos repetidos)
# =========================

def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    rows = []
    current_section = None
    current_date_str = None
    ctx = {"hora_inicio": None}
    row_idx = 0

    for line in text.splitlines():
        # Captura data apenas em linhas de "Data de Realização"
        if "Data de Realização" in line or "Data de Realiza" in line:
            m_date = DATE_RE.search(line)
            if m_date:
                current_date_str = m_date.group(1)

        # Tokenização tolerante a vírgulas/aspas
        try:
            tokens = next(csv.reader([line]))
            tokens = [t.strip() for t in tokens if t is not None]
        except Exception:
            continue

        if not tokens:
            continue

        # Detecta seção
        if "Centro Cirurgico" in line or "Centro Cirúrgico" in line:
            current_section = next((kw for kw in SECTION_KEYWORDS if kw in line), None)
            ctx = {"hora_inicio": None}
            continue

        # Ignora cabeçalhos óbvios
        header_phrases = ["Hora", "Atendimento", "Paciente", "Convênio", "Prestador"]
        if any(h in line for h in header_phrases):
            continue

        # Linhas com horários → linha "principal" do caso
        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]
        if time_idxs:
            h0 = time_idxs[0]
            h1 = h0 + 1 if (h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0+1])) else None
            hora_inicio, hora_fim = tokens[h0], (tokens[h1] if h1 else None)

            # Aviso: token imediatamente anterior ao horário, se numérico
            aviso = tokens[h0-1] if (h0-1 >= 0 and re.fullmatch(r"\d{3,}", tokens[h0-1])) else None

            # Atendimento e Paciente
            atendimento, paciente = None, None
            for i, t in enumerate(tokens):
                if re.fullmatch(r"\d{7,10}", t):  # atendimento típico 7-10 dígitos
                    atendimento = t
                    upper_bound = (h0 - 2) if h0 else len(tokens) - 1
                    for j in range(i+1, upper_bound+1):
                        if j < len(tokens) and HAS_LETTER_RE.search(tokens[j]) and not TIME_RE.match(tokens[j]) and not _is_probably_procedure_token(tokens[j]):
                            paciente = tokens[j]
                            break
                    break

            base_idx = h1 if h1 else h0
            cirurgia, convenio = (tokens[base_idx + i] if base_idx + i < len(tokens) else None for i in [1, 2])

            # Prestador pode vir acompanhado de uma data (ex.: nascimento) → pular para o próximo token
            p_cand = tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None
            if p_cand and DATE_RE.search(p_cand):
                prestador = tokens[base_idx + 4] if base_idx + 4 < len(tokens) else p_cand
                anest, tipo, quarto = (tokens[base_idx+i] if base_idx+i < len(tokens) else None for i in [5, 6, 7])
            else:
                prestador = p_cand
                anest, tipo, quarto = (tokens[base_idx+i] if base_idx+i < len(tokens) else None for i in [4, 5, 6])

            rows.append({
                "Centro": current_section, "Data": current_date_str, "Atendimento": atendimento,
                "Paciente": paciente, "Aviso": aviso, "Hora_Inicio": hora_inicio, "Hora_Fim": hora_fim,
                "Cirurgia": cirurgia, "Convenio": convenio, "Prestador": prestador,
                "Anestesista": anest, "Tipo_Anestesia": tipo, "Quarto": quarto, "_row_idx": row_idx
            })
            ctx["hora_inicio"] = hora_inicio
            row_idx += 1
            continue

        # Linhas complementares dentro da mesma seção (sem horário)
        if current_section and any(t for t in tokens):
            nonempty = [t for t in tokens if t]
            if len(nonempty) >= 4:
                rows.append({
                    "Centro": current_section, "Data": current_date_str, "Atendimento": None,
                    "Paciente": None, "Aviso": None, "Hora_Inicio": ctx["hora_inicio"],
                    "Cirurgia": nonempty[0], "Convenio": nonempty[-5] if len(nonempty) >= 5 else None,
                    "Prestador": nonempty[-4], "Anestesista": nonempty[-3], "Tipo_Anestesia": nonempty[-2],
                    "Quarto": nonempty[-1], "_row_idx": row_idx
                })
                row_idx += 1

    return pd.DataFrame(rows)

# ===========================================
# Herança - TRAVA POR BLOCO E POR MÉDICO
# ===========================================

def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    """
    Copia Atendimento/Paciente/Aviso ao longo de blocos dentro da mesma Data,
    garantindo que cada médico fique com um único conjunto herdado por bloco.
    """
    if df is None or df.empty: 
        return df
    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)
    df["Data"] = df["Data"].ffill().bfill()

    if "_row_idx" not in df.columns:
        df["_row_idx"] = range(len(df))

    for _, grp in df.groupby("Data", sort=False):
        last_att, last_pac, last_av = pd.NA, pd.NA, pd.NA
        medicos_no_bloco = set()

        for i in grp.sort_values("_row_idx").index:
            curr_att = df.at[i, "Atendimento"]
            curr_pac = df.at[i, "Paciente"]
            curr_av  = df.at[i, "Aviso"]
            curr_prest_raw = df.at[i, "Prestador"]
            curr_prest = str(curr_prest_raw).strip().upper() if pd.notna(curr_prest_raw) else ""

            tem_dados_nativos = pd.notna(curr_att) or pd.notna(curr_pac) or pd.notna(curr_av)

            if tem_dados_nativos:
                # Se mudou o bloco (novo atendimento/aviso), reseta o conjunto de médicos no bloco
                str_curr_att = str(curr_att) if pd.notna(curr_att) else "None"
                str_last_att = str(last_att) if pd.notna(last_att) else "None"
                str_curr_av  = str(curr_av)  if pd.notna(curr_av)  else "None"
                str_last_av  = str(last_av)  if pd.notna(last_av)  else "None"

                if (str_curr_att != str_last_att) or (str_curr_av != str_last_av):
                    medicos_no_bloco = set()

                last_att, last_pac, last_av = curr_att, curr_pac, curr_av
                if curr_prest != "":
                    medicos_no_bloco.add(curr_prest)
            else:
                # Herdar uma única vez por médico dentro do bloco
                if curr_prest != "" and curr_prest not in medicos_no_bloco:
                    df.at[i, "Atendimento"] = last_att
                    df.at[i, "Paciente"]    = last_pac
                    df.at[i, "Aviso"]       = last_av
                    medicos_no_bloco.add(curr_prest)

    return df

# ===========================================
# Normalização/Resolução de 'Aviso' + Diagnóstico
# ===========================================

def _normalize_and_resolve_aviso_conflicts(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Normaliza Aviso para conter apenas dígitos.
    - Resolve conflitos de Aviso por (Data, Atendimento) com regra determinística:
        mais frequente -> se empate, mais longo -> se empate, primeiro na ordem original.
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # Aviso somente dígitos (remove ruído)
    df["Aviso"] = (
        df["Aviso"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)  # pega sequência numérica
        .str.strip()
    )

    def _pick(series: pd.Series) -> str | None:
        s = series.dropna().astype(str)
        if s.empty:
            return None
        vc = s.value_counts()
        top_count = vc.max()
        candidatos = vc[vc == top_count].index.tolist()
        if len(candidatos) == 1:
            return candidatos[0]
        # Empate: mais longo, depois pela primeira posição na ordem original
        # Para recuperar "ordem original", usamos a primeira ocorrência em s.tolist()
        candidatos_sorted = sorted(
            candidatos, 
            key=lambda x: (len(x), s.tolist().index(x)), 
            reverse=True
        )
        return candidatos_sorted[0]

    df["Aviso"] = df.groupby(["Data", "Atendimento"], dropna=False)["Aviso"].transform(_pick)
    return df

def _diagnose_aviso_conflicts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna uma tabela com conflitos de Aviso por (Data, Atendimento),
    útil para exibir no Streamlit (opcional).
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["Data", "Atendimento", "Avisos_Diferentes", "Qtd"])
    g = df.groupby(["Data", "Atendimento"])["Aviso"].nunique(dropna=True)
    confl = g[g > 1].reset_index(name="Qtd")
    if confl.empty: 
        return confl
    avisos = df.groupby(["Data", "Atendimento"])["Aviso"].apply(
        lambda s: sorted(set(x for x in s.dropna().astype(str)))
    ).reset_index(name="Avisos_Diferentes")
    return confl.merge(avisos, on=["Data", "Atendimento"], how="left")

# =========================
# Pipeline principal
# =========================

def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    """
    Lê upload (CSV bruto ou texto), faz parsing, herança, filtro por prestadores,
    normaliza/deduplica e retorna colunas: 
    ['Hospital','Ano','Mes','Dia','Data','Atendimento','Paciente','Aviso','Convenio','Prestador','Quarto']
    """

    # 1) Ler CSV; se estrutura inesperada, tratar como texto bruto
    name = getattr(upload, "name", "").lower()
    if name.endswith(".csv"):
        try:
            df_in = pd.read_csv(upload, sep=",", encoding="utf-8")
            if len(set(EXPECTED_COLS) & set(df_in.columns)) < 6:
                upload.seek(0)
                text = upload.read().decode("utf-8", errors="ignore")
                df_in = _parse_raw_text_to_rows(text)
        except Exception:
            # fallback: texto bruto
            upload.seek(0)
            text = upload.read().decode("utf-8", errors="ignore")
            df_in = _parse_raw_text_to_rows(text)
    else:
        # Arquivos não-CSV chegam (na prática) como texto bruto exportado
        text = upload.read().decode("utf-8", errors="ignore")
        df_in = _parse_raw_text_to_rows(text)

    # 2) Normaliza nomes de colunas e garante _row_idx
    df_in = _normalize_columns(df_in)
    if "_row_idx" not in df_in.columns:
        df_in["_row_idx"] = range(len(df_in))

    # 3) Herança por data/ordem original (trava por bloco e por médico)
    df = _herdar_por_data_ordem_original(df_in)

    # 4) Filtro de prestadores escolhidos (case/acentos insensitive)
    target = [_strip_accents(p).strip().upper() for p in prestadores_lista]
    df["Prestador_norm"] = df["Prestador"].apply(lambda x: _strip_accents(x).strip().upper())
    df = df[df["Prestador_norm"].isin(target)].copy()

    # 5) Remover linhas sem nenhum dos 3 pilares (Atendimento/Paciente/Aviso)
    df = df.dropna(subset=["Atendimento", "Paciente", "Aviso"], how="all")

    # 6) Datas + metadados do hospital
    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Hospital"], df["Ano"], df["Mes"], df["Dia"] = selected_hospital, dt.dt.year, dt.dt.month, dt.dt.day

    # 7) Normaliza/resolve 'Aviso' e deduplica por (Data, Prestador, Atendimento)
    df = _normalize_and_resolve_aviso_conflicts(df)

    # Ordenação estável pela ordem original do arquivo
    if "_row_idx" in df.columns:
        df = df.sort_values(["Ano", "Mes", "Dia", "_row_idx"], kind="mergesort")
    else:
        df = df.sort_values(["Ano", "Mes", "Dia"], kind="mergesort")

    # Deduplicação prática p/ "Pacientes únicos por dia e prestador"
    df = df.drop_duplicates(subset=["Data", "Prestador", "Atendimento"], keep="first")

    # 8) Seleção de colunas finais
    cols_to_return = [
        "Hospital", "Ano", "Mes", "Dia", "Data",
        "Atendimento", "Paciente", "Aviso",
        "Convenio", "Prestador", "Quarto"
    ]
    # Garante presença e ordem; se faltar alguma no parsing, cria vazia
    for c in cols_to_return:
        if c not in df.columns:
            df[c] = "" if c in {"Data","Atendimento","Paciente","Aviso","Convenio","Prestador","Quarto","Hospital"} else np.nan

    return df[cols_to_return].reset_index(drop=True)

