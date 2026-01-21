
# processing.py
import io
import csv
import re
import numpy as np
import pandas as pd
from dateutil import parser as dtparser  # reservado para futuras evoluções

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

REQUIRED_COLS = [
    "Data", "Prestador", "Hora_Inicio",
    "Atendimento", "Paciente", "Aviso",
    "Convenio", "Quarto"
]

# Conjunto de "hints" que indicam texto de procedimento (não nome de paciente)
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
    "CATETER", "AMIGDALECTOMIA LINGUAL", "CERVICOTOMIA", "TIREOIDECTOMIA",
    "LINFADENECTOMIA", "RECONSTRUÇÃO", "RETOSSIGMOIDECTOMIA", "PLEUROSCOPIA",
}

def _is_probably_procedure_token(tok) -> bool:
    """
    Heurística para sinalizar que um token parece ser texto de procedimento (não paciente).
    Evita avaliar boolean de pd.NA.
    """
    if tok is None or pd.isna(tok):
        return False
    T = str(tok).upper().strip()
    # Sinais de procedimento/painel técnico
    if any(h in T for h in PROCEDURE_HINTS):
        return True
    # Muitos sinais de "frase técnica"
    if ("," in T) or ("/" in T) or ("(" in T) or (")" in T) or ("%" in T) or ("  " in T) or ("-" in T):
        return True
    # Muito longo para nome de pessoa
    if len(T) > 50:
        return True
    return False

# =========================
# Normalização de colunas
# =========================

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza cabeçalhos para evitar KeyError:
    - remove BOM, espaços no início/fim
    - mapeia sinônimos/acento para nomes esperados
    """
    if df is None or df.empty:
        return df

    # strip + remove BOM
    df.columns = [
        str(c).replace("\ufeff", "").strip()
        for c in df.columns
    ]

    # mapa de sinônimos -> nomes esperados
    col_map = {
        "Convênio": "Convenio",
        "Convênio*": "Convenio",
        "Tipo Anestesia": "Tipo_Anestesia",
        "Hora Inicio": "Hora_Inicio",
        "Hora Início": "Hora_Inicio",
        "Hora Fim": "Hora_Fim",
        "Centro Cirurgico": "Centro",
        "Centro Cirúrgico": "Centro",
    }
    df.rename(columns=col_map, inplace=True)
    return df


# =========================
# Parser de texto bruto
# =========================

def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    """
    Parser robusto para CSV 'bruto' (relatórios exportados),
    lendo linha a linha em ordem original e extraindo campos.
    Corrigido para não confundir 'Paciente' com 'Cirurgia'.
    """
    rows = []
    current_section = None
    current_date_str = None
    ctx = {
        "atendimento": None, "paciente": None, "aviso": None,
        "hora_inicio": None, "hora_fim": None, "quarto": None
    }
    row_idx = 0

    for line in text.splitlines():
        # Detecta Data em qualquer linha
        m_date = DATE_RE.search(line)
        if m_date:
            current_date_str = m_date.group(1)

        # Tokeniza respeitando aspas
        tokens = next(csv.reader([line]))
        tokens = [t.strip() for t in tokens if t is not None]
        if not tokens:
            continue

        # Detecta seção (reinicia contexto)
        if "Centro Cirurgico" in line or "Centro Cirúrgico" in line:
            current_section = next(
                (kw for kw in SECTION_KEYWORDS if kw in line),
                None
            )
            ctx = {k: None for k in ctx}
            continue

        # Ignora cabeçalhos/rodapés
        header_phrases = [
            "Hora", "Atendimento", "Paciente", "Convênio", "Prestador",
            "Anestesista", "Tipo Anestesia", "Total", "Total Geral"
        ]
        if any(h in line for h in header_phrases):
            continue

        # Procura horários
        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]
        if time_idxs:
            h0 = time_idxs[0]
            h1 = h0 + 1 if (h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0 + 1])) else None
            hora_inicio = tokens[h0]
            hora_fim = tokens[h1] if h1 is not None else None

            # Aviso imediatamente antes do primeiro horário (código 3+ dígitos)
            aviso = None
            if h0 - 1 >= 0 and re.fullmatch(r"\d{3,}", tokens[h0 - 1]):
                aviso = tokens[h0 - 1]

            # Atendimento e Paciente
            atendimento = None
            paciente = None

            # Procura atendimento (número 7-10 dígitos)
            for i, t in enumerate(tokens):
                if re.fullmatch(r"\d{7,10}", t):
                    atendimento = t
                    # Limita a busca do paciente ao intervalo antes do horário (h0 - 2), para não pegar 'Cirurgia'
                    upper_bound = (h0 - 2) if h0 is not None else len(tokens) - 1
                    if upper_bound >= i + 1:
                        for j in range(i + 1, upper_bound + 1):
                            tj = tokens[j]
                            # Deve ter letras, não ser horário e não "parecer" procedimento
                            if tj and HAS_LETTER_RE.search(tj) and not TIME_RE.match(tj) and not _is_probably_procedure_token(tj):
                                paciente = tj
                                break
                    break

            base_idx = h1 if h1 is not None else h0
            cirurgia     = tokens[base_idx + 1] if base_idx + 1 < len(tokens) else None
            convenio     = tokens[base_idx + 2] if base_idx + 2 < len(tokens) else None
            prestador    = tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None
            anestesista  = tokens[base_idx + 4] if base_idx + 4 < len(tokens) else None
            tipo         = tokens[base_idx + 5] if base_idx + 5 < len(tokens) else None
            quarto       = tokens[base_idx + 6] if base_idx + 6 < len(tokens) else None

            rows.append({
                "Centro": current_section,
                "Data": current_date_str,
                "Atendimento": atendimento,
                "Paciente": paciente,
                "Aviso": aviso,
                "Hora_Inicio": hora_inicio,
                "Hora_Fim": hora_fim,
                "Cirurgia": cirurgia,
                "Convenio": convenio,
                "Prestador": prestador,
                "Anestesista": anestesista,
                "Tipo_Anestesia": tipo,
                "Quarto": quarto,
                "_row_idx": row_idx
            })

            # Atualiza contexto para eventuais linhas subsequentes sem horário
            ctx["atendimento"] = atendimento
            ctx["paciente"] = paciente
            ctx["aviso"] = aviso
            ctx["hora_inicio"] = hora_inicio
            ctx["hora_fim"] = hora_fim
            ctx["quarto"] = quarto

            row_idx += 1
            continue

        # Linhas sem horário (procedimentos adicionais) herdam contexto
        if current_section and any(tok for tok in tokens):
            nonempty = [t for t in tokens if t]
            if len(nonempty) >= 4:
                cirurgia     = nonempty[0]
                quarto       = nonempty[-1] if nonempty else None
                tipo         = nonempty[-2] if len(nonempty) >= 2 else None
                anestesista  = nonempty[-3] if len(nonempty) >= 3 else None
                prestador    = nonempty[-4] if len(nonempty) >= 4 else None
                convenio     = nonempty[-5] if len(nonempty) >= 5 else None

                rows.append({
                    "Centro": current_section,
                    "Data": current_date_str,
                    "Atendimento": ctx["atendimento"],
                    "Paciente": ctx["paciente"],
                    "Aviso": ctx["aviso"],
                    "Hora_Inicio": ctx["hora_inicio"],
                    "Hora_Fim": ctx["hora_fim"],
                    "Cirurgia": cirurgia,
                    "Convenio": convenio,
                    "Prestador": prestador,
                    "Anestesista": anestesista,
                    "Tipo_Anestesia": tipo,
                    "Quarto": quarto,
                    "_row_idx": row_idx
                })
                row_idx += 1

    return pd.DataFrame(rows)


# =========================
# Herança CONTROLADA
# =========================

def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    """
    Herança linha-a-linha por Data, preservando ordem original do arquivo.

    Regras:
    - Aplica herança somente quando há Prestador na linha atual.
    - 'Atendimento' e 'Aviso' herdam sempre que estiverem vazios e houver valor anterior.
    - 'Paciente' só herda se o último paciente conhecido (no mesmo dia) NÃO estiver vazio;
      caso contrário, mantém 'Paciente' em branco para edição posterior.
    - Linhas que venham sem 'Paciente' permanecem em branco.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)

    if "_row_idx" not in df.columns:
        df["_row_idx"] = range(len(df))

    if "Data" not in df.columns:
        return df

    # Garante que Data exista em todas as linhas
    df["Data"] = df["Data"].ffill().bfill()

    # Varre dia a dia na ordem original
    for _, grp in df.groupby("Data", sort=False):
        last_att = pd.NA
        last_pac = pd.NA
        last_aviso = pd.NA

        for i in grp.sort_values("_row_idx").index:
            att = df.at[i, "Atendimento"] if "Atendimento" in df.columns else pd.NA
            pac = df.at[i, "Paciente"] if "Paciente" in df.columns else pd.NA
            av  = df.at[i, "Aviso"] if "Aviso" in df.columns else pd.NA

            # Atualiza memória com valores não vazios
            if pd.notna(att) and str(att).strip():
                last_att = att
            if pd.notna(pac) and str(pac).strip():
                last_pac = pac
            if pd.notna(av) and str(av).strip():
                last_aviso = av

            # Herança só se houver Prestador na linha atual
            has_prestador = (
                "Prestador" in df.columns and
                pd.notna(df.at[i, "Prestador"]) and
                str(df.at[i, "Prestador"]).strip() != ""
            )
            if not has_prestador:
                continue

            # Atendimento: herda se vazio
            if "Atendimento" in df.columns and (pd.isna(att) or str(att).strip() == "") and pd.notna(last_att):
                df.at[i, "Atendimento"] = last_att

            # Aviso: herda se vazio
            if "Aviso" in df.columns and (pd.isna(av) or str(av).strip() == "") and pd.notna(last_aviso):
                df.at[i, "Aviso"] = last_aviso

            # Paciente: herda somente se last_pac não estiver vazio; senão mantém blank
            if "Paciente" in df.columns and (pd.isna(pac) or str(pac).strip() == ""):
                if pd.notna(last_pac) and str(last_pac).strip() != "":
                    df.at[i, "Paciente"] = last_pac
                else:
                    df.at[i, "Paciente"] = pd.NA

    return df


# =========================
# Sanitização do campo Paciente
# =========================

def _sanitize_patient_field(df: pd.DataFrame) -> pd.DataFrame:
    """
    Zera 'Paciente' quando:
    - Paciente == Cirurgia
    - Paciente contém hints de procedimento
    - Paciente aparenta texto técnico (muito longo / vírgulas / barras / parênteses / traços)
    (Evita avaliar boolean de pd.NA)
    """
    if df is None or df.empty:
        return df
    df = df.copy()

    def clean_pac(row: pd.Series) -> str:
        pac_val = row.get("Paciente", pd.NA)
        cir_val = row.get("Cirurgia", pd.NA)

        pac = "" if pd.isna(pac_val) else str(pac_val).strip()
        cir = "" if pd.isna(cir_val) else str(cir_val).strip()

        if pac == "":
            return pac  # já está vazio

        # Igual à cirurgia -> não é paciente
        if cir and pac.upper() == cir.upper():
            return ""  # zera

        # Heurística de procedimento
        if _is_probably_procedure_token(pac):
            return ""  # zera

        return pac

    if "Paciente" not in df.columns:
        df["Paciente"] = pd.NA

    df["Paciente"] = df.apply(clean_pac, axis=1).replace({"": pd.NA})
    return df


# =========================
# Pipeline principal
# =========================

def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    """
    Entrada:
      - upload: arquivo enviado pelo Streamlit (CSV/Excel/Texto)
      - prestadores_lista: lista de prestadores alvo (strings)
      - selected_hospital: nome do Hospital informado no app (aplicado a todas as linhas)

    Saída:
      DataFrame final com colunas:
        Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
    """
    name = upload.name.lower()

    # 1) Ler arquivo (CSV/Excel ou texto bruto)
    if name.endswith(".xlsx"):
        df_in = pd.read_excel(upload, engine="openpyxl")
    elif name.endswith(".xls"):
        df_in = pd.read_excel(upload, engine="xlrd")
    elif name.endswith(".csv"):
        try:
            df_in = pd.read_csv(upload, sep=",", encoding="utf-8")
            # Se não tem colunas suficientes, parseia como texto bruto
            if len(set(EXPECTED_COLS) & set(df_in.columns)) < 6:
                upload.seek(0)
                text = upload.read().decode("utf-8", errors="ignore")
                df_in = _parse_raw_text_to_rows(text)
        except Exception:
            upload.seek(0)
            text = upload.read().decode("utf-8", errors="ignore")
            df_in = _parse_raw_text_to_rows(text)
    else:
        text = upload.read().decode("utf-8", errors="ignore")
        df_in = _parse_raw_text_to_rows(text)

    # 1.1) Normaliza colunas e garante mínimas
    df_in = _normalize_columns(df_in)

    if "_row_idx" not in df_in.columns:
        df_in["_row_idx"] = range(len(df_in))

    for c in REQUIRED_COLS:
        if c not in df_in.columns:
            # cria coluna vazia com alinhamento de índice
            df_in[c] = pd.NA

    # 2) Herança linha-a-linha por Data
    df = _herdar_por_data_ordem_original(df_in)

    # 2.1) Sanitização do campo Paciente (evita nomes de procedimentos)
    df = _sanitize_patient_field(df)

    # 3) Filtro de prestadores (case-insensitive com normalização)
    def norm(s):
        s = "" if (s is None or pd.isna(s)) else str(s)
        return s.strip().upper()

    target = [norm(p) for p in prestadores_lista]

    # Garante coluna Prestador
    if "Prestador" not in df.columns:
        df["Prestador"] = pd.NA

    df["Prestador_norm"] = df["Prestador"].apply(norm)
    df = df[df["Prestador_norm"].isin(target)].copy()

    # 4) Ordenação por tempo
    hora_inicio = df["Hora_Inicio"] if "Hora_Inicio" in df.columns else pd.Series("", index=df.index)
    data_series = df["Data"] if "Data" in df.columns else pd.Series("", index=df.index)

    df["start_key"] = pd.to_datetime(
        data_series.fillna("").astype(str) + " " + hora_inicio.fillna("").astype(str),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )

    # 4.1) Deduplicação híbrida (corrige contagem 32 vs 37)
    def _norm_blank(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip().str.upper()

    P = _norm_blank(df.get("Paciente", pd.Series(index=df.index)))
    A = _norm_blank(df.get("Atendimento", pd.Series(index=df.index)))
    V = _norm_blank(df.get("Aviso", pd.Series(index=df.index)))
    D = _norm_blank(df.get("Data", pd.Series(index=df.index)))
    PR = df["Prestador_norm"].fillna("").astype(str)

    # Tag de deduplicação: P (preferencial), senão A, senão V, senão fallback com tempo
    df["__dedup_tag"] = np.where(P != "",
                                 "P|" + D + "|" + P + "|" + PR,
                                 np.where(A != "",
                                          "A|" + D + "|" + A + "|" + PR,
                                          np.where(V != "",
                                                   "V|" + D + "|" + V + "|" + PR,
                                                   "T|" + D + "|" + PR + "|" + df["start_key"].astype(str)
                                          )
                                 ))

    df = df.sort_values(["Data", "Paciente", "Prestador_norm", "start_key"])
    df = df.drop_duplicates(subset=["__dedup_tag"], keep="first")
    df = df.drop(columns=["__dedup_tag"])

    # 5) Hospital + Ano/Mes/Dia
    hosp = selected_hospital if (selected_hospital and not pd.isna(selected_hospital)) else ""
    hosp = hosp.strip() or "Hospital não informado"
    df["Hospital"] = hosp

    # Garante coluna Data antes de extrair Ano/Mes/Dia
    if "Data" not in df.columns:
        df["Data"] = pd.NA

    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Ano"] = dt.dt.year
    df["Mes"] = dt.dt.month
    df["Dia"] = dt.dt.day

    # 6) Seleção das colunas finais (organizado por ano/mês/dia)
    final_cols = [
        "Hospital", "Ano", "Mes", "Dia",
        "Data", "Atendimento", "Paciente", "Aviso",
        "Convenio", "Prestador", "Quarto"
    ]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA

    out = df[final_cols].copy()

    # Ordenação para retorno
    out = out.sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]).reset_index(drop=True)
    return out
