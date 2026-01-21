
# export.py
import io
import re
import pandas as pd

# ---------------- Helpers de formatação ----------------

_INVALID_SHEET_CHARS_RE = re.compile(r'[:\\/?*\[\]]')

def _sanitize_sheet_name(name: str, fallback: str = "Dados") -> str:
    """
    Limpa o nome da aba para atender restrições do Excel:
    - remove caracteres inválidos: : \ / ? * [ ]
    - limita a 31 caracteres
    - se vazio após limpeza, usa fallback
    """
    if not name:
        name = fallback

    name = str(name).strip()
    name = _INVALID_SHEET_CHARS_RE.sub("", name)

    if not name:
        name = fallback

    # Excel limita a 31 caracteres
    return name[:31]


def _write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
    """
    Escreve o DataFrame com cabeçalho formatado, autofiltro e ajuste de larguras.
    """
    if df is None or df.empty:
        return

    df = df.copy()

    # Converte colunas com objetos complexos em string para evitar erros de escrita
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(lambda x: "" if x is None else str(x))

    df.to_excel(writer, sheet_name=sheet_name, index=False)
    wb = writer.book
    ws = writer.sheets[sheet_name]

    # Cabeçalho
    header_fmt = wb.add_format({
        "bold": True,
        "bg_color": "#DCE6F1",
        "border": 1
    })

    for col_num, value in enumerate(df.columns.values):
        ws.write(0, col_num, value, header_fmt)

    # Autofiltro (range correto)
    last_row = max(len(df), 1)
    ws.autofilter(0, 0, last_row, max(0, len(df.columns) - 1))

    # Ajuste automático de largura com limites razoáveis
    for i, col in enumerate(df.columns):
        valores = [str(x) for x in df[col].tolist()]
        maxlen = max([len(str(col))] + [len(v) for v in valores if v]) + 2
        ws.set_column(i, i, max(14, min(maxlen, 60)))


# ---------------- Exportações (Pacientes) ----------------

def to_formatted_excel_cirurgias(df: pd.DataFrame) -> io.BytesIO:
    """
    Gera um Excel organizado por Hospital, garantindo que as colunas
    estjam na ordem correta e visualmente limpas.
    """
    output = io.BytesIO()
    
    if df is None or df.empty:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            pd.DataFrame({"Aviso": ["Nenhum dado encontrado para os filtros selecionados"]}).to_excel(writer, index=False)
        output.seek(0)
        return output

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Se houver a coluna Hospital, criamos abas separadas
        hospital_col = "Hospital" if "Hospital" in df.columns else None
        
        if hospital_col:
            hospitais = df[hospital_col].unique()
            for hosp in sorted(hospitais):
                df_hosp = df[df[hospital_col] == hosp].copy()
                # Removemos colunas técnicas do Excel final
                cols_to_drop = ["id", "Procedimento_Tipo_ID", "Situacao_ID", "has_id", "_row_idx"]
                df_hosp = df_hosp.drop(columns=[c for c in cols_to_drop if c in df_hosp.columns], errors="ignore")
                
                sheet_name = _sanitize_sheet_name(str(hosp))
                _write_sheet(writer, sheet_name, df_hosp)
        else:
            _write_sheet(writer, "Cirurgias", df)

    output.seek(0)
    return output


def to_formatted_excel_by_hospital(df: pd.DataFrame) -> io.BytesIO:
    """
    Gera um Excel com uma aba por Hospital. Proteção contra None incluída.
    """
    output = io.BytesIO()

    # ✅ Validação e coerção de tipo (corrige erro de atributo .columns)
    if df is None or not hasattr(df, "columns"):
        try:
            df = pd.DataFrame(df)
        except Exception:
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                pd.DataFrame({"Aviso": ["Nenhum dado disponível para exportação"]}).to_excel(writer, index=False)
            output.seek(0)
            return output

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if "Hospital" not in df.columns:
            _write_sheet(writer, "Dados", df)
        else:
            df_aux = df.copy()
            df_aux["Hospital"] = (
                df_aux["Hospital"]
                .fillna("Sem_Hospital")
                .astype(str)
                .str.strip()
                .replace("", "Sem_Hospital")
            )

            order_cols = [c for c in ["Ano", "Mes", "Dia", "Paciente", "Prestador"] if c in df_aux.columns]

            # Ordena hospitais para gerar abas previsíveis
            for hosp in sorted(df_aux["Hospital"].unique()):
                dfh = df_aux[df_aux["Hospital"] == hosp].copy()
                if order_cols:
                    dfh = dfh.sort_values(order_cols, kind="mergesort")

                sheet_name = _sanitize_sheet_name(hosp, fallback="Sem_Hospital")
                _write_sheet(writer, sheet_name, dfh)

    output.seek(0)
    return output


# ---------------- Exportações (Cirurgias) ----------------

def to_formatted_excel_cirurgias(df: pd.DataFrame) -> io.BytesIO:
    """
    Exporta cirurgias em Excel com proteção contra dados nulos.
    """
    output = io.BytesIO()

    # ✅ Validação e coerção de tipo
    if df is None or not hasattr(df, "columns"):
        try:
            df = pd.DataFrame(df)
        except Exception:
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                pd.DataFrame({"Aviso": ["Nenhum dado disponível para exportação"]}).to_excel(writer, index=False)
            output.seek(0)
            return output

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if "Hospital" not in df.columns:
            _write_sheet(writer, "Cirurgias", df)
        else:
            df_aux = df.copy()
            df_aux["Hospital"] = (
                df_aux["Hospital"]
                .fillna("Sem_Hospital")
                .astype(str)
                .str.strip()
                .replace("", "Sem_Hospital")
            )

            # Ordena hospitais para gerar abas consistentes
            for hosp in sorted(df_aux["Hospital"].unique()):
                dfh = df_aux[df_aux["Hospital"] == hosp].copy()

                # Ordena colunas se existirem
                order_cols = [c for c in ["Data_Cirurgia", "Paciente"] if c in dfh.columns]
                if order_cols:
                    dfh = dfh.sort_values(order_cols, kind="mergesort")

                sheet_name = _sanitize_sheet_name(hosp, fallback="Sem_Hospital")
                _write_sheet(writer, sheet_name, dfh)

    output.seek(0)
    return output
