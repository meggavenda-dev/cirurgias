
# db_merge.py
# -*- coding: utf-8 -*-
"""
Mesclagem de dois bancos SQLite do app (local ↔ remoto) em um terceiro arquivo de saída.

Regra geral:
- Começamos do REMOTO (cópia para output_path).
- Anexamos o LOCAL como 'localdb'.
- Executamos UPSERT por tabela, respeitando UNIQUE e preferindo 'last-write-wins' quando aplicável.

Tabelas e regras:
1) pacientes_unicos_por_dia_prestador
   UNIQUE(Hospital, Atendimento, Paciente, Prestador, Data)
   • Atualiza: Aviso, Convenio, Quarto.

2) procedimento_tipos
   UNIQUE(nome)
   • Atualiza: ativo, ordem.

3) cirurgia_situacoes
   UNIQUE(nome)
   • Atualiza: ativo, ordem.

4) cirurgias
   UNIQUE(Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
   • last-write-wins por updated_at.
   • created_at é mantido via COALESCE(local.created_at, remoto.created_at).

Uso:
    merge_sqlite_dbs(local_path, remote_path, output_path)

Requer:
- sqlalchemy
"""

from __future__ import annotations

import shutil
from sqlalchemy import create_engine, text


def merge_sqlite_dbs(local_path: str, remote_path: str, output_path: str) -> None:
    """
    Mescla os bancos SQLite 'local_path' e 'remote_path' gerando 'output_path'.

    Estratégia:
    1) Copia o REMOTO para 'output_path' (o remoto é a base).
    2) ATTACH DATABASE do LOCAL como 'localdb'.
    3) Executa INSERT ... ON CONFLICT DO UPDATE para cada tabela seguindo as regras definidas.
    """
    # 1) Copia o banco REMOTO para o arquivo de saída
    shutil.copyfile(remote_path, output_path)

    # 2) Conecta no banco de saída e anexa o LOCAL como 'localdb'
    eng = create_engine(f"sqlite:///{output_path}", future=True)
    with eng.begin() as conn:
        # Anexa o banco local
        conn.execute(text(f"ATTACH DATABASE '{local_path}' AS localdb;"))

        # ----------------------------------------------------
        # 1) Base de pacientes
        # ----------------------------------------------------
        conn.execute(text("""
            INSERT INTO pacientes_unicos_por_dia_prestador
            (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
            FROM localdb.pacientes_unicos_por_dia_prestador
            ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data)
            DO UPDATE SET
                Aviso    = excluded.Aviso,
                Convenio = excluded.Convenio,
                Quarto   = excluded.Quarto;
        """))

        # ----------------------------------------------------
        # 2) Catálogo de Tipos de Procedimento
        # ----------------------------------------------------
        conn.execute(text("""
            INSERT INTO procedimento_tipos (nome, ativo, ordem)
            SELECT nome, ativo, ordem
            FROM localdb.procedimento_tipos
            ON CONFLICT(nome) DO UPDATE
            SET ativo = excluded.ativo,
                ordem = excluded.ordem;
        """))

        # ----------------------------------------------------
        # 3) Catálogo de Situações da Cirurgia
        # ----------------------------------------------------
        conn.execute(text("""
            INSERT INTO cirurgia_situacoes (nome, ativo, ordem)
            SELECT nome, ativo, ordem
            FROM localdb.cirurgia_situacoes
            ON CONFLICT(nome) DO UPDATE
            SET ativo = excluded.ativo,
                ordem = excluded.ordem;
        """))

        # ----------------------------------------------------
        # 4) Cirurgias — last-write-wins por updated_at
        # ----------------------------------------------------
        conn.execute(text("""
            INSERT INTO cirurgias (
                Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
                Convenio, Procedimento_Tipo_ID, Situacao_ID,
                Guia_AMHPTISS, Guia_AMHPTISS_Complemento,
                Fatura, Observacoes, created_at, updated_at
            )
            SELECT
                l.Hospital, l.Atendimento, l.Paciente, l.Prestador, l.Data_Cirurgia,
                l.Convenio, l.Procedimento_Tipo_ID, l.Situacao_ID,
                l.Guia_AMHPTISS, l.Guia_AMHPTISS_Complemento,
                l.Fatura, l.Observacoes,
                COALESCE(l.created_at, r.created_at),
                CASE 
                    WHEN r.updated_at IS NULL THEN l.updated_at
                    WHEN l.updated_at IS NULL THEN r.updated_at
                    WHEN l.updated_at > r.updated_at THEN l.updated_at
                    ELSE r.updated_at
                END AS updated_at_resolved
            FROM localdb.cirurgias l
            LEFT JOIN cirurgias r
              ON r.Hospital   = l.Hospital
             AND r.Atendimento= l.Atendimento
             AND r.Paciente   = l.Paciente
             AND r.Prestador  = l.Prestador
             AND r.Data_Cirurgia = l.Data_Cirurgia
            ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
            DO UPDATE SET
                Convenio                   = excluded.Convenio,
                Procedimento_Tipo_ID       = excluded.Procedimento_Tipo_ID,
                Situacao_ID                = excluded.Situacao_ID,
                Guia_AMHPTISS              = excluded.Guia_AMHPTISS,
                Guia_AMHPTISS_Complemento  = excluded.Guia_AMHPTISS_Complemento,
                Fatura                     = excluded.Fatura,
                Observacoes                = excluded.Observacoes,
                updated_at                 = excluded.updated_at;
        """))

        # Desanexa o banco local
        conn.execute(text("DETACH DATABASE localdb;"))
