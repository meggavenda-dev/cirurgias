import sqlite3
import os

# Caminho para o seu banco de dados
DB_PATH = "data/exemplo.db" # Ajuste conforme o caminho real no seu projeto

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Erro: O arquivo {DB_PATH} não foi encontrado.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Colunas que queremos garantir que existam
    # O formato é (Nome da Coluna, Tipo SQL)
    novas_colunas = [
        ("Data_Pagamento", "TEXT"),
        ("Guia_AMHPTISS_Complemento", "TEXT")
    ]

    for coluna, tipo in novas_colunas:
        try:
            # Tenta adicionar a coluna
            cursor.execute(f"ALTER TABLE cirurgias ADD COLUMN {coluna} {tipo}")
            print(f"✅ Coluna '{coluna}' adicionada com sucesso.")
        except sqlite3.OperationalError as e:
            # Se a coluna já existir, o SQLite lançará este erro
            if "duplicate column name" in str(e).lower():
                print(f"ℹ️ Coluna '{coluna}' já existe. Pulando...")
            else:
                print(f"❌ Erro ao adicionar '{coluna}': {e}")

    conn.commit()
    conn.close()
    print("\nSincronização concluída.")

if __name__ == "__main__":
    migrate()
