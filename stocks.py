import sqlite3

# Caminho para o banco de dados SQLite
db_path = 'b3_stocks.db'

# Função para criar a tabela e adicionar alguns tickers
def initialize_database():
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Criar tabela se não existir
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL
        )
        ''')

        # Adicionar alguns tickers de exemplo
        sample_tickers = ['PETR4.SA', 'VALE3.SA', 'ABEV3.SA']
        cursor.executemany('INSERT INTO stocks (ticker) VALUES (?)', [(ticker,) for ticker in sample_tickers])

        conn.commit()
        conn.close()
        print("Tabela 'stocks' criada e tickers adicionados com sucesso.")
    except sqlite3.Error as e:
        print(f"Erro ao inicializar o banco de dados: {e}")

# Inicializar o banco de dados
initialize_database()
