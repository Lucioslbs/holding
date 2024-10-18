import yfinance as yf
import pandas as pd
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

# Função para buscar tickers no banco de dados
def get_tickers_from_database():
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT ticker FROM stocks")
        tickers = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tickers
    except sqlite3.Error as e:
        print(f"Erro ao buscar tickers no banco de dados: {e}")
        return []

def filter_stocks_above_sma30(tickers):
    result = []
    for ticker in tickers:
        try:
            # Obter dados históricos de preços
            data = yf.download(ticker, period='1mo', interval='1d')

            if data.empty:
                print(f"Sem dados para {ticker}")
                continue

            if len(data) >= 30:  # Certificar-se de ter pelo menos 30 dias de dados
                # Calcular a SMA de 30 períodos
                data['SMA30'] = data['Close'].rolling(window=30).mean()

                # Verificar se o preço de fechamento atual está acima da SMA30
                if data['Close'].iloc[-1] > data['SMA30'].iloc[-1]:
                    result.append(ticker)
        except Exception as e:
            print(f"Erro ao processar ticker {ticker}: {e}")

    return result

# Inicializar o banco de dados
initialize_database()

# Carregar tickers do banco de dados
tickers = get_tickers_from_database()

# Filtrar ações com preço acima da SMA30
if tickers:
    filtered_stocks = filter_stocks_above_sma30(tickers)
    if filtered_stocks:
        df = pd.DataFrame(filtered_stocks, columns=["Ações com preço acima da SMA30"])
        df.to_csv('analysis_result.csv', index=False)
        print("A tabela foi salva no arquivo 'analysis_result.csv'.")
    else:
        print("Nenhuma ação com preço acima da SMA30 foi encontrada.")
else:
    print("Não foi possível carregar a lista de tickers do banco de dados.")
