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

def calculate_rsi(data, window=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def analyze_tickers(tickers):
    analysis_result = []

    for ticker in tickers:
        try:
            # Obter dados históricos de preços
            data = yf.download(ticker, period='6mo', interval='1d')

            if data.empty:
                print(f"Sem dados para {ticker}")
                continue

            # Calcular RSI
            data['RSI'] = calculate_rsi(data)

            # Analisar RSI
            rsi_current = data['RSI'].iloc[-1]

            if rsi_current < 30:
                analysis_result.append((ticker, rsi_current))

        except Exception as e:
            print(f"Erro ao processar ticker {ticker}: {e}")

    return analysis_result

# Inicializar o banco de dados
initialize_database()

# Carregar tickers do banco de dados
tickers = get_tickers_from_database()

# Analisar tickers
if tickers:
    analysis_result = analyze_tickers(tickers)
    if analysis_result:
        df = pd.DataFrame(analysis_result, columns=["Ticker", "RSI"])
        df = df.drop_duplicates()
        df.to_csv('analysis_result.csv', index=False)
        print("A tabela foi salva no arquivo 'analysis_result.csv'.")
    else:
        print("Nenhuma ação com RSI abaixo de 30 foi encontrada.")
else:
    print("Não foi possível carregar a lista de tickers do banco de dados.")
