import yfinance as yf
import pandas as pd
import sqlite3

# Caminho para o banco de dados SQLite
db_path = 'b3_stocks.db'

# Caminho para o arquivo CSV
csv_path = 'b3_stocks.csv'

# Função para buscar tickers no arquivo CSV
def get_tickers_from_csv():
    try:
        df = pd.read_csv(csv_path)
        tickers = df['Ticker'].tolist()
        return tickers
    except Exception as e:
        print(f"Erro ao buscar tickers no arquivo CSV: {e}")
        return []

# Função para criar a tabela e adicionar os tickers do CSV
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

        # Carregar tickers do arquivo CSV
        tickers = get_tickers_from_csv()
        if tickers:
            cursor.executemany('INSERT INTO stocks (ticker) VALUES (?)', [(ticker,) for ticker in tickers])
            conn.commit()
            print("Tabela 'stocks' criada e tickers adicionados com sucesso.")
        else:
            print("Nenhum ticker encontrado no arquivo CSV.")

        conn.close()
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

def filter_stocks_above_atr20(tickers):
    result = []
    for ticker in tickers:
        try:
            # Obter dados históricos de preços
            data = yf.download(ticker, period='3mo', interval='1d')

            if data.empty:
                print(f"Sem dados para {ticker}")
                continue

            if len(data) >= 20:  # Certificar-se de ter pelo menos 20 dias de dados
                # Calcular o ATR de 20 períodos
                data['TR'] = data[['High', 'Low', 'Close']].max(axis=1) - data[['High', 'Low', 'Close']].min(axis=1)
                data['ATR20'] = data['TR'].rolling(window=20).mean()

                # Verificar se o preço de fechamento atual está acima do ATR20
                if data['Close'].iloc[-1] > data['ATR20'].iloc[-1]:
                    result.append(ticker)
        except Exception as e:
            print(f"Erro ao processar ticker {ticker}: {e}")

    return result

# Inicializar o banco de dados e adicionar tickers do CSV
initialize_database()

# Carregar tickers do banco de dados
tickers = get_tickers_from_database()

# Filtrar ações com preço acima do ATR20
if tickers:
    filtered_stocks = filter_stocks_above_atr20(tickers)
    if filtered_stocks:
        df = pd.DataFrame(filtered_stocks, columns=["Ações com preço acima do ATR20"])
        df.to_csv('analysis_result.csv', index=False)
        print("A tabela foi salva no arquivo 'analysis_result.csv'.")
    else:
        print("Nenhuma ação com preço acima do ATR20 foi encontrada.")
else:
    print("Não foi possível carregar a lista de tickers do banco de dados.")
