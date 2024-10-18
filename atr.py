import yfinance as yf
import pandas as pd

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

# Carregar tickers do arquivo CSV
tickers = get_tickers_from_csv()

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
    print("Não foi possível carregar a lista de tickers do arquivo CSV.")
