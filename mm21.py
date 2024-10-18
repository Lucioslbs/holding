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
        sample_tickers = ['BBDC4', 'HAPV3', 'PETR4', 'COGN3', 'ABEV3', 'ITSA4', 'B3SA3', 'BBDC3', 'ITUB4', 'CIEL3',
    'MGLU3', 'AMER3', 'IFCM3', 'PETR3', 'LWSA3', 'MRVE3', 'RAIL3', 'CPLE6', 'CMIG4', 'CVCB3',
    'VALE3', 'BBAS3', 'RAIZ4', 'CMIN3', 'LREN3', 'CCRO3', 'AZUL4', 'SOMA3', 'SUZB3', 'EQTL3',
    'HBSA3', 'CSAN3', 'PRIO3', 'USIM5', 'AZEV4', 'GGBR4', 'JBSS3', 'BRFS3', 'ASAI3', 'ELET3',
    'EMBR3', 'VAMO3', 'BEEF3', 'GOLL4', 'WEGE3', 'GMAT3', 'VBBR3', 'NTCO3', 'OIBR3', 'SBSP3',
    'GOAU4', 'AURE3', 'KLBN4', 'RADL3', 'PETZ3', 'AESB3', 'RDOR3', 'UGPA3', 'PCAR3', 'ENEV3',
    'RENT3', 'STBP3', 'TIMS3', 'TRPL4', 'MRFG3', 'CPLE3', 'JHSF3', 'CRFB3', 'YDUQ3', 'POMO4',
    'CLSA3', 'ALOS3', 'CSNA3', 'ECOR3', 'SIMH3', 'MOVI3', 'TOTS3', 'BBSE3', 'CYRE3', 'VIVA3',
    'QUAL3', 'VIVT3', 'DXCO3', 'LJQQ3', 'BHIA3', 'ANIM3', 'RRRP3', 'DIRR3', 'BRAP4', 'SLCE3',
    'GFSA3', 'ONCO3', 'SMFT3', 'ALPA4', 'MLAS3', 'CBAV3', 'MULT3', 'FLRY3', 'EZTC3', 'BRKM5',
    'CEAB3', 'AMBP3', 'LIGT3', 'HYPE3', 'ELET6', 'SAPR4', 'RECV3', 'VVEO3', 'CPFE3', 'SMTO3',
    'SBFG3', 'ARZZ3', 'RAPT4', 'SRNA3', 'CURY3', 'TEND3', 'PDGR3', 'ENJU3', 'KRSA3', 'CXSE3',
    'DASA3', 'CSMG3', 'BRSR6', 'CSED3', 'GUAR3', 'MILS3', 'NEOE3', 'EGIE3', 'NGRD3', 'BPAN4',
    'ODPV3', 'MYPK3', 'ENAT3', 'MTRE3', 'PSSA3', 'SEER3', 'POSI3', 'GRND3', 'ZAMP3', 'KEPL3',
    'IRBR3', 'KLBN3', 'FESA4', 'GGPS3', 'SEQL3', 'INTB3', 'ITUB3', 'SOJA3', 'TTEN3', 'PLPL3',
    'EVEN3', 'TUPY3', 'MBLY3', 'ABCB4', 'BMGB4', 'ARML3', 'MDIA3', 'ESPA3', 'TRIS3', 'PGMN3',
    'VULC3', 'JSLG3', 'RANI3', 'OPCT3', 'RCSL3', 'PTBL3', 'CAML3', 'SHUL4', 'AZEV3', 'TRAD3',
    'RCSL4', 'BRIT3', 'CASH3', 'HBRE3', 'TASA4', 'MELK3', 'MEAL3', 'JALL3', 'PORT3', 'ORVR3',
    'BMOB3', 'TAEE4', 'VLID3', 'LEVE3', 'MATD3', 'HBOR3', 'RPMG3', 'SYNE3', 'PRNR3', 'VITT3',
    'AERI3', 'TECN3', 'USIM3', 'MDNE3', 'WIZC3', 'LAVV3', 'PNVL3', 'BIOM3', 'CMIG3', 'TAEE3',
    'ALPK3', 'SGPS3', 'AGRO3', 'FRAS3', 'ETER3', 'BLAU3', 'AMAR3', 'SAPR3', 'TGMA3', 'POMO3',
    'ROMI3', 'PFRM3', 'ITSA3', 'LOGG3', 'PINE4', 'UCAS3', 'DESK3', 'UNIP6', 'FIQE3', 'LUPA3',
    'ALLD3', 'TFCO4', 'EPAR3', 'NINJ3', 'AGXY3', 'LPSB3', 'BRAP3', 'DMVF3', 'SANB4', 'RNEW4',
    'RSID3', 'EUCA4', 'SHOW3', 'CTSA4', 'SANB3', 'CSUD3', 'GGBR3', 'INEP3', 'VIVR3', 'GOAU3',
    'ELMD3', 'INEP4', 'PMAM3', 'CAMB3', 'LAND3', 'DEXP3', 'RNEW3', 'AALR3', 'PDTC3', 'LOGN3',
    'BMEB4', 'TCSA3', 'RAPT3', 'WEST3', 'JFEN3', 'IGTI3', 'IGTI3', 'BRKM3', 'ALUP4', 'OIBR4',
    'ALUP3', 'BEES3', 'TASA3', 'BOBR4', 'ENGI4', 'EQPA3', 'ATOM3', 'NUTR3', 'TPIS3', 'BAZA3',
    'UNIP3', 'BRSR3', 'EALT4', 'RDNI3', 'LVTC3', 'DOTZ3', 'HAGA4', 'VSTE3', 'CTNM4', 'CEBR6',
    'ALPA3', 'NEXP3', 'OFSA3', 'COCE5', 'ENGI3', 'HAGA3', 'PTNT4', 'FHER3', 'CRPG5', 'REDE3',
    'BAHI3', 'BEES4', 'BPAC5', 'HOOT4', 'SCAR3', 'ATMP3', 'CEBR3', 'CGRA4', 'CTSA3', 'SNSY3',
    'MTSA4', 'MNPR3', 'PTNT3', 'DOHL4', 'CGRA3', 'DEXP4', 'WHRL3', 'AZEV11', 'CLSC4', 'BPAC3',
    'BAUH4', 'EMAE4', 'CSRN3', 'RSUL4', 'CEBR5', 'CGAS5', 'TRPL3', 'PINE3', 'FRIO3', 'OSXB3',
    'FESA3', 'DOHL3', 'IGTI4', 'IGTI4', 'ENMT4', 'TELB4', 'PLAS3', 'WHRL4', 'BALM4', 'GEPA4',
    'CGAS3', 'CLSC3', 'EKTR4', 'CRPG6', 'BMEB3', 'MGEL4', 'GOLL11', 'BNBR3', 'UNIP5', 'APER3',
    'CEEB3', 'WLMM4', 'TEKA4', 'CEDO4', 'CTKA4', 'BSLI4', 'MAPT3', 'AVLL3', 'BMKS3', 'MWET4',
    'FIEI3', 'PINE11', 'MAPT4', 'RPAD5', 'MNDL3', 'BGIP4', 'TELB3', 'RPAD6', 'SNSY5', 'EALT3',
    'MRSA3B', 'EUCA3', 'PSVM11', 'HETA4', 'BGIP3', 'WLMM3', 'BSLI3', 'RPAD3', 'CEDO3', 'LUXM4',
    'HBTS5', 'GEPA3', 'EQMA3B', 'CSRN6', 'BMIN4', 'GSHP3', 'CTNM3', 'NORD3', 'PEAB3', 'PEAB4',
    'COCE3', 'JOPA3', 'CEEB5', 'BRKM6', 'ESTR4', 'PATI3', 'CEED3', 'BDLL4', 'AHEB3', 'BRSR5',
    'DASA11', 'MRSA5B', 'MRSA6B', 'CSRN5', 'MWET3', 'AFLT3']
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

def filter_stocks_above_sma21(tickers):
    result = []
    for ticker in tickers:
        try:
            # Obter dados históricos de preços
            data = yf.download(ticker, period='1mo', interval='1d')

            if data.empty:
                print(f"Sem dados para {ticker}")
                continue

            if len(data) >= 21:  # Certificar-se de ter pelo menos 21 dias de dados
                # Calcular a SMA de 21 períodos
                data['SMA21'] = data['Close'].rolling(window=21).mean()

                # Verificar se o preço de fechamento atual está acima da SMA21
                if data['Close'].iloc[-1] > data['SMA21'].iloc[-1]:
                    result.append(ticker)
        except Exception as e:
            print(f"Erro ao processar ticker {ticker}: {e}")

    return result

# Inicializar o banco de dados
initialize_database()

# Carregar tickers do banco de dados
tickers = get_tickers_from_database()

# Filtrar ações com preço acima da SMA21
if tickers:
    filtered_stocks = filter_stocks_above_sma21(tickers)
    if filtered_stocks:
        df = pd.DataFrame(filtered_stocks, columns=["Ações com preço acima da SMA21"])
        df.to_csv('analysis_result.csv', index=False)
        print("A tabela foi salva no arquivo 'analysis_result.csv'.")
    else:
        print("Nenhuma ação com preço acima da SMA21 foi encontrada.")
else:
    print("Não foi possível carregar a lista de tickers do banco de dados.")
