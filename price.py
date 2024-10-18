import yfinance as yf

# Prompting user for the share name
STK = input("Enter share name: ")

# Fetching historical market data
data = yf.Ticker(STK).history(period="1d")

#Extracting the Last market price
last_market_price = data['Close'].iloc[-1]

# Displaying the Last market price
print("Last market price:", last_market_price)


