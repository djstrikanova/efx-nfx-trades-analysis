pip install requests pandas openpyxl plotly numpy


##########################################
Analyzing EFX/NFX Trades

1. Pull transactions from effecttokens (set fetch_state last_position to 2500000 because EFX/NFX Defibox trading doesn't happen until 2021)
fetch-efx-nfx-transactions.py

2. Analyze the transactions for EFX/NFX trades on Defibox
analyze-efx-nfx-trades.py

3. Create Charts 
create-efx-nfx-charts.py


