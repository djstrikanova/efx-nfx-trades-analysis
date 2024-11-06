1: Get everyone who voted in the DAO. Get all proposals to check their cycle.

Run get-eos-table.py
table: vote, scope: daoproposals, contract: daoproposals
create a file daoproposals_vote.csv

Run: get-eos-table.py
table: proposal, scope: daoproposals, contract: daoproposals
create a file proposals-cycle.csv

2: Calculate activity of each DAO member who voted in cycles.
Run cycle-vote.py

3: Document how much NFX each active DAO member has
Run get-nfx.py









##########################################
Analyzing EFX/NFX Trades

1. Pull transactions from effecttokens (set fetch_state last_position to 2500000 because EFX/NFX Defibox trading doesn't happen until 2021)
fetch-efx-nfx-transactions.py

2. Analyze the transactions for EFX/NFX trades on Defibox
analyze-efx-nfx-trades.py

3. Create Charts 
create-efx-nfx-charts.py


