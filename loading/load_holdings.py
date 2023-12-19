import os
import glob
import json


f = open('load_holdings.sql','w')

print('Loading FactHoldings...')
insert_holdings_query = """
INSERT INTO FactHoldings(TradeId, CurrentTradeID, SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID, SK_DateID, SK_TimeID, CurrentPrice, CurrentHolding, BatchID)
SELECT HH_H_T_ID AS TradeId, HH_T_ID AS CurrentTradeID, DT.SK_CustomerID, DT.SK_AccountID, DT.SK_SecurityID, DT.SK_CompanyID, DT.SK_CloseDateID AS SK_DateID, DT.SK_CloseTimeID AS SK_TimeID, DT.TradePrice AS CurrentPrice, HH_AFTER_QTY AS CurrentHolding, 1 AS BatchID
FROM S_Holdings H
INNER JOIN DimTrade DT ON (H.HH_T_ID = DT.TradeID)
WHERE DT.SK_CloseDateID IS NOT NULL AND DT.SK_CloseTimeID IS NOT NULL
"""
f.write(insert_holdings_query)
print('Done.')