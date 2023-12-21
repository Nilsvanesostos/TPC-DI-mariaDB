import os
import glob
import json

batch_number = 1

f = open('load_watches.sql','w')


print('Loading FactWatches...')
insert_watches_query = f"""
INSERT INTO FactWatches (SK_CustomerID, SK_SecurityID, SK_DateID_DatePlaced, SK_DateID_DateRemoved, BatchID)
WITH Active AS (
    SELECT W_C_ID, W_S_SYMB, DC.SK_CustomerID, DS.SK_SecurityID, DD.SK_DateID
    FROM S_Watches W
        INNER JOIN DimCustomer DC ON (W.W_C_ID = DC.CUSTOMERID)
        INNER JOIN DimSecurity DS ON (W.W_S_SYMB = DS.Symbol)
        INNER JOIN DimDate DD ON (TO_CHAR(W.W_DTS, 'YYYY-MM-DD') = TO_CHAR(DD.DateValue, 'YYYY-MM-DD')) 
    WHERE W.W_ACTION = 'ACTV'
),
Cancelled AS (
    SELECT W_C_ID, W_S_SYMB, DD.SK_DateID
    FROM S_Watches W 
        INNER JOIN DimDate DD ON (TO_CHAR(W.W_DTS, 'YYYY-MM-DD') = TO_CHAR(DD.DateValue, 'YYYY-MM-DD')) 
    WHERE W.W_ACTION = 'CNCL'
)
SELECT A.SK_CustomerID, A.SK_SecurityID, A.SK_DateID, C.SK_DateID, {batch_number} BatchID
FROM Active A LEFT OUTER JOIN Cancelled C ON (A.W_C_ID = C.W_C_ID AND A.W_S_SYMB = C.W_S_SYMB)
"""
f.write(insert_watches_query)

print('Done.')