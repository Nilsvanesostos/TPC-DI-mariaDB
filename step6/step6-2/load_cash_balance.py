import os
import glob
import json

batch_number = 1

f = open('load_cash_balances.sql','w')


print('Loading FactCashBalances...')
insert_query = f"""
CREATE INDEX IF NOT EXISTS idx_CT_CA_ID ON S_Cash_Balances (CT_CA_ID);
CREATE INDEX IF NOT EXISTS idx_AccountID ON DimAccount (AccountID);
CREATE INDEX IF NOT EXISTS idx_DateValue ON DimDate (DateValue);

INSERT INTO FactCashBalances (SK_CustomerID, SK_AccountID, SK_DateID, Cash, BatchID)
SELECT SK_CustomerID, SK_AccountID, SK_DateID, SUM(CT_AMT) OVER (PARTITION BY SK_AccountID ORDER BY DateValue) Cash, {batch_number} AS BatchID
    FROM S_Cash_Balances CB INNER JOIN DimAccount DA ON (CB.CT_CA_ID = DA.AccountID)
                            INNER JOIN DimDate DD ON (TO_CHAR(CB.CT_DTS, 'YYYY-MM-DD') = TO_CHAR(DateValue, 'YYYY-MM-DD'))
    WHERE DA.EffectiveDate <= CB.CT_DTS AND CB.CT_DTS <= DA.EndDate
"""

f.write(insert_query)

print('Done.')