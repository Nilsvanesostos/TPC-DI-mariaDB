
INSERT INTO FactCashBalances (SK_CustomerID, SK_AccountID, SK_DateID, Cash, BatchID)
SELECT SK_CustomerID, SK_AccountID, SK_DateID, SUM(CT_AMT) OVER (PARTITION BY SK_AccountID ORDER BY DateValue) Cash, 1 BatchID
    FROM S_Cash_Balances CB INNER JOIN DimAccount DA ON (CB.CT_CA_ID = DA.AccountID)
                            INNER JOIN DimDate DD ON (TO_CHAR(CB.CT_DTS, 'YYYY-MM-DD') = TO_CHAR(DateValue, 'YYYY-MM-DD'))
    WHERE DA.EffectiveDate <= CB.CT_DTS AND CB.CT_DTS <= DA.EndDate
