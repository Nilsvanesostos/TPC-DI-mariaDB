CREATE INDEX IF NOT EXISTS idx_T_ID ON S_Trade (T_ID);
CREATE INDEX IF NOT EXISTS idx_T_ST_ID ON S_Trade (T_ST_ID);
CREATE INDEX IF NOT EXISTS idx_T_TT_ID ON S_Trade (T_TT_ID);
CREATE INDEX IF NOT EXISTS idx_T_S_SYMB ON S_Trade (T_S_SYMB);
CREATE INDEX IF NOT EXISTS idx_T_CA_ID ON S_Trade (T_CA_ID);
CREATE INDEX IF NOT EXISTS idx_TH_T_ID ON S_Trade_History (TH_T_ID);
CREATE INDEX IF NOT EXISTS idx_TH_DTS ON S_Trade_History (TH_DTS);
CREATE INDEX IF NOT EXISTS idx_Symbol_Effective_EndDate ON DimSecurity (Symbol, EffectiveDate, EndDate);
CREATE INDEX IF NOT EXISTS idx_AccountID_Effective_EndDate ON DimAccount (AccountID, EffectiveDate, EndDate);

INSERT INTO DimTrade (SK_CreateDateID, SK_CreateTimeID, SK_CloseDateID,
                    SK_CloseTimeID, TradeID, CashFlag, Quantity, 
                    BidPrice, ExecutedBy, TradePrice, Fee, Commission, Tax,
                    Status, Type, SK_SecurityID, SK_CompanyID,
                    SK_AccountID, SK_CustomerID, SK_BrokerID, BatchID)
SELECT  (   
            CASE WHEN TH_ST_ID = 'SBMT' 
                AND (T_TT_ID = 'TMB' OR T_TT_ID = 'TMS')
                OR TH_ST_ID = 'PDNG' 
            THEN SK_DateID
            ELSE NULL
            END
        ) AS SK_CreateDateID,
        (
            CASE WHEN TH_ST_ID = 'SBMT' 
                AND (T_TT_ID = 'TMB' OR T_TT_ID = 'TMS')
                OR TH_ST_ID = 'PDNG' 
            THEN SK_TimeID
            ELSE NULL
            END
        )AS SK_CreateTimeID,
        (
            CASE WHEN TH_ST_ID = 'CMPT' OR TH_ST_ID = 'CNCL'
            THEN SK_DateID
            ELSE NULL
            END
        ) AS SK_CloseDateID, 
        (
            CASE WHEN TH_ST_ID = 'CMPT' OR TH_ST_ID = 'CNCL'
            THEN SK_TimeID
            ELSE NULL
            END
        ) AS SK_CloseTimeID,
        T_ID TradeID, 
        (
            CASE WHEN T_IS_CASH = 0 THEN 'false'
            ELSE 'true'
            END
        ) AS CashFlag, T_QTY Quantity, 
        T_BID_PRICE BidPrice, T_EXEC_NAME ExecutedBy, 
        T_TRADE_PRICE TradePrice, T_CHRG Fee, T_COMM Commission, T_TAX Tax,
        ST_NAME Status, TT_NAME Type, SK_SecurityID, SK_CompanyID,
        SK_AccountID, SK_CustomerID, SK_BrokerID, 1 BatchID
FROM    S_Trade T   INNER JOIN S_Trade_History TH ON (T.T_ID = TH.TH_T_ID)
                    INNER JOIN StatusType ST ON (T.T_ST_ID = ST.ST_ID)
                    INNER JOIN TradeType TT ON (T.T_TT_ID = TT.TT_ID)
                    INNER JOIN DimSecurity DS ON (T.T_S_SYMB = DS.Symbol)
                    INNER JOIN DimAccount DA ON (T_CA_ID = DA.AccountID)
                    INNER JOIN DimDate DD ON (TO_CHAR(TH_DTS, 'YYYY-MM-DD') = TO_CHAR(DateValue, 'YYYY-MM-DD'))
                    INNER JOIN DimTime DT ON (TO_CHAR(TH_DTS, 'HH24:MI:SS') = TO_CHAR(TimeValue, 'HH24:MI:SS'))
WHERE   DS.EffectiveDate <= TH_DTS AND TH_DTS <= DS.EndDate
        AND DA.EffectiveDate <= TH_DTS AND TH_DTS <= DA.EndDate;


INSERT INTO DImessages 
(MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
SELECT CURRENT_TIMESTAMP, T.BatchID, 'DimTrade', 'Invalid trade commission', 'Alert', CONCAT('T_ID = ', T.TradeID, ', T_COMM = ', T.Commission)
FROM DimTrade T
WHERE T.Commission IS NOT NULL AND T.Commission > T.TradePrice * T.Quantity;



INSERT INTO DImessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
SELECT CURRENT_TIMESTAMP, T.BatchID, 'DimTrade', 'Invalid trade fee', 'Alert', CONCAT('T_ID = ', T.TradeID, ', T_CHRG = ', T.Fee)
FROM DimTrade T
WHERE T.Fee IS NOT NULL AND T.Fee > T.TradePrice * T.Quantity;

