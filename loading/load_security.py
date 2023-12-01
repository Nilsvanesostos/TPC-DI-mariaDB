import os
import glob

"""
Create Security table in the staging database and then load rows by joining staging_security, status_type and dim_company
"""
print('Loading DimSecurity...')
load_dim_security_query_1 = """
INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, TO_DATE(SS.FIRST_TRADE_DATE,'YYYY-MM-DD'),
      TO_DATE(FIRST_TRADE_EXCHANGE, 'YYYY-MM-DD'), SS.DIVIDEN, 'true', 1, TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD'), TO_DATE('99991231','YYYY-MM-DD')
FROM S_Security SS
JOIN StatusType ST ON SS.STATUS = ST.ST_ID
JOIN DimCompany DC ON DC.SK_CompanyID = CAST(SS.COMPANY_NAME_OR_CIK AS INTEGER)
                    AND DC.EffectiveDate <= TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD')
                    AND TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD') < DC.EndDate
                    AND LPAD(SS.COMPANY_NAME_OR_CIK,1)='0'
""" ###aqui se ha puesto batch_number=1
load_dim_security_query_2 = """                    
INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, TO_DATE(SS.FIRST_TRADE_DATE,'YYYY-MM-DD'),
      TO_DATE(FIRST_TRADE_EXCHANGE, 'YYYY-MM-DD'), SS.DIVIDEN, 'true', 1, TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD'), TO_DATE('99991231','YYYY-MM-DD')
FROM S_Security SS
JOIN StatusType ST ON SS.STATUS = ST.ST_ID
JOIN DimCompany DC ON RTRIM(SS.COMPANY_NAME_OR_CIK) = DC.Name
                    AND DC.EffectiveDate <= TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD')
                    AND TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD') < DC.EndDate
                    AND LPAD(SS.COMPANY_NAME_OR_CIK,1) <> '0'
"""###aqui se ha puesto batch_number=1
create_sdc_dimsecurity_query = """                        
CREATE TABLE sdc_dimsecurity
  (	SK_SECURITYID NUMBER(11,0), 
  SYMBOL CHAR(15) NOT NULL, 
  ISSUE CHAR(6) NOT NULL, 
  STATUS CHAR(10) NOT NULL, 
  NAME CHAR(70) NOT NULL, 
  EXCHANGEID CHAR(6) NOT NULL, 
  SK_COMPANYID NUMBER(11,0) NOT NULL, 
  SHARESOUTSTANDING NUMBER(12,0) NOT NULL, 
  FIRSTTRADE DATE NOT NULL, 
  FIRSTTRADEONEXCHANGE DATE NOT NULL, 
  DIVIDEND NUMBER(10,2) NOT NULL, 
  ISCURRENT CHAR(5) NOT NULL, 
  BATCHID NUMBER(5,0) NOT NULL, 
  EFFECTIVEDATE DATE NOT NULL, 
  ENDDATE DATE NOT NULL, 
  CHECK (IsCurrent = 'false' or IsCurrent = 'true') ENABLE, 
  PRIMARY KEY ("SK_SECURITYID"))
"""
alter_sdc_dimsecurity_query = """        
ALTER TABLE sdc_dimsecurity
  ADD RN DECIMAL
"""
fill_sdc_dimsecurity_query = """
INSERT INTO sdc_dimsecurity
SELECT DS.*, ROW_NUMBER() OVER(ORDER BY Symbol, EffectiveDate) RN
FROM DimSecurity DS
"""
update_sdc_dimsecurity_query = """
UPDATE DimSecurity
SET DimSecurity.EndDate = 
    (SELECT EndDate FROM (
        SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
        FROM sdc_dimsecurity s1
        JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
        WHERE s1.SK_SecurityID = DimSecurity.SK_SecurityID)),
    DimSecurity.IsCurrent = 
    (SELECT 'false' FROM (
        SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
        FROM sdc_dimsecurity s1
        JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
        WHERE s1.SK_SecurityID = DimSecurity.SK_SecurityID))
    WHERE EXISTS (SELECT * FROM (
        SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
        FROM sdc_dimsecurity s1
        JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
        WHERE s1.SK_SecurityID = DimSecurity.SK_SecurityID))
"""
drop_sdc_dimsecurity_query = """            
DROP TABLE sdc_dimsecurity
"""

with open('load_security.sql','w') as f:
    f.write(load_dim_security_query_1)
    f.write(load_dim_security_query_2)
    f.write(create_sdc_dimsecurity_query)
    f.write(alter_sdc_dimsecurity_query)
    f.write(fill_sdc_dimsecurity_query)
    f.write(update_sdc_dimsecurity_query)
    f.write(drop_sdc_dimsecurity_query)

print('Done.')