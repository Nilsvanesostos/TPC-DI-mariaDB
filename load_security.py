import os
import glob

batch_number = 1

"""
Create Security table in the staging database and then load rows by joining staging_security, status_type and dim_company
"""
print('Loading DimSecurity...')
load_dim_security_query_1 = f"""
INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, STR_TO_DATE(SS.FIRST_TRADE_DATE, '%Y-%m-%d'),
      STR_TO_DATE(FIRST_TRADE_EXCHANGE,  '%Y-%m-%d'), SS.DIVIDEN, true, {batch_number}, STR_TO_DATE(LPAD(SS.PTS,8), '%Y-%m-%d'), STR_TO_DATE('99991231', '%Y-%m-%d')
FROM S_Security SS
JOIN StatusType ST ON SS.STATUS = ST.ST_ID
JOIN DimCompany DC ON DC.SK_CompanyID = CAST(SS.COMPANY_NAME_OR_CIK AS INTEGER)
                    AND DC.EffectiveDate <= STR_TO_DATE(LPAD(SS.PTS,8), '%Y-%m-%d')
                    AND STR_TO_DATE(LPAD(SS.PTS,8), '%Y-%m-%d') < DC.EndDate
                    AND LPAD(SS.COMPANY_NAME_OR_CIK,1)='0';\n 
"""
load_dim_security_query_2 = f"""                    
INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, STR_TO_DATE(SS.FIRST_TRADE_DATE,'YYYY-MM-DD'),
      STR_TO_DATE(FIRST_TRADE_EXCHANGE,  '%Y-%m-%d'), SS.DIVIDEN, true, {batch_number}, STR_TO_DATE(LPAD(SS.PTS,8), '%Y-%m-%d'), STR_TO_DATE('9999-12-31', '%Y-%m-%d')
FROM S_Security SS
JOIN StatusType ST ON SS.STATUS = ST.ST_ID
JOIN DimCompany DC ON RTRIM(SS.COMPANY_NAME_OR_CIK) = DC.Name
                    AND DC.EffectiveDate <= STR_TO_DATE(LPAD(SS.PTS,8), '%Y-%m-%d')
                    AND STR_TO_DATE(LPAD(SS.PTS,8), '%Y-%m-%d') < DC.EndDate
                    AND LPAD(SS.COMPANY_NAME_OR_CIK,1) <> '0';\n
"""
create_sdc_dimsecurity_query = """                        
CREATE TABLE sdc_dimsecurity
  (	
  SK_SECURITYID INT NOT NULL, 
  SYMBOL CHAR(15) NOT NULL, 
  ISSUE CHAR(6) NOT NULL, 
  STATUS CHAR(10) NOT NULL, 
  NAME CHAR(70) NOT NULL, 
  EXCHANGEID CHAR(6) NOT NULL, 
  SK_COMPANYID BIGINT NOT NULL, 
  SHARESOUTSTANDING BIGINT NOT NULL, 
  FIRSTTRADE DATE NOT NULL, 
  FIRSTTRADEONEXCHANGE DATE NOT NULL, 
  DIVIDEND DECIMAL(10,2) NOT NULL, 
  ISCURRENT boolean NOT NULL, 
  BATCHID INT NOT NULL, 
  EFFECTIVEDATE DATE NOT NULL, 
  ENDDATE DATE NOT NULL, 
  CHECK (IsCurrent IN (false, true)), 
  PRIMARY KEY (SK_SECURITYID)
  );\n
"""
alter_sdc_dimsecurity_query = """        
ALTER TABLE sdc_dimsecurity
  ADD RN DECIMAL;\n
"""
fill_sdc_dimsecurity_query = """
INSERT INTO sdc_dimsecurity
SELECT DS.*, ROW_NUMBER() OVER(ORDER BY Symbol, EffectiveDate) RN
FROM DimSecurity DS;\n
"""
update_sdc_dimsecurity_query_1 = """
UPDATE DimSecurity
SET DimSecurity.EndDate = 
    (SELECT EndDate FROM (
        SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
        FROM sdc_dimsecurity s1
        JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
        JOIN DimSecurity ON s1.SK_SecurityID = DimSecurity.SK_SecurityID) as SubqueryOne);\n
"""
update_sdc_dimsecurity_query_2 = """
UPDATE DimSecurity
SET DimSecurity.IsCurrent = 
    (SELECT false FROM (
        SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
        FROM sdc_dimsecurity s1
        JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
        JOIN DimSecurity ON s1.SK_SecurityID = DimSecurity.SK_SecurityID) as SubqueryTwo)
    WHERE EXISTS (SELECT * FROM (
        SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
        FROM sdc_dimsecurity s1
        JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
        JOIN DimSecurity ON s1.SK_SecurityID = DimSecurity.SK_SecurityID) as SubqueryThree);\n
"""
drop_sdc_dimsecurity_query = """            
DROP TABLE sdc_dimsecurity;\n
"""

with open('load_security.sql','w') as f:
    f.write(load_dim_security_query_1)
    f.write(load_dim_security_query_2)
    f.write(create_sdc_dimsecurity_query)
    f.write(alter_sdc_dimsecurity_query)
    f.write(fill_sdc_dimsecurity_query)
    f.write(update_sdc_dimsecurity_query_1)
    f.write(update_sdc_dimsecurity_query_2)
    f.write(drop_sdc_dimsecurity_query)

print('Done.')