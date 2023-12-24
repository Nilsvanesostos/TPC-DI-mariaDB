import os
import glob

"""
Create Dim Company table in the staging database and then load rows by joining staging_company, staging_industry, and staging StatusType
"""
print('Loading DimCompany table...')
load_dim_company_query = """
INSERT INTO DimCompany (CompanyID, Status,Name,Industry,SPrating,isLowGrade,CEO,AddressLine1,AddressLine2,PostalCode,City,StateProv,Country,Description,FoundingDate,IsCurrent,BatchID,EffectiveDate,EndDate)
SELECT C.CIK, S.ST_NAME, C.COMPANY_NAME, I.IN_NAME,C.SP_RATING, 
    CASE 
        WHEN LPAD(C.SP_RATING,1)='A' OR LPAD(C.SP_RATING,3)='BBB' THEN
            false
        ELSE
            true
        END,
    C.CEO_NAME, C.ADDR_LINE_1,C.ADDR_LINE_2, C.POSTAL_CODE, C.CITY, C.STATE_PROVINCE, C.COUNTRY, C.DESCRIPTION,
    STR_TO_DATE(FOUNDING_DATE,'%Y%m%d'),true, 1, STR_TO_DATE(LPAD(C.PTS,8),'%Y%m%d'), STR_TO_DATE('9999-12-31','%Y-%m-%d')
FROM S_Company C
JOIN Industry I ON C.INDUSTRY_ID = I.IN_ID
JOIN StatusType S ON C.STATUS = S.ST_ID
WHERE FOUNDING_DATE IS NOT NULL;\n
"""
create_sdc_dimcompany_query = """
CREATE TABLE sdc_dimcompany 
  (	SK_COMPANYID INT, 
  COMPANYID BIGINT NOT NULL, 
  STATUS CHAR(10) NOT NULL, 
  NAME CHAR(60) NOT NULL, 
  INDUSTRY CHAR(50) NOT NULL, 
  SPRATING CHAR(4), 
  ISLOWGRADE boolean NOT NULL, 
  CEO CHAR(100) NOT NULL, 
  ADDRESSLINE1 CHAR(80), 
  ADDRESSLINE2 CHAR(80), 
  POSTALCODE CHAR(12) NOT NULL, 
  CITY CHAR(25) NOT NULL, 
  STATEPROV CHAR(20) NOT NULL, 
  COUNTRY CHAR(24), 
  DESCRIPTION CHAR(150) NOT NULL, 
  FOUNDINGDATE DATE, 
  ISCURRENT boolean NOT NULL, 
  BATCHID INT NOT NULL, 
  EFFECTIVEDATE DATE NOT NULL, 
  ENDDATE DATE NOT NULL, 
  CHECK (isLowGrade IN (true, false)), 
  CHECK (IsCurrent IN (false, true)), 
  PRIMARY KEY (SK_COMPANYID));
"""
alter_sdc_dimcompany_query = """
ALTER TABLE sdc_dimcompany
    ADD RN DECIMAL;\n
"""
fill_sdc_dimcompany_query = """
INSERT INTO sdc_dimcompany
SELECT DC.*, ROW_NUMBER() OVER(ORDER BY CompanyID, EffectiveDate) RN
FROM DimCompany DC;\n
"""
update_sdc_dimcompany_query_1 = """
UPDATE DimCompany 
SET DimCompany.EndDate = 
    (SELECT EndDate FROM ( 
        SELECT s1.SK_CompanyID,
                s2.EffectiveDate EndDate
        FROM sdc_dimcompany s1
        JOIN sdc_dimcompany s2 ON (s1.RN = (s2.RN - 1) AND s1.CompanyID = s2.CompanyID)
        JOIN DimCompany ON s1.SK_CompanyID = DimCompany.SK_CompanyID LIMIT 1) as SubqueryOne);\n
"""
update_sdc_dimcompany_query_2 = """
UPDATE DimCompany AS DC
SET DC.IsCurrent = FALSE
WHERE EXISTS (
    SELECT *
    FROM sdc_dimcompany s1
    JOIN sdc_dimcompany s2 ON (s1.RN = (s2.RN - 1) AND s1.CompanyID = s2.CompanyID)
    WHERE s1.SK_CompanyID = DC.SK_CompanyID
);\n
"""
drop_sdc_dimcompany_query = """
DROP TABLE sdc_dimcompany;\n
"""
with open('load_company.sql','w') as f:
    f.write(load_dim_company_query)
    f.write(create_sdc_dimcompany_query)
    f.write(alter_sdc_dimcompany_query)
    f.write(fill_sdc_dimcompany_query)
    f.write(update_sdc_dimcompany_query_1)
    f.write(update_sdc_dimcompany_query_2)
    f.write(drop_sdc_dimcompany_query)

### adding invalid SPRatings to DIMessages
#cmd = TPCDI_Loader.BASE_SQL_CMD+' @%s' % (self.load_path+'/dimessages_company.sql')
#os.system(cmd)
print('Done.')