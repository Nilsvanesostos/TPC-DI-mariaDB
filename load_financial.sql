
INSERT INTO Financial
  SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, STR_TO_DATE(SF.QTR_START_DATE, 'YYYY-MM-DD'), SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
  FROM S_Financial SF
  JOIN DimCompany DC ON DC.SK_CompanyID = cast(SF.CO_NAME_OR_CIK as INT)
                      AND DC.EffectiveDate <= STR_TO_DATE(SUBSTR(SF.PTS, 1,8),'YYYY-MM-DD')
                      AND STR_TO_DATE(SUBSTR(SF.PTS, 1,8),'YYYY-MM-DD') < DC.EndDate
                      AND SUBSTR(CO_NAME_OR_CIK, 1,1)='0';

INSERT INTO Financial
  SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, STR_TO_DATE(SF.QTR_START_DATE, 'YYYY-MM-DD'), SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
  FROM S_Financial SF
  JOIN DimCompany DC ON RTRIM(SF.CO_NAME_OR_CIK) = DC.Name
                      AND DC.EffectiveDate <= STR_TO_DATE(SUBSTR(SF.PTS, 1,8),'YYYY-MM-DD')
                      AND STR_TO_DATE(SUBSTR(SF.PTS, 1,8),'YYYY-MM-DD') < DC.EndDate
                      AND SUBSTR(CO_NAME_OR_CIK, 1,1) <> '0';