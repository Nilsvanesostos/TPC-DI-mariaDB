INSERT INTO FactMarketHistory (SK_SecurityID, SK_CompanyID, SK_DateID, PERatio, Yield, FiftyTwoWeekHigh,
        SK_FiftyTwoWeekHighDate, FiftyTwoWeekLow, SK_FiftyTwoWeekLowDate, ClosePrice, DayHigh, DayLow, Volume, BatchID)
    WITH MAXCLOSE AS (
        SELECT DM_DATE, DM_S_SYMB, DM_CLOSE, MAX(DM_HIGH) OVER (PARTITION BY DM_S_SYMB ORDER BY DM_DATE ROWS BETWEEN 365 PRECEDING AND CURRENT ROW) AS MAXC
        FROM S_Daily_Market DM),
      MAXDATE AS (
        SELECT DM.DM_S_SYMB AS SYMB, MIN(DM.DM_DATE) AS MAXD, MAX(MC.MAXC) AS MAXC, MC.DM_DATE AS BASEDATE
        FROM S_Daily_Market DM
        JOIN MAXCLOSE MC ON MC.DM_S_SYMB = DM.DM_S_SYMB
        WHERE DM.DM_DATE >= MC.DM_DATE - 365 AND DM.DM_DATE < MC.DM_DATE AND DM.DM_HIGH = MC.MAXC
        GROUP BY DM.DM_S_SYMB, MC.DM_DATE
      ),
      MINCLOSE AS (
        SELECT DM.DM_DATE, DM.DM_S_SYMB, MIN(DM.DM_LOW) OVER (PARTITION BY DM.DM_S_SYMB ORDER BY DM.DM_DATE ROWS BETWEEN 365 PRECEDING AND CURRENT ROW) AS MINC
        FROM S_Daily_Market DM),
      MINDATE AS (
      SELECT DM.DM_S_SYMB AS SYMB, MIN(DM.DM_DATE) AS MIND, MAX(MC.MINC) AS MINC, MC.DM_DATE AS BASEDATE
        FROM S_Daily_Market DM
        JOIN MINCLOSE MC ON MC.DM_S_SYMB = DM.DM_S_SYMB
        WHERE DM.DM_DATE >= MC.DM_DATE - 365 AND DM.DM_DATE < MC.DM_DATE AND DM.DM_LOW = MC.MINC
        GROUP BY DM.DM_S_SYMB, MC.DM_DATE
      ),
      EPS AS (
        SELECT DM.DM_DATE, DM.DM_S_SYMB, SUM(F.FI_BASIC_EPS) OVER (PARTITION BY DM.DM_S_SYMB ORDER BY DM.DM_DATE ROWS BETWEEN 130 PRECEDING AND CURRENT ROW) AS TOTAL_EPS
        FROM S_Daily_Market DM
        JOIN DimSecurity DS ON (DM.DM_S_SYMB = DS.Symbol)
        JOIN Financial F ON (F.SK_CompanyID = DS.SK_CompanyID))
    SELECT  DS.SK_SecurityID,
        DS.SK_CompanyID,
        DD.SK_DateID,
          CASE
            WHEN EPS.TOTAL_EPS IS NULL THEN NULL
            ELSE DM.DM_CLOSE/EPS.TOTAL_EPS
          END,
          DS.DIVIDEND/DM.DM_CLOSE*100,
          MXC.MAXC,
          DMX.SK_DateID,
          MNC.MINC,
          DMN.SK_DateID,
          DM.DM_CLOSE,
          DM.DM_HIGH,
          DM.DM_LOW,
          DM.DM_VOL,
          1
    FROM    S_Daily_Market DM INNER JOIN DimDate DD ON (TO_CHAR(DM.DM_DATE, 'YYYY-MM-DD') = TO_CHAR(DD.DateValue, 'YYYY-MM-DD'))
                              INNER JOIN DimSecurity DS ON (DM.DM_S_SYMB = DS.Symbol)
                              INNER JOIN MAXCLOSE MXC ON (DM.DM_S_SYMB = MXC.DM_S_SYMB AND MXC.DM_DATE = DM.DM_DATE)
                              INNER JOIN MAXDATE MXD ON (MXD.BASEDATE = DM.DM_DATE AND MXD.SYMB = DM.DM_S_SYMB)
                              INNER JOIN DimDate DMX ON (TO_CHAR(MXD.MAXD, 'YYYY-MM-DD') = TO_CHAR(DMX.DateValue, 'YYYY-MM-DD'))
                              INNER JOIN MINCLOSE MNC ON (DM.DM_S_SYMB = MNC.DM_S_SYMB AND MNC.DM_DATE = MNC.DM_DATE)
                              INNER JOIN MINDATE MND ON (MND.BASEDATE = DM.DM_DATE AND MND.SYMB = DM.DM_S_SYMB)
                              INNER JOIN DimDate DMN ON (TO_CHAR(MND.MIND, 'YYYY-MM-DD') = TO_CHAR(DMN.DateValue, 'YYYY-MM-DD'))
                              INNER JOIN EPS ON (DM.DM_S_SYMB = EPS.DM_S_SYMB AND DM.DM_DATE = EPS.DM_DATE)
    WHERE   DS.EffectiveDate <= DM.DM_DATE AND DM.DM_DATE < DS.EndDate;

