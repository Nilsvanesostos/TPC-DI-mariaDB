LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/DailyMarket.txt'
INTO TABLE S_Daily_Market
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\r\n'
(
DM_DATE,
DM_S_SYMB,
DM_CLOSE,
DM_HIGH,
DM_LOW,
DM_VOL
)

