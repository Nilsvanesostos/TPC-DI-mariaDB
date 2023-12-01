LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/staging/3/Batch1/Time.txt'
INTO TABLE DimTime
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
    SK_TimeID ,
    TimeValue ,
    HourID ,
    HourDesc ,
    MinuteID ,
    MinuteDesc ,
    SecondID ,
    SecondDesc ,
    MarketHoursFlag ,
    OfficeHoursFlag
);


--THIS ONE WORKS