    LOAD DATA INFILE 'C:/ULB/MA2/tpc-di/staging/7/Batch1/Time.txt'
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
        @MarketHoursFlag ,
        @OfficeHoursFlag
    )
    SET MarketHoursFlag = IF(@MarketHoursFlag = 'true', 1, 0),
        OfficeHoursFlag = IF(@OfficeHoursFlag = 'true', 1, 0);


