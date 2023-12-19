LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/loading/staging/3/Batch1/BatchDate.txt'
INTO TABLE BatchDate
FIELDS TERMINATED BY '|'
LINES STARTING BY ''
TERMINATED BY '\r\n'
(
BatchDate 
);



LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/Date.txt'
INTO TABLE DimDate
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
SK_DateID ,
DateValue ,
DateDesc ,
CalendarYearID ,
CalendarYearDesc ,
CalendarQtrID ,
CalendarQtrDesc ,
CalendarMonthID ,
CalendarMonthDesc ,
CalendarWeekID ,
CalendarWeekDesc ,
DayOfWeeknumeric ,
DayOfWeekDesc ,
FiscalYearID ,
FiscalYearDesc ,
FiscalQtrID ,
FiscalQtrDesc ,
HolidayFlag
);

