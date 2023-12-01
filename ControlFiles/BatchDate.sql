LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/staging/3/Batch1/Date.txt'
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
