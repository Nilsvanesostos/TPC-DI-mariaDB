LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/HR.csv'
INTO TABLE S_Broker
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
(
EmployeeID ,
ManagerID ,
EmployeeFirstName ,
EmployeeLastName ,
EmployeeMI ,
@var ,
EmployeeBranch ,
EmployeeOffice ,
EmployeePhone
)

SET EmployeeJobCode = NULLIF(@var, '');



INSERT INTO DimBroker (BrokerID,ManagerID,FirstName,LastName,MiddleInitial,Branch,Office,Phone,IsCurrent,BatchID,EffectiveDate,EndDate)
      SELECT SB.EmployeeID, SB.ManagerID, SB.EmployeeFirstName, SB.EmployeeLastName, SB.EmployeeMI, SB.EmployeeBranch, SB.EmployeeOffice, SB.EmployeePhone, TRUE, 1, (SELECT MIN(DateValue) FROM DimDate), STR_TO_DATE('9999/12/31', '%Y/%m/%d')
      FROM S_Broker SB
      WHERE SB.EmployeeJobCode = 314;
