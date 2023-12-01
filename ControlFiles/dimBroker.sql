LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/staging/3/Batch1/HR.csv'
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


--THIS ONE WORKS 

INSERT INTO DimBroker (BrokerID,ManagerID,FirstName,LastName,MiddleInitial,Branch,Office,Phone,IsCurrent,BatchID,EffectiveDate,EndDate)
      SELECT SB.EmployeeID, SB.ManagerID, SB.EmployeeFirstName, SB.EmployeeLastName, SB.EmployeeMI, SB.EmployeeBranch, SB.EmployeeOffice, SB.EmployeePhone, 'true', %d, (SELECT MIN(DateValue) FROM DimDate), TO_DATE('9999/12/31', 'yyyy/mm/dd')
      FROM S_Broker SB
      WHERE SB.EmployeeJobCode = 314;

--I still have to try this