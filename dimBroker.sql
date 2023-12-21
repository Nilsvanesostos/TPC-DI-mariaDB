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

