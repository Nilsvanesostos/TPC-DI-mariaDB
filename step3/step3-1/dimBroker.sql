LOAD DATA INFILE 'C:/ULB/MA2/tpc-di/staging/7/Batch1/HR.csv'
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

