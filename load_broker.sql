
  INSERT INTO DimBroker (BrokerID,ManagerID,FirstName,LastName,MiddleInitial,Branch,Office,Phone,IsCurrent,BatchID,EffectiveDate,EndDate)
  SELECT SB.EmployeeID, SB.ManagerID, SB.EmployeeFirstName, SB.EmployeeLastName, SB.EmployeeMI, SB.EmployeeBranch, SB.EmployeeOffice, SB.EmployeePhone, true, 1, (SELECT MIN(DateValue) FROM DimDate), STR_TO_DATE('9999-12-31', '%Y-%m-%d')
  FROM S_Broker SB
  WHERE SB.EmployeeJobCode = 314
