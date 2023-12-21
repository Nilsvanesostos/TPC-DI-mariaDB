import os
import glob

batch_number = 1

"""
Create DimBroker table in the target database and then load rows in HR.csv into it.
"""
print('Loading broker...')
load_dim_broker_query = f"""
  INSERT INTO DimBroker (BrokerID,ManagerID,FirstName,LastName,MiddleInitial,Branch,Office,Phone,IsCurrent,BatchID,EffectiveDate,EndDate)
  SELECT SB.EmployeeID, SB.ManagerID, SB.EmployeeFirstName, SB.EmployeeLastName, SB.EmployeeMI, SB.EmployeeBranch, SB.EmployeeOffice, SB.EmployeePhone, true, {batch_number}, (SELECT MIN(DateValue) FROM DimDate), STR_TO_DATE('9999-12-31', '%Y-%m-%d')
  FROM S_Broker SB
  WHERE SB.EmployeeJobCode = 314
"""

with open("load_broker.sql", "w") as f:
    f.write(load_dim_broker_query)
    
print('Done.')