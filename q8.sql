select RegionDescription||"|"||FirstName||"|"||LastName||"|"||BirthDate 
as result_q8
from(select * from Employee 
              left join EmployeeTerritory on Employee.Id=EmployeeTerritory.employeeId       
              left join Territory on EmployeeTerritory.TerritoryId=Territory.Id       
              left join Region on Territory.RegionId=Region.Id       
    order by Employee.BirthDate desc) 
group by RegionId  
order by RegionId; 
