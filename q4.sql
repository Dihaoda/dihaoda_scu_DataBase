select CategoryName||"|"||count(CategoryName)||"|"||round(avg(UnitPrice),2)||"|"||min(UnitPrice)||"|"||max(UnitPrice)||"|"||sum(UnitsOnOrder) 
AS result_q4
from Product left join Category        
where CategoryId=Category.Id  
group by Category.Id
having count(CategoryName) > 10  
order by CategoryId;
