select CompanyName||"|"||printf("%.2f",count(case when ShippedDate > RequiredDate then 1 else null end)/round(count('Order'.Id))*100)  
AS result_q3 
from "Order" left join Shipper        
where "Order".shipVia = Shipper.Id  
group by Shipper.Id  
order by (count(case when ShippedDate > RequiredDate then 1 else null end)/round(count('Order'.Id))*100) desc;
