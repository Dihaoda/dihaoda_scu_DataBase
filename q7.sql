select CompanyName||"|"||CustomerId||"|"||round(exp,2)
from
(select ifnull(CompanyName, "MISSING_NAME") as CompanyName, CustomerId, exp, NTILE(4) over(order by CAST(exp AS float)) as quartile 
     FROM
     (select "Order".CustomerId, sum((UnitPrice*Quantity)) as exp from "Order" left join OrderDetail on "Order".Id = OrderDetail.OrderId group by "Order".CustomerId)
     left join Customer on CustomerId = Customer.Id) 
 where quartile=1
 order by CAST(exp AS float);
