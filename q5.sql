select ProductName||"|"||CompanyName||"|"||ContactName             
as result_q5
from (select * from "Order"                                        
                left join OrderDetail on "Order".Id=OrderId         
                left join Customer on CustomerId=Customer.Id        
                left join Product on ProductId=Product.Id         
                where Discontinued = 1                        
                order by orderDate)                           
group by ProductId                                           
order by ProductName;           
