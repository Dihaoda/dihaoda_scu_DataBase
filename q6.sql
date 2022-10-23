select Id||"|"||OrderDate||"|"||(lag(OrderDate,1,orderDate) over(order by OrderDate))||"|"||  
round(julianday(OrderDate)-julianday(lag(OrderDate,1,orderDate) over(ORDER BY OrderDate)),2)  
as result_q6     
from 'Order'                                                                                  
where CustomerId="BLONP"                                                                      
order by OrderDate                                                                            
limit 10;
