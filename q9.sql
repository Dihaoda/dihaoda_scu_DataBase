select GROUP_CONCAT(ProductName,", ") as result_q9
from "Order"
     left join OrderDetail on "Order".Id = OrderDetail.OrderId
     left join Product on OrderDetail.ProductId = Product.Id
     left join Customer on "Order".CustomerId = Customer.Id
where Customer.CompanyName = 'Queen Cozinha' and "Order".OrderDate like "2014-12-25 __:__:__";
