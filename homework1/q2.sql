select Id||"|"||shipCountry||"|"||(case when shipCountry in ('USA', 'Mexico','Canada') then "NorthAmerica" else "OtherPlace" end)
as result_q3
from "Order"  
where Id>=15445
limit 20;
