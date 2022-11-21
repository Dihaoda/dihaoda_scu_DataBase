select distinct shipName || "|" || substr(shipName,1,instr(shipName,'-')-1) as result_q2 from 'Order' where shipName like "%-%" order by shipName;
