with a1 as
(
SELECT 
sum(GAS_PROD_NSJ) cl
FROM dwd.DWD_PO_GAS_PROD_DAILY_DETAIL
where  SCRQ=toDate('${stop_date}')
)
select
	a1.cl/1255 wcz,
	'天然气当量'lx
from a1
union all
SELECT 
sum(ifnull(OIL_LIQUID_RSJ,0))/10000 as wcz,
'石油液体当量'lx
FROM dwd.DWD_PO_GAS_PROD_DAILY_DETAIL
where SCRQ>=toDate('${start_date}')
and SCRQ<=toDate('${stop_date}')
;

/*
* 基本逻辑：选择时间内的油气当量
* 如果开始时间为1月1号，可以直接取结束时间的当天的年产量
*/

SELECT 
SUM(GAS_PROD_RSJ),
SUM(OIL_LIQUID_RSJ)
FROM dwd.DWD_PO_GAS_PROD_DAILY_DETAIL
where  SCRQ>=toDate('${stop_date}') AND SCRQ <= today()
AND SCRQ<=toDate('${start_date}') AND SCRQ >= date(year(today()),1,1)
;
with t_1 as
(
SELECT 
GAS_PROD_NSJ G_NSJ,
OIL_LIQUID_NSJ O_NSJ
FROM dwd.DWD_PO_GAS_PROD_DAILY_DETAIL
where  SCRQ=toDate('${stop_date}')
),
t_2 as
(
SELECT 
coalesce(SUM(GAS_PROD_RSJ),0) G_RSJ,
coalesce(SUM(OIL_LIQUID_RSJ),0) O_RSJ
FROM dwd.DWD_PO_GAS_PROD_DAILY_DETAIL
where  (SCRQ>=toDate('${stop_date}') AND SCRQ < today())
OR SCRQ<toDate('${start_date}') 
AND year(SCRQ) = year(toDate('${start_date}'))
)
SELECT (t_1.G_NSJ - t_2.G_RSJ)/1255 wcz,'天然气当量'lx
FROM t_1
LEFT JOIN t_2 ON 1 = 1
union all
SELECT (t_1.O_NSJ - t_2.O_RSJ)/10000 wcz,'石油液体当量'lx
FROM t_1
LEFT JOIN t_2 ON 1 = 1