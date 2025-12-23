--月产量执行情况
select A.PROD_YEAR,
       A.PROD_MON,
       SUM(A."实际产气量") AS "月度产气量",
       SUM(A."月度运行计划") AS "月度运行计划",
       SUM(A."年度分月计划") AS "年度分月计划",
       IF("月度运行计划" = 0 , 0 ,"月度产气量" / "月度运行计划") AS "月度运行计划完成率",
       IF("年度分月计划" = 0 , 0 ,"月度产气量" / "年度分月计划") AS "年度分月计划完成率"
from(
        select  year(toDate(d.PROD_DATE)) AS PROD_YEAR,
                month(toDate(d.PROD_DATE)) AS PROD_MON,
                year(toDate(d.PROD_DATE_STR)) AS TC_YEAR,
                month(toDate(d.PROD_DATE_STR)) AS TC_MONTH,
                d.WELL_ID,
                sum(coalesce(toFloat64(d.PROD_TIME),toFloat64(0))) AS "生产时间",
			    sum(coalesce(toFloat64(d.GAS_PROD_DAILY),toFloat64(0))) AS "产气量",
			    sum(coalesce(toFloat64(d.GAS_RELEASE_DAILY),toFloat64(0))) AS "放气量",
                sum(coalesce(toFloat64(d.WATER_PROD_DAILY),toFloat64(0))) AS "产水量",
                sum(coalesce(toFloat64(d.OIL_PROD_DAILY),toFloat64(0))) AS "产油量",
                max(coalesce(toFloat64(d.GAS_PROD_INCR_MON),toFloat64(0))) / 10000 AS "月度增产气量",
                max(coalesce(toFloat64(d.PRODUCTIVITY_CALIBRATE),toFloat64(d.PLAN_SUIT_PRODUCTIVITY)))  AS "新建产能",
                max(ifnull(d.GAS_PROD_CUM,0)) "累计产气量",
                max(ifnull(d.OIL_PROD_CUM,0)) "累计产油量",
                ("产气量" - "放气量") /10000 AS "实际产气量",
                sum(coalesce(toFloat64(d.DAY_VOL),toFloat64(0))) AS "月度运行计划",
                avg(coalesce(toFloat64(d.SUM_ALLOC_VOL_MONTH),toFloat64(0))) AS "年度分月计划",
                IF(TC_YEAR = ${year},1,0) AS "是否当年新井",
                IF(TC_YEAR = ${year} and TC_MONTH = PROD_MON,1,0) AS "是否当月新井",
                IF(TC_YEAR = ${year} and TC_MONTH = PROD_MON,"新建产能",0) AS "当月新井新建产能",
                IF(TC_YEAR = ${year} - 1,1,0) AS "是否上年新井",
                IF(TC_YEAR <= ${year} - 2,1,0) AS "是否老井",
                IF((PROD_YEAR + PROD_MON) >= (TC_YEAR+TC_MONTH) ,1,0) AS "是否投产井",
                IF("实际产气量" > 0 or "生产时间" > 0 or "产水量" > 0 or "产油量" > 0,1,0) AS "是否开井"
		        from dwd.DWD_WELL_PROD_DAILY_DETAIL_D_CZBCQC d
		        where 1 = 1
                and PROD_YEAR = '${year}'
                group by d.WELL_ID,PROD_YEAR,PROD_MON,TC_YEAR,TC_MONTH
) A
WHERE 1 = 1
GROUP BY A.PROD_YEAR,A.PROD_MON
ORDER BY A.PROD_YEAR,A.PROD_MON
;

--商品率
SELECT psrii.*,psrii.ROWID FROM ODS_JCZH.PC_SPL_REPORT_INDEX_ITEM psrii
WHERE PROD_DATE = TIMESTAMP '2025-11-16 00:00:00.000000' AND ((REPORT_NAME = '川中北部采气管理处商品率及输差跟踪表'))


SELECT A.ORG_NAME,
 	   TO_CHAR(A.PROD_DATE,'MM') MON ,
 	   sum(GAS_PROD_IND) IND,
 	   sum(GAS_PROD_AMT) AMT,
 	   sum(GAS_PROD_AMT) / sum(GAS_PROD_IND) RATIO
FROM(
	SELECT *
	FROM (
		SELECT s.ORG_NAME
		       ,p.PROD_DATE 
			   ,p.INDEX_NAME
			   ,p.ITEM_VALUE
		FROM ODS_JCZH.PC_SPL_REPORT_INDEX_ITEM p
		     ,ODS_JCZH.PC_SPL_REPORT s
		WHERE 1 = 1
		AND p.REPORT_NAME = s.REPORT_NAME 
		AND s.ORG_NAME = '川中北部采气管理处'
		AND TO_CHAR(p.PROD_DATE,'YYYY') = ${year} 
		AND p.INDEX_NAME in ('工业产量','商品量','商品率%','硫化氢酸气','井口产量')
	)
	PIVOT (
		MAX(ITEM_VALUE) 
		FOR(INDEX_NAME) 
		IN ('工业产量' AS GAS_PROD_IND ,
		    '商品量' AS GAS_PROD_AMT, 
		    '商品率%' AS GAS_PROD_AMT_RAT,
		    '硫化氢酸气' AS GAS_ACID,
		    '井口产量' AS GAS_PROD)
	) 
	ORDER BY PROD_DATE 
)A
WHERE 1 = 1
GROUP BY A.ORG_NAME,
 	   TO_CHAR(A.PROD_DATE,'MM')
ORDER BY TO_CHAR(A.PROD_DATE,'MM')


--产能建设

SELECT a.WELL_COMMON_NAME
        ,a.WELL_ID 
        ,a.PROD_DATE_STR
        ,case a.DOMAIN_NAME 
		 when '蓬莱' then '蓬莱灯二'
		 when '灯二' then '台缘灯二'
		 when '台缘' then '台缘灯四'
		 else DOMAIN_NAME end DOMAIN_NAME
        ,year(toDate(a.PROD_DATE_STR)) AS PROD_YEAR
        ,if(toDate(a.PROD_DATE_STR) <= today(),1,0) AS IS_PRODUCE
        ,1 AS IS_TOTAL
        ,coalesce(a.PLAN_SUIT_PRODUCTIVITY,0) *330 AS PLAN_SUIT_PRODUCTIVITY
        ,if(coalesce(a.PRODUCTIVITY_CALIBRATE,0) > 0 ,coalesce(a.PRODUCTIVITY_CALIBRATE,0),coalesce(a.PLAN_SUIT_PRODUCTIVITY,0)) *330 AS PRODUCTIVITY_CALIBRATE
FROM dim.DIM_CD_WELL_CZBCQC a
where 1 = 1
and PROD_YEAR = ${year};


--产能到位率

select a.N_DOMAIN_NAME,
        a.PROD_YEAR,
        sum(a."实际产气量")  AS "年度产气量",
        sum(a."新建产能") * 330 / 10000 AS "新建产能"
from(
        select  year(toDate(d.PROD_DATE)) AS PROD_YEAR,
                month(toDate(d.PROD_DATE)) AS PROD_MON,
                year(toDate(d.PROD_DATE_STR)) AS TC_YEAR,
                month(toDate(d.PROD_DATE_STR)) AS TC_MONTH,
                case d.DOMAIN_NAME 
		 when '蓬莱' then '蓬莱灯二'
		 when '灯二' then '台缘灯二'
		 when '台缘' then '台缘灯四'
		 else DOMAIN_NAME end N_DOMAIN_NAME,
                d.WELL_ID,
                d.WELL_COMMON_NAME,
                sum(coalesce(toFloat64(d.PROD_TIME),toFloat64(0))) AS "生产时间",
			    sum(coalesce(toFloat64(d.GAS_PROD_DAILY),toFloat64(0))) AS "产气量",
			    sum(coalesce(toFloat64(d.GAS_RELEASE_DAILY),toFloat64(0))) AS "放气量",
                sum(coalesce(toFloat64(d.WATER_PROD_DAILY),toFloat64(0))) AS "产水量",
                sum(coalesce(toFloat64(d.OIL_PROD_DAILY),toFloat64(0))) AS "产油量",
                max(coalesce(toFloat64(d.GAS_PROD_INCR_MON),toFloat64(0))) / 10000 AS "月度增产气量",
                max(coalesce(toFloat64(d.PRODUCTIVITY_CALIBRATE),toFloat64(d.PLAN_SUIT_PRODUCTIVITY)))  AS "新建产能",
                max(ifnull(d.GAS_PROD_CUM,0)) "累计产气量",
                max(ifnull(d.OIL_PROD_CUM,0)) "累计产油量",
                ("产气量" - "放气量") /10000 AS "实际产气量",
                sum(coalesce(toFloat64(d.DAY_VOL),toFloat64(0))) AS "月度运行计划",
                avg(coalesce(toFloat64(d.SUM_ALLOC_VOL_MONTH),toFloat64(0))) AS "年度分月计划",
                IF(TC_YEAR = ${year},1,0) AS "是否当年新井",
                IF(TC_YEAR = ${year} and TC_MONTH = PROD_MON,1,0) AS "是否当月新井",
                IF(TC_YEAR = ${year} and TC_MONTH = PROD_MON,"新建产能",0) AS "当月新井新建产能",
                IF(TC_YEAR = ${year} - 1,1,0) AS "是否上年新井",
                IF(TC_YEAR <= ${year} - 2,1,0) AS "是否老井",
                IF((PROD_YEAR + PROD_MON) >= (TC_YEAR+TC_MONTH) ,1,0) AS "是否投产井",
                IF("实际产气量" > 0 or "生产时间" > 0 or "产水量" > 0 or "产油量" > 0,1,0) AS "是否开井"
		        from dwd.DWD_WELL_PROD_DAILY_DETAIL_D_CZBCQC d
		        where 1 = 1
                and PROD_YEAR = '${year}'
                and TC_YEAR + 1 = ${year}
                group by d.WELL_ID,PROD_YEAR,PROD_MON,TC_YEAR,TC_MONTH,d.WELL_COMMON_NAME,N_DOMAIN_NAME
)a
where 1 = 1
group by a.N_DOMAIN_NAME,a.PROD_YEAR


--综合递减率
WITH PROD_YEAR AS(
SELECT A.PROD_YEAR,
        sum(A."实际产气量")  AS "年度产气量",
        sum(A."月度增产气量")  AS "年度增产气量",
        sum(A."当年新井实际产气量" )  AS "当年新井年度实际产气量",
        sum(A."当年新井月度增产气量" )  AS "当年新井年度增产气量",
        sum(A."上年新井实际产气量" )  AS "上年新井年度实际产气量",
        sum(A."上年新井月度增产气量" )  AS "上年新井年度增产气量",
        sum(A."老井实际产气量" )  AS "老井年度产气量",
        sum(A."老井月度增产气量" )  AS "老井年度增产气量"
FROM(
        select  year(toDate(d.PROD_DATE)) AS PROD_YEAR,
                month(toDate(d.PROD_DATE)) AS PROD_MON,
                year(toDate(d.PROD_DATE_STR)) AS TC_YEAR,
                month(toDate(d.PROD_DATE_STR)) AS TC_MONTH,
                case d.DOMAIN_NAME 
		 when '蓬莱' then '蓬莱灯二'
		 when '灯二' then '台缘灯二'
		 when '台缘' then '台缘灯四'
		 else DOMAIN_NAME end N_DOMAIN_NAME,
                d.WELL_ID,
                d.WELL_COMMON_NAME,
                sum(coalesce(toFloat64(d.PROD_TIME),toFloat64(0))) AS "生产时间",
			    sum(coalesce(toFloat64(d.GAS_PROD_DAILY),toFloat64(0))) AS "产气量",
			    sum(coalesce(toFloat64(d.GAS_RELEASE_DAILY),toFloat64(0))) AS "放气量",
                sum(coalesce(toFloat64(d.WATER_PROD_DAILY),toFloat64(0))) AS "产水量",
                sum(coalesce(toFloat64(d.OIL_PROD_DAILY),toFloat64(0))) AS "产油量",
                max(coalesce(toFloat64(d.GAS_PROD_INCR_MON),toFloat64(0)))  AS "月度增产气量",
                max(coalesce(toFloat64(d.PRODUCTIVITY_CALIBRATE),toFloat64(d.PLAN_SUIT_PRODUCTIVITY)))  AS "新建产能",
                max(ifnull(d.GAS_PROD_CUM,0)) "累计产气量",
                max(ifnull(d.OIL_PROD_CUM,0)) "累计产油量",
                ("产气量" - "放气量") /10000 AS "实际产气量",
                sum(coalesce(toFloat64(d.DAY_VOL),toFloat64(0))) AS "月度运行计划",
                avg(coalesce(toFloat64(d.SUM_ALLOC_VOL_MONTH),toFloat64(0))) AS "年度分月计划",
                IF(TC_YEAR = PROD_YEAR,1,0) AS "是否当年新井",
                IF("是否当年新井" > 0,"实际产气量",0) AS "当年新井实际产气量",
                IF("是否当年新井" > 0,"月度增产气量",0) AS "当年新井月度增产气量",
                IF(TC_YEAR = PROD_YEAR and TC_MONTH = PROD_MON,1,0) AS "是否当月新井",
                IF("是否当月新井" > 0,"实际产气量",0) AS "当月新井实际产气量",
                IF("是否当月新井" > 0,"月度增产气量",0) AS "当月新井月度增产气量",
                IF(TC_YEAR = PROD_YEAR and TC_MONTH = PROD_MON,"新建产能",0) AS "当月新井新建产能",
                IF(TC_YEAR = PROD_YEAR - 1,1,0) AS "是否上年新井",
                IF("是否上年新井" > 0,"实际产气量",0) AS "上年新井实际产气量",
                IF("是否上年新井" > 0,"月度增产气量",0) AS "上年新井月度增产气量",
                IF(TC_YEAR <= PROD_YEAR - 2,1,0) AS "是否老井",
                IF("是否老井" > 0,"实际产气量",0) "老井实际产气量",
                IF("是否老井" > 0,"月度增产气量",0) "老井月度增产气量",
                IF((PROD_YEAR + PROD_MON) >= (TC_YEAR+TC_MONTH) ,1,0) AS "是否投产井",
                IF("实际产气量" > 0 or "生产时间" > 0 or "产水量" > 0 or "产油量" > 0,1,0) AS "是否开井"
		        from dwd.DWD_WELL_PROD_DAILY_DETAIL_D_CZBCQC d
		        where 1 = 1
                and PROD_YEAR < CAST('${year}' AS INT)   and PROD_YEAR > CAST('${year}' AS INT) - 6        
                group by d.WELL_ID,PROD_YEAR,PROD_MON,TC_YEAR,TC_MONTH,d.WELL_COMMON_NAME,N_DOMAIN_NAME
)A
where 1 = 1
GROUP BY A.PROD_YEAR
)
--通过lag函数计算年度综合递减率
SELECT A.PROD_YEAR,
       (1 -  (A."年度产气量" - A."当年新井年度实际产气量")  / B."年度产气量") AS "综合递减率",
       (1 -  (A."年度产气量" - A."当年新井年度实际产气量" - A."老井年度增产气量")  / B."年度产气量") AS "自然递减率",
       (1 -  (A."老井年度产气量" / (B."老井年度产气量" + B."上年新井年度实际产气量") ) ) AS "老井综合递减率"
FROM PROD_YEAR A
LEFT JOIN PROD_YEAR B ON A.PROD_YEAR = B.PROD_YEAR + 1
ORDER BY A.PROD_YEAR

--地质产量
SELECT A.PROD_YEAR,
        sum(A."实际产气量") / 10000  AS "年度产气量-亿方",
        sum(A."年度分月计划") / 10000  AS "年度分月计划-亿方",
        sum(A."实际产气量") / sum(A."年度分月计划")
FROM(
        select  year(toDate(d.PROD_DATE)) AS PROD_YEAR,
                month(toDate(d.PROD_DATE)) AS PROD_MON,
                year(toDate(d.PROD_DATE_STR)) AS TC_YEAR,
                month(toDate(d.PROD_DATE_STR)) AS TC_MONTH,
                case d.DOMAIN_NAME 
		 when '蓬莱' then '蓬莱灯二'
		 when '灯二' then '台缘灯二'
		 when '台缘' then '台缘灯四'
		 else DOMAIN_NAME end N_DOMAIN_NAME,
                d.WELL_ID,
                d.WELL_COMMON_NAME,
                sum(coalesce(toFloat64(d.PROD_TIME),toFloat64(0))) AS "生产时间",
			    sum(coalesce(toFloat64(d.GAS_PROD_DAILY),toFloat64(0))) AS "产气量",
			    sum(coalesce(toFloat64(d.GAS_RELEASE_DAILY),toFloat64(0))) AS "放气量",
                sum(coalesce(toFloat64(d.WATER_PROD_DAILY),toFloat64(0))) AS "产水量",
                sum(coalesce(toFloat64(d.OIL_PROD_DAILY),toFloat64(0))) AS "产油量",
                max(coalesce(toFloat64(d.GAS_PROD_INCR_MON),toFloat64(0)))  AS "月度增产气量",
                max(coalesce(toFloat64(d.PRODUCTIVITY_CALIBRATE),toFloat64(d.PLAN_SUIT_PRODUCTIVITY)))  AS "新建产能",
                max(ifnull(d.GAS_PROD_CUM,0)) "累计产气量",
                max(ifnull(d.OIL_PROD_CUM,0)) "累计产油量",
                ("产气量" - "放气量") /10000 AS "实际产气量",
                sum(coalesce(toFloat64(d.DAY_VOL),toFloat64(0))) AS "月度运行计划",
                avg(coalesce(toFloat64(d.SUM_ALLOC_VOL_MONTH),toFloat64(0))) AS "年度分月计划",
                IF(TC_YEAR = PROD_YEAR,1,0) AS "是否当年新井",
                IF("是否当年新井" > 0,"实际产气量",0) AS "当年新井实际产气量",
                IF("是否当年新井" > 0,"月度增产气量",0) AS "当年新井月度增产气量",
                IF(TC_YEAR = PROD_YEAR and TC_MONTH = PROD_MON,1,0) AS "是否当月新井",
                IF("是否当月新井" > 0,"实际产气量",0) AS "当月新井实际产气量",
                IF("是否当月新井" > 0,"月度增产气量",0) AS "当月新井月度增产气量",
                IF(TC_YEAR = PROD_YEAR and TC_MONTH = PROD_MON,"新建产能",0) AS "当月新井新建产能",
                IF(TC_YEAR = PROD_YEAR - 1,1,0) AS "是否上年新井",
                IF("是否上年新井" > 0,"实际产气量",0) AS "上年新井实际产气量",
                IF("是否上年新井" > 0,"月度增产气量",0) AS "上年新井月度增产气量",
                IF(TC_YEAR <= PROD_YEAR - 2,1,0) AS "是否老井",
                IF("是否老井" > 0,"实际产气量",0) "老井实际产气量",
                IF("是否老井" > 0,"月度增产气量",0) "老井月度增产气量",
                IF((PROD_YEAR + PROD_MON) >= (TC_YEAR+TC_MONTH) ,1,0) AS "是否投产井",
                IF("实际产气量" > 0 or "生产时间" > 0 or "产水量" > 0 or "产油量" > 0,1,0) AS "是否开井"
		        from dwd.DWD_WELL_PROD_DAILY_DETAIL_D_CZBCQC d
		        where 1 = 1
                and PROD_YEAR < CAST('${year}' AS INT)   and PROD_YEAR > CAST('${year}' AS INT) - 6        
                group by d.WELL_ID,PROD_YEAR,PROD_MON,TC_YEAR,TC_MONTH,d.WELL_COMMON_NAME,N_DOMAIN_NAME
)A
where 1 = 1
GROUP BY A.PROD_YEAR

--商品量
                SELECT s.ORG_NAME
		       ,p.PROD_DATE 
			,p.INDEX_NAME
			,sum(p.ITEM_VALUE) ITEM_VALUE
		FROM ODS_JCZH.PC_SPL_REPORT_INDEX_ITEM p
		     ,ODS_JCZH.PC_SPL_REPORT s
		WHERE 1 = 1
		AND p.REPORT_NAME = s.REPORT_NAME 
		AND s.ORG_NAME = '川中北部采气管理处'
		AND TO_CHAR(p.PROD_DATE,'YYYY') = ${year} 
		AND p.INDEX_NAME in ('商品量')
                group by s.ORG_NAME,p.PROD_DATE,p.INDEX_NAME


SELECT x."NDMB" NDMB
FROM business_bi."GJZB_WCJDGZHZ2" x
where 1 = 1 
and x."EJZB" = '商品量（亿方）'
and x."YEAR"::char(4) = '${year}'
order by x."UP_TIME" DESC
limit 1

--日产量及开井数
SELECT A.PROD_YEAR,
        sum(A."实际产气量")   AS "当日产气量-万方",
        sum(A."是否开井") AS "当日开井数-口"
FROM(
        select  year(toDate(d.PROD_DATE)) AS PROD_YEAR,
                month(toDate(d.PROD_DATE)) AS PROD_MON,
                year(toDate(d.PROD_DATE_STR)) AS TC_YEAR,
                month(toDate(d.PROD_DATE_STR)) AS TC_MONTH,
                case d.DOMAIN_NAME 
		 when '蓬莱' then '蓬莱灯二'
		 when '灯二' then '台缘灯二'
		 when '台缘' then '台缘灯四'
		 else DOMAIN_NAME end N_DOMAIN_NAME,
                d.WELL_ID,
                d.WELL_COMMON_NAME,
                sum(coalesce(toFloat64(d.PROD_TIME),toFloat64(0))) AS "生产时间",
			    sum(coalesce(toFloat64(d.GAS_PROD_DAILY),toFloat64(0))) AS "产气量",
			    sum(coalesce(toFloat64(d.GAS_RELEASE_DAILY),toFloat64(0))) AS "放气量",
                sum(coalesce(toFloat64(d.WATER_PROD_DAILY),toFloat64(0))) AS "产水量",
                sum(coalesce(toFloat64(d.OIL_PROD_DAILY),toFloat64(0))) AS "产油量",
                max(coalesce(toFloat64(d.GAS_PROD_INCR_MON),toFloat64(0)))  AS "月度增产气量",
                max(coalesce(toFloat64(d.PRODUCTIVITY_CALIBRATE),toFloat64(d.PLAN_SUIT_PRODUCTIVITY)))  AS "新建产能",
                max(ifnull(d.GAS_PROD_CUM,0)) "累计产气量",
                max(ifnull(d.OIL_PROD_CUM,0)) "累计产油量",
                ("产气量" - "放气量") /10000 AS "实际产气量",
                sum(coalesce(toFloat64(d.DAY_VOL),toFloat64(0))) AS "月度运行计划",
                avg(coalesce(toFloat64(d.SUM_ALLOC_VOL_MONTH),toFloat64(0))) AS "年度分月计划",
                IF(TC_YEAR = PROD_YEAR,1,0) AS "是否当年新井",
                IF("是否当年新井" > 0,"实际产气量",0) AS "当年新井实际产气量",
                IF("是否当年新井" > 0,"月度增产气量",0) AS "当年新井月度增产气量",
                IF(TC_YEAR = PROD_YEAR and TC_MONTH = PROD_MON,1,0) AS "是否当月新井",
                IF("是否当月新井" > 0,"实际产气量",0) AS "当月新井实际产气量",
                IF("是否当月新井" > 0,"月度增产气量",0) AS "当月新井月度增产气量",
                IF(TC_YEAR = PROD_YEAR and TC_MONTH = PROD_MON,"新建产能",0) AS "当月新井新建产能",
                IF(TC_YEAR = PROD_YEAR - 1,1,0) AS "是否上年新井",
                IF("是否上年新井" > 0,"实际产气量",0) AS "上年新井实际产气量",
                IF("是否上年新井" > 0,"月度增产气量",0) AS "上年新井月度增产气量",
                IF(TC_YEAR <= PROD_YEAR - 2,1,0) AS "是否老井",
                IF("是否老井" > 0,"实际产气量",0) "老井实际产气量",
                IF("是否老井" > 0,"月度增产气量",0) "老井月度增产气量",
                IF((PROD_YEAR + PROD_MON) >= (TC_YEAR+TC_MONTH) ,1,0) AS "是否投产井",
                IF("实际产气量" > 0 or "生产时间" > 0 or "产水量" > 0 or "产油量" > 0,1,0) AS "是否开井"
		        from dwd.DWD_WELL_PROD_DAILY_DETAIL_D_CZBCQC d
		        where 1 = 1
                and toDate(d.PROD_DATE) = today()        
                group by d.WELL_ID,PROD_YEAR,PROD_MON,TC_YEAR,TC_MONTH,d.WELL_COMMON_NAME,N_DOMAIN_NAME
)A
where 1 = 1
GROUP BY A.PROD_YEAR
--日商品量及商品率
        SELECT *
	FROM (
		SELECT s.ORG_NAME
		       ,p.PROD_DATE 
			   ,p.INDEX_NAME
			   ,p.ITEM_VALUE
		FROM ODS_JCZH.PC_SPL_REPORT_INDEX_ITEM p
		     ,ODS_JCZH.PC_SPL_REPORT s
		WHERE 1 = 1
		AND p.REPORT_NAME = s.REPORT_NAME 
		AND s.ORG_NAME = '川中北部采气管理处'
		AND p.PROD_DATE = TO_DATE(TO_CHAR(SYSDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
		AND p.INDEX_NAME in ('工业产量','商品量','商品率%','硫化氢酸气','井口产量')
	)
	PIVOT (
		MAX(ITEM_VALUE) 
		FOR(INDEX_NAME) 
		IN ('工业产量' AS GAS_PROD_IND ,
		    '商品量' AS GAS_PROD_AMT, 
		    '商品率%' AS GAS_PROD_AMT_RAT,
		    '硫化氢酸气' AS GAS_ACID,
		    '井口产量' AS GAS_PROD)
	) 
	ORDER BY PROD_DATE 
--储量相关
select B.QIKUANG,
       B.N_DOMAIN_NAME,
       SUM(B.DRY_GAS_GEO_RESERVE) AS "地质储量",
       SUM(B.GAS_TECH_REC_RESERVE) AS "技术储量",
       SUM(B.GAS_ECON_REC_RESERVE) AS "经济储量"
from
(
select a.QIKUANG,
       a.DOMAIN_NAME,
       case when BLOCK_NAME='磨溪' then '台缘灯四'
	when BLOCK_NAME='蓬莱' then '蓬莱灯二'
		when (BLOCK_NAME='高石梯' and CAL_UNIT_NAME like '%小层') then '台缘灯二'
		when (BLOCK_NAME='高石梯' and CAL_UNIT_NAME like '%灯四%') then '台缘灯四'
	else BLOCK_NAME end as N_DOMAIN_NAME,
       a.CAL_UNIT_ID,
       a.CAL_UNIT_NAME,
       a.DRY_GAS_GEO_RESERVE,
       a.GAS_TECH_REC_RESERVE,
       a.GAS_ECON_REC_RESERVE
from dws.DWS_RESERVE_PROD_REL a
where 1 = 1
and CAST(a.STATISTICS_YEAR AS INT ) + 1 = ${year}
and a.QIKUANG = '川中北部采气管理处'
group by a.QIKUANG,
       a.DOMAIN_NAME,
        N_DOMAIN_NAME,
       a.CAL_UNIT_ID,
       a.CAL_UNIT_NAME,
       a.DRY_GAS_GEO_RESERVE,
       a.GAS_TECH_REC_RESERVE,
       a.GAS_ECON_REC_RESERVE
)B
GROUP BY B.QIKUANG,B.N_DOMAIN_NAME

--储量动用率


SELECT a."YEAR" ,
	   sum(a."Prov_geo_res") Prov_geo_res,
	   sum(a."Util_geo_res" ) Util_geo_res,
	   sum(a.PLDE_rate) PLDE_rate,
	   sum(a.TWDS_rate) TWDS_rate
FROM (SELECT a."BLOCK" ,
	   a."YEAR" ,
	   a."Prov_geo_res" ,
	   a."Util_geo_res" ,
	   case when a."BLOCK"  = '蓬莱灯二' then a."Util_geo_res" / a."Prov_geo_res"   else 0 end PLDE_rate,
	   case when a."BLOCK"  = '台缘灯四' then a."Util_geo_res" / a."Prov_geo_res"   else 0 end TWDS_rate,
	   round(a."Res_util_rate",6) 
FROM business_bi."CZBB_CLDYL" AS a
where 1 = 1
and CAST(a."YEAR" AS INT ) + 1 = ${year}
) AS a
where 1 = 1
group by a."YEAR";

--动静储量比


SELECT a."YEAR" ,
	   sum(a."Prov_reser") Prov_geo_res,
	   sum(a."Dyna_reser" ) Util_geo_res,
	   sum(a.PLDE_rate) PLDE_rate,
	   sum(a.TWDS_rate) TWDS_rate
FROM (SELECT a."gas_reser" ,
	   a."YEAR" ,
	   a."Prov_reser" ,
	   a."Dyna_reser" ,
	   case when a."gas_reser"  = '蓬莱灯二' then 1 / (a."Prov_reser" / a."Dyna_reser")    else 0 end PLDE_rate,
	   case when a."gas_reser"  = '台缘灯四' then 1 / (a."Prov_reser" / a."Dyna_reser")    else 0 end TWDS_rate
FROM business_bi."CZBB_DJCLB" AS a
where 1 = 1
and CAST(a."YEAR" AS INT ) + 1 = ${year}
) AS a
where 1 = 1
group by a."YEAR"

--专项监测次数



        SELECT 
	   CLASS_CODE_NAME as CLASS_CODE_NAME,
	   substring(YEAR_MONTH,1,4) as YEAR_MONTH,
	   sum(cast(PM_YEAR_MONTH_CNT as Int)) as PM_YEAR_MONTH_CNT,
           sum(cast(AC_YEAR_MONTH_CNT as Int)) as AC_YEAR_MONTH_CNT
	FROM dws.DWS_DTJC_SPEC_MONITOR_DATA
        where SECORGNAME='川中北部采气管理处'
	and YEAR_MONTH = '${year}'
	and MONITOR_TYPE='1'
        group by CLASS_CODE_NAME,YEAR_MONTH

---点地图

select
	'川中北部采气管理处' as 区域,
	'105.590854' as 经度,
	'30.504799' as 纬度,		'川中古隆起安岳气田高磨台缘带和蓬莱气田灯二、灯四气藏，地跨遂宁、资阳、绵阳等6市16县，工区面积达10050平方千米' as 工区概况,
	'各类场站'||集气站+生产井+非生产井||'座（集气站'||集气站||'座，生产井'||生产井||'座，非生产井'||非生产井||'座），管道'||管道数||'条，总长'||管道长度||'千米' as 集输现状
from
(
select 
	"business_bi"."CQC_GLCJSXZ"."YEAR" as 年月,
	"business_bi"."CQC_GLCJSXZ"."JQZ" as 集气站,
	"business_bi"."CQC_GLCJSXZ"."SCJ" as 生产井,
	"business_bi"."CQC_GLCJSXZ"."FSCJ" as 非生产井,
	"business_bi"."CQC_GLCJSXZ"."GDS" as 管道数,
	"business_bi"."CQC_GLCJSXZ"."GDCD" as 管道长度
from "business_bi"."CQC_GLCJSXZ"
where "business_bi"."CQC_GLCJSXZ"."YEAR"='${year}'
) a

--实时数据周报
select A.DEVICE_NAME,
	  A.ISO_YEAR,
	  A.ISO_WEEK p_week,
	  A.WEEK_START p_week_start,
	  A.WEEK_END p_week_end,
	  coalesce(A.SSLL_DIFF_WEEK,A.YJJL_DIFF_WEEK) FIRST_FLAG
	  A.YL_DIFF_WEEK SECOND_FLAG, 
from sx.RTDA_WEEK_TIME_CZBCQC A
WHERE 1 = 1
--AND A.ISO_YEAR = '${w_year}'
--AND A.ISO_WEEK >= '${s_w_week}'
--AND A.ISO_WEEK <= '${e_w_week}'
AND toDate(A.WEEK_START) >= toDate('${w_start_date}')
AND toDate(A.WEEK_END) <= toDate('${w_end_date}')
order by A.DEVICE_NAME,A.ISO_YEAR,
	  A.ISO_WEEK