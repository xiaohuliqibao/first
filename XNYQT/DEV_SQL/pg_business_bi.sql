/**
* 人力资源部门相关
*/

--人力资源组件展示表结构
CREATE TABLE business_bi.yzt_hris_indicator_display (
	display varchar(255)  NULL,
	indicator_seq varchar(255) NULL,
	indicator_name varchar(255) NULL,
	indicator_url varchar(255) NULL,
	indicator_uuid varchar(255) not NULL,
	createuser varchar(255) NULL,
	createtime varchar(255) NULL,
	CONSTRAINT yzt_hris_indicator_display_pk PRIMARY KEY (indicator_uuid)
);

select distinct indicator_name,cast(indicator_seq as int)
from yzt_hris_indicator_display
where  1 = 1
and display_year = '${year}'
order by cast(indicator_seq as int) ;

--人力资源评分规则表结构
CREATE TABLE business_bi.yzt_hris_ass_rules (
	rule_name varchar(255) NULL,
	rule_description varchar(1000) NULL,
	rule_type varchar(255) NULL,
	rule_uuid varchar(255) not NULL,
	createuser varchar(255) NULL,
	createtime varchar(255) NULL,
	CONSTRAINT yzt_hris_ass_rules_pk PRIMARY KEY (rule_uuid)
);

select * 
from yzt_hris_ass_rules
where  1 = 1
order by rule_type;

--人力资源部总分权重表结构
CREATE TABLE business_bi.yzt_hris_indicator_weight (
	indicator_name varchar(255) NULL,
	second_indicator varchar(255) NULL,
	second_indicator_weight varchar(255) NULL,
	second_indicator_year varchar(255) NULL,
	second_indicator_rule varchar(255) NULL,
	second_indicator_uuid varchar(255) not NULL,
	createuser varchar(255) NULL,
	createtime varchar(255) NULL,
	CONSTRAINT yzt_hris_indicator_weight_pk PRIMARY KEY (second_indicator_uuid)
);

select a.*
from yzt_hris_indicator_weight a
left join yzt_hris_indicator_display b
 on a.indicator_name = b.indicator_name 
where 1 = 1
order by cast(b.indicator_seq as int);

-- DROP FUNCTION business_bi.hris_cual_rule(text, numeric, numeric);

--人力资源得分快速计算函数
--参数：规则名称，目标指标，实际指标 输出：实际评分
CREATE OR REPLACE FUNCTION business_bi.hris_cual_rule(rules text, plan numeric, act numeric)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
  results NUMERIC;
BEGIN
  CASE rules
  	when '规则一' then 
  		if act > plan then results = 120; 
					  else results = act / plan * 100; 
	    end if;
  	when '规则二' then 
  		results = act / plan * 100;
  	when '规则三' then
  		if act < plan then results = 120; 
					  else results = act / plan * 100; 
	    end if;
  	when '规则四' then
		results = (1 - act / plan) * 100 + 100;
	when '规则五' then
		results = 100 + 20 * (act - plan) / (100 - act);
	ELSE
      RAISE EXCEPTION '无效操作:%!', rules;
  END CASE;
  if results > 120 then results = 120;
  end if;
  RETURN results;
END;
$function$
;


--人力部得分明细表
select c.indicator_name
	   ,c."PERFORMANCE_METRIC" 
	   ,c."DATE_YEAR" 
	   ,c."UNIT"
	   ,c.second_indicator_weight 
	   ,hris_cual_rule(coalesce(c.second_indicator_rule,'规则一'),c.month_target_value,coalesce(cast(c.achieved_value as numeric),c.month_target_value)) * cast(c.second_indicator_weight as numeric) grade
	   ,c."ANNUAL_TARGET_VALUE"
	   ,c.month_target_value
	   ,c.achieved_value 
	   ,c.second_indicator_rule
	   ,hris_cual_rule(coalesce(c.second_indicator_rule,'规则一'),c.month_target_value,coalesce(cast(c.achieved_value as numeric),c.month_target_value)) df
from(
	SELECT x."PERFORMANCE_METRIC" 
		   ,x."DATE_YEAR" 
		   ,x."ANNUAL_TARGET_VALUE" 
		   ,x."UNIT"
		   ,z.achieved_value 
		   ,y.indicator_name
		   ,y.second_indicator_weight 
		   ,case when y.second_indicator_rule =  '' then '规则一' else y.second_indicator_rule end second_indicator_rule
		   ,case when y.second_indicator_rule in ('规则一','规则二') then CAST(coalesce(x."ANNUAL_TARGET_VALUE",'1') as numeric) / 12 * 4 else CAST(coalesce(x."ANNUAL_TARGET_VALUE",'1') as numeric) end month_target_value
	FROM business_bi."ANNUAL_PERFORMANCE_TARGETS_FORM" AS x
	left join yzt_hris_indicator_weight as y 
		on y.second_indicator  = x."PERFORMANCE_METRIC" 
		and y.second_indicator_year  = "DATE_YEAR" 
	left join (
				select z.indicator_type,
					   z.years ,
					   cast(z.achieved_value as float) achieved_value 
					from hris_assessment_indicators z
					where 1 = 1
					and z.months = '4' 
					union all
					SELECT '新能源产量当量' as indicator_type,
						    left(a.ny,4) as years,
					        sum(case when a.xmlb in ('光伏','地热','余压') then a.act * 3.055 else   case when a.xmlb in ('生物天然气') then a.act * 13.3 else a.act end end ) / 10000 as achieved_value
					FROM "business_bi"."yzt_ghjh_xny" a
					where 1 = 1 
					and right(a.ny,2)='04'
					group by left(a.ny,4)
					union all
					select '单位完全成本（新能源）' indicator_type,
						   z.years ,
						   cast(z.achieved_value as float) / b.achieved_value achieved_value
					from hris_assessment_indicators z
					  left join (SELECT '新能源产量当量' as indicator_type,
								 	    left(a.ny,4) as years,
							    	    sum(case when a.xmlb in ('光伏','地热','余压') then a.act * 3.055 else   case when a.xmlb in ('生物天然气') then a.act * 13.3 else a.act end end ) / 10000 as achieved_value
								FROM "business_bi"."yzt_ghjh_xny" a
								where 1 = 1 
								and right(a.ny,2)='04'
								group by left(a.ny,4)
								)b
							on z.years = b.years
					where 1 = 1
					and z.months = '4' 
					and z.indicator_type = '单位完全成本（新能源）-成本总额'
					) z
		on z.indicator_type = x."PERFORMANCE_METRIC" 
		and z.years = x."DATE_YEAR" 
)c 
where 1 = 1

--人力资源奋斗目标
select * from business_bi.business_bi.yzt_lnkqmj


--？？
select z.indicator_type,
	   z.years ,
	   cast(z.achieved_value as float) achieved_value 
from hris_assessment_indicators z
where 1 = 1
and z.months = '4' 
union all
SELECT '新能源产量当量' as indicator_type,
	    left(a.ny,4) as years,
        sum(case when a.xmlb in ('光伏','地热','余压') then a.act * 3.055 else   case when a.xmlb in ('生物天然气') then a.act * 13.3 else a.act end end ) / 10000 as achieved_value
FROM "business_bi"."yzt_ghjh_xny" a
where 1 = 1 
and right(a.ny,2)='04'
group by left(a.ny,4)
;

--？？
select '单位完全成本（新能源）' indicator_type,
	   z.years ,
	   cast(z.achieved_value as float) / b.achieved_value achieved_value
from hris_assessment_indicators z
  left join (SELECT '新能源产量当量' as indicator_type,
			 	    left(a.ny,4) as years,
		    	    sum(case when a.xmlb in ('光伏','地热','余压') then a.act * 3.055 else   case when a.xmlb in ('生物天然气') then a.act * 13.3 else a.act end end ) / 10000 as achieved_value
			FROM "business_bi"."yzt_ghjh_xny" a
			where 1 = 1 
			and right(a.ny,2)='04'
			group by left(a.ny,4)
			)b
		on z.years = b.years
where 1 = 1
and z.months = '4' 
and z.indicator_type = '单位完全成本（新能源）-成本总额'


/*
* 全国各盆地天然气相关
*/

--全国各盆地天然气产量表结构
CREATE TABLE business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN" (
	basin	varchar(255) NOT NULL,
	enterprise varchar(255) NOT NULL,
	year varchar(255) NOT NULL,
	prod_type varchar(255) NOT NULL,
	production_amt numeric(255, 8) NULL,
	remark varchar(255) NULL,
	create_user varchar(255) NULL,
	create_time varchar(255) NULL,
	update_user varchar(255) NULL,
	update_time varchar(255) NULL
);

update business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN" set company = '中国石油天然气集团公司';

update business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN" set enterprise = '江汉油田' where enterprise = '江汉' ;

--全国天然气产量各领域表结构
CREATE TABLE business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN_ENERGY" (
	basin	varchar(255) NOT NULL,
	enterprise varchar(255) NOT NULL,
	year varchar(255) NOT NULL,
	prod_type varchar(255) NOT NULL,
	energy_type varchar(255) NOT NULL,
	production_amt numeric(255, 8) NULL,
	remark varchar(255) NULL,
	create_user varchar(255) NULL,
	create_time varchar(255) NULL,
	update_user varchar(255) NULL,
	update_time varchar(255) NULL
);

delete from  business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN";


/**
 * 财务部相关
 */

--财务简报数据表结构
CREATE TABLE business_bi."CWB_FACT_DOCUMENT_LIST" (
	document_uuid	varchar(255) NOT NULL,
	document_name varchar(255) NOT NULL,
	document_mon varchar(255) NOT NULL,
	update_user varchar(255) NOT NULL,
	remark varchar(255) NULL
);
--参股公司利润表结构
CREATE TABLE business_bi."CWB_PROFIT_COMPANY" (	
	BBZD_MC varchar(255) NOT null,
	BBZD_ID varchar(255) NOT NULL,
	LBZD_MC varchar(255) NOT NULL,
	BBZD_DATE varchar(255) NOT NULL,
	HZD_MC varchar(255) NOT NULL,
	HZD_ID varchar(255) NOT NULL,
	NUMBER_MONTH numeric NOT NULL,
	ACCUMULATED_YEAR numeric NOT NULL,
	SAME_LAST_YEAR	numeric NOT NULL,
	DATA_UUID	varchar(255) NOT NULL,
	creater_user varchar(255) NOT NULL,
	creater_time varchar(255) NOT NULL,
	update_user varchar(255) NOT NULL,
	update_time varchar(255) NOT NULL,
	REMARK varchar(255) NULL
);

comment on table business_bi."CWB_PROFIT_COMPANY" is '参股公司利润表';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".bbzd_mc IS '源表名';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".bbzd_id IS '源表ID';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".lbzd_mc IS '公司';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".bbzd_date IS '数据年月';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".hzd_mc IS '项目名称';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".hzd_id IS '项目行数';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".number_month IS '本月实际';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".accumulated_year IS '本年累计';
COMMENT ON COLUMN business_bi."CWB_PROFIT_COMPANY".same_last_year IS '上年同期';


--公司整体增加值

CREATE TABLE business_bi."CWB_ADDED_VALUE" (	
	year_month VARCHAR(6) NOT NULL,  -- 年月（格式：YYYYMM，如202405）
    total_profit NUMERIC(18, 6) NOT NULL DEFAULT 0,  
    employee_compensation_cost NUMERIC(18, 6) NOT NULL DEFAULT 0, 
    taxes_and_fees NUMERIC(18, 6) NOT NULL DEFAULT 0, 
    depreciation_amortization NUMERIC(18, 6) NOT NULL DEFAULT 0,  
    company_overall_value_added NUMERIC(18, 6) NOT NULL DEFAULT 0, 
	DATA_UUID	varchar(255) NOT NULL,
	creater_user varchar(255) ,
	creater_time varchar(255) ,
	update_user varchar(255) ,
	update_time varchar(255) ,
	REMARK varchar(255) NULL
);

-- 字段注释（增强可读性）
COMMENT ON TABLE business_bi."CWB_ADDED_VALUE" IS '公司整体增加值统计表，记录各月核心财务指标及增加值计算';
COMMENT ON COLUMN business_bi."CWB_ADDED_VALUE".year_month IS '年月，格式YYYYMM（如202405表示2024年5月）';
COMMENT ON COLUMN business_bi."CWB_ADDED_VALUE".total_profit IS '利润总额（企业一定时期内全部利润总和，单位：元）';
COMMENT ON COLUMN business_bi."CWB_ADDED_VALUE".employee_compensation_cost IS '员工薪酬成本（含工资、奖金、社保、福利等，单位：元）';
COMMENT ON COLUMN business_bi."CWB_ADDED_VALUE".taxes_and_fees IS '税费（企业缴纳的全部税费，如增值税、所得税等，单位：元）';
COMMENT ON COLUMN business_bi."CWB_ADDED_VALUE".depreciation_amortization IS '折旧折耗及摊销（固定资产折旧、无形资产摊销等非现金支出，单位：元）';
COMMENT ON COLUMN business_bi."CWB_ADDED_VALUE".company_overall_value_added IS '公司整体增加值（核心指标，计算公式：利润总额+员工薪酬成本+税费+折旧折耗及摊销，单位：元）';

INSERT INTO business_bi."CWB_ADDED_VALUE" (year_month, total_profit, employee_compensation_cost, taxes_and_fees, depreciation_amortization, company_overall_value_added,data_uuid) 
VALUES 
('201601', 923027312.8, 5768585076.69, 1369154382.38, 9929055942.91, 17989822714.78,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa1fd'),
('201701', 3877044291.6, 5906617827.89, 1419035270.01, 10187983691.2, 21390681080.69,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa2fd'),
('201801', 7269839148.23, 6543468623.0, 1497273394.45, 8679674183.5, 23990255349.18,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa3fd'),
('201901', 6316824313.49, 7172797536.11, 1929561646.89, 10250009269.0, 25669192765.48,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa4fd'),
('202001', 10920758105.89, 6728772239.65, 2210887487.92, 10103224689.52, 29963642522.97,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa5fd'),
('202101', 12198190000, 7475800000, 2373180000, 12667620000, 34714790000,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa6fd'),
('202201', 13473355688.05, 8117817761.2, 2596195487.7, 14017382495.38, 38204751432.33,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa7fd'),
('202301', 14222009192.81, 8591551771.34, 2792774898.24, 15212558508.67, 40818894371.06,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa8fd'),
('202401', 16343722441.96, 8623590390.52, 3370146408.61, 16010490169.35, 44347949410.44,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa9fd'),
('202501', 23473912802.61, 7782368231.27, 7284936435.69, 17395194508.44, 55936411978.01,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa0fd');


-- 创建表：上市部分ROE（表名：business_bi."CWB_LISTED_ROE"）
CREATE TABLE business_bi."CWB_LISTED_ROE" (
    year_month VARCHAR(6) NOT NULL,    
    total_profit NUMERIC(18, 6) NOT NULL DEFAULT 0,    
    net_profit NUMERIC(18, 6) NOT NULL DEFAULT 0,
    return_on_equity_roe NUMERIC(10, 4) NOT NULL DEFAULT 0,
    net_assets NUMERIC(18, 6) NOT NULL DEFAULT 0,
	DATA_UUID	varchar(255) NOT NULL,
	creater_user varchar(255) ,
	creater_time varchar(255) ,
	update_user varchar(255) ,
	update_time varchar(255) ,
	REMARK varchar(255) NULL
);

-- 表注释（说明表用途）
COMMENT ON TABLE business_bi."CWB_LISTED_ROE" IS '上市部分企业净资产收益率（ROE）统计表，记录各月核心财务指标及ROE计算依据';

-- 字段注释（含业务含义、单位、格式）
COMMENT ON COLUMN business_bi."CWB_LISTED_ROE".year_month IS '年月，格式YYYYMM（如202405表示2024年5月），主键';
COMMENT ON COLUMN business_bi."CWB_LISTED_ROE".total_profit IS '利润总额（企业税前总利润，单位：元，保留两位小数）';
COMMENT ON COLUMN business_bi."CWB_LISTED_ROE".net_profit IS '净利润（利润总额扣除所得税后净额，单位：元，保留两位小数）';
COMMENT ON COLUMN business_bi."CWB_LISTED_ROE".return_on_equity_roe IS '净资产收益率ROE（计算公式：净利润/净资产×100%，单位：%，保留4位小数）';
COMMENT ON COLUMN business_bi."CWB_LISTED_ROE".net_assets IS '净资产（所有者权益，资产总额-负债总额，单位：元，保留两位小数）';

-- 插入数据到 business_bi."CWB_LISTED_ROE" 表
INSERT INTO business_bi."CWB_LISTED_ROE" (year_month, total_profit, net_profit, return_on_equity_roe, net_assets) VALUES
(201612, 780457312.8, 663388715.88, 0.012320814726, 53842926025.7,'bd1d5a22-a7a6-4bd9-abe7-1ba4a65aa1fd'),
(201712, 3783834291.6, 3216259147.86, 0.058259682536, 55205572840.13,'bd1d5a22-a7a6-4bd9-abe7-2ba4a65aa1fd'),
(201812, 7109539148.23, 6043108276, 0.110912663752, 54485286635.02,'bd1d5a22-a7a6-4bd9-abe7-3ba4a65aa1fd'),
(201912, 6506514313.49, 5530537166.47, 0.084917701085, 65128201726.83,'bd1d5a22-a7a6-4bd9-abe7-4ba4a65aa1fd'),
(202012, 10879026286.69, 9247172343.69, 0.130176391423, 71035709644.46,'bd1d5a22-a7a6-4bd9-abe7-5ba4a65aa1fd'),
(202112, 11946840000, 10154814000, 0.125366405028, 81001078381.14,'bd1d5a22-a7a6-4bd9-abe7-6ba4a65aa1fd'),
(202212, 12890885688.05, 10957252834.84, 0.130029233996, 84267610429.6,'bd1d5a22-a7a6-4bd9-abe7-7ba4a65aa1fd'),
(202312, 13556559192.81, 11523075313.89, 0.13030000, 88446979489.64,'bd1d5a22-a7a6-4bd9-abe7-8ba4a65aa1fd'),
(202412, 15668053346.4, 13317845344.44, 0.14460000, 92101494146.29,'bd1d5a22-a7a6-4bd9-abe7-9ba4a65aa1fd'),
(202512, 23108106546.46, 19641890564.49, 0.206391360764, 95168181903.47,'bd1d5a22-a7a6-4bd9-abe7-0ba4a65aa1fd');

/**
* 地球物理部
*/
--开发部矿权面积
SELECT x."PERFORMANCE_METRIC" 
		   ,x."DATE_YEAR" 
		   ,x."ANNUAL_TARGET_VALUE" 
		 	FROM business_bi."ANNUAL_PERFORMANCE_TARGETS_FORM" AS x;


/**
* 气田开发部
*/
--开发部 每日推送欠产文本
SELECT 
     array_to_string(ARRAY(SELECT unnest(array_agg("PROCESS_TEXT" order by "SORT" ))),'') AS 
content
FROM  
    "PRODUCTION_VARIANCE_ANALYSIS_DAILY"
where "TEXT_DATE"=to_char(now(),'yyyy-mm-dd')
;
-- 川西北气矿低丰度
SELECT * FROM business_bi.cxbqk_dim_low_abundance where well_common_name in ('关4','双探1');

--页岩气欠产原因周报
SELECT x.* FROM business_bi."YYQQCYYFX" AS x
ORDER BY x.jssj desc;




SELECT 
    n.nspname AS schema_name,
    c.relname AS table_name,
    pg_catalog.obj_description(a.attrelid, 'pg_class') AS table_comment,
    a.attname AS column_name,
    d.description AS column_comment
FROM 
    pg_catalog.pg_class c
JOIN 
    pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN 
    pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
LEFT JOIN 
    pg_catalog.pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
WHERE 
    c.relkind = 'r' 
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname NOT LIKE 'pg_toast%'
ORDER BY 
    schema_name, 
    table_name, 
    a.attnum;

SELECT  
	   device_name, 
	   sum(case when dw_lx = '瞬时流量' then cast(dw_yjz as float ) else 0 end) ssll_yjz,
	   sum(case when dw_lx = '井口油压' then cast(dw_yjz as float ) else 0 end) yl_yjz,
	   sum(case when dw_lx = '一级节流压力' then cast(dw_yjz as float ) else 0 end) yjjl_yjz,
	   sum(case when dw_lx = '二级节流压力' then cast(dw_yjz as float ) else 0 end) ejjl_yjz
FROM rtda.dw_info_czbcqc
where 1= 1 
and device_type = '1'
and dw_lx in ('瞬时流量','井口油压','一级节流压力','二级节流压力')
group by device_name
order by device_name;


select
  concat( a.energy_type,'--',round( a.production_amt,2),'亿方') company,
  a.production_amt,
  sum(production_amt) over (partition by a."year" order by a."year")  production_sum,
  a.production_amt / sum(production_amt) over (partition by a."year" order by a."year")  zb,
  1 rm
from business_bi.business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN_ENERGY" a
where 1=1
and a."year" ='2024'
order by a.production_amt desc;

with a as (
SELECT sum(production_amt) as production_amt 
FROM business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN" 
where 1 = 1
and  "year" ='${year}'
and prod_type = '实际'
and basin = '四川盆地'
),b as(
SELECT sum(production_amt) as production_amt 
FROM business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN" 
where 1 = 1
and  cast("year" as integer) = cast('${year}' as integer) - 1
and prod_type = '实际'
and basin = '四川盆地'
)
select a.production_amt as "年产量",
	   a.production_amt - b.production_amt as "同比",
	   a.production_amt/b.production_amt - 1 as "增长率"
from a left join b on 1 = 1
where 1 = 1;



select "date" as "year","name",增长率 from 
(
SELECT 
"date",
"short_name" as "name",
sum("production_amt")/sum("last_production_amt")-1 as 增长率
FROM "business_bi"."qgtrqcltj"
where 1=1
and CAST("date" as INTEGER)>= 2000
GROUP BY "date","short_name"
union
SELECT 
"date",
'全国' as "name",
"qghj"/"last_qghj"-1 as 增长率
FROM (select distinct "date","all_amt","last_all_amt","qghj","last_qghj"
FROM "business_bi"."qgtrqcltj"
where "last_all_amt" is not null
) tab
where 1=1
and CAST("date" as INTEGER)>= 2000
union 
SELECT 
"date",
"company" as name,
sum("production_amt")/sum("last_production_amt")-1 as 增长率
FROM "business_bi"."qgtrqcltj"
where 1=1
and "company"='中国石油天然气集团公司'
and CAST("date" as INTEGER)>= 2000
GROUP BY "date","company"
) tab 
order by "date",增长率 desc;




SELECT 
"date",
"short_name",
sum("production_amt") as 产量,
sum("production_amt")-sum("last_production_amt") as 同比,
sum("production_amt")/sum("last_production_amt")-1 as 增长率
FROM "business_bi"."qgtrqcltj"
where 1=1
and CAST("date" as INTEGER)>= 2000
GROUP BY "date","short_name" order by "date" asc;


with a as (
SELECT "year",enterprise,sum(production_amt) as production_amt 
FROM business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN" 
where 1 = 1
and  cast("year" as integer) >= 2000
and prod_type = '实际'
and basin = '四川盆地'、
company = '中国石油天然气集团公司'
group by "year" ,enterprise
),
b as (
SELECT cast("year" as integer) + 1  as "year",enterprise ,sum(production_amt) as production_amt 
FROM business_bi."QTKFGLB_FACT_QGTRQCLTJB_BASIN" 
where 1 = 1
and  cast("year" as integer) >= 2000
and prod_type = '实际'
and basin = '四川盆地'
company = '中国石油天然气集团公司'
group by "year",enterprise
)
select a."year" "year", 
 	   a.enterprise "name",
       a.production_amt as "年产量",
	   a.production_amt - b.production_amt as "同比",
	   case when b.production_amt = 0 then 0 else a.production_amt/b.production_amt - 1 end as "增长率"
from a left join b on 1 = 1 and cast(a."year" as integer)  = b.year and a.enterprise = b.enterprise
where 1 = 1
order by a.year,a.enterprise;

select *
from business_bi."CWB_FACT_DOCUMENT_LIST"
where 1 = 1
and left(document_mon,4) = '2025'


