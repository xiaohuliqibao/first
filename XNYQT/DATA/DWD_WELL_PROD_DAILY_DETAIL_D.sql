    SELECT A.RQ,
         PPWVD.PROD_TIME,
         coalesce(PPWVD.GAS_PROD_DAILY,0),
         coalesce(PPWVD.WATER_PROD_DAILY,0),
         PPWVD.OIL_PROD_DAILY,
         coalesce(PPWVD.GAS_RELEASE_DAILY,0),
         PPWVD.GAS_PROD_MONTH,
         PPWVD.WATER_PROD_MONTH,
         PPWVD.OIL_PROD_MONTH, --2023.10.24
         PPWVD.GAS_RELEASE_MONTH,
         PPWVD.GAS_PROD_YEAR,
         PPWVD.WATER_PROD_YEAR,
         PPWVD.OIL_PROD_YEAR,
         PPWVD.GAS_RELEASE_YEAR, --2023.10.24
         PPWVD.GAS_PROD_CUM,
         PPWVD.WATER_PROD_CUM,
         PPWVD.OIL_PROD_CUM,
         PPWVD.TUBING_PRES,
         PPWVD.MAX_OIL_PRES,
         PPWVD.MIN_OIL_PRES,
         PPWVD.AVG_OIL_PRES, --2023.10.24
         PPWVD.CASING_PRES,
         PPWVD.SHUTDOWN_TUBING_PRES,
         PPWVD.SHUTDOWN_CASING_PRES,
         PPWVD.STATION_EXIT_PRES,
         A.WELL_ID,
         PPWVP.PLAN_DATE,
         PPWVP.GAS_PLAN_DAILY,
         PPWVP.GAS_PLAN_MONTH,
         PPWVP.GAS_PLAN_YEAR,
         A.ROOT_ID,
         A.ROOT_NAME,
         A.OPC_ID,
         A.OPC_NAME,
         A.OPC_SEQ,
         A.ORG_ID,
         A.ORG_NAME,
         A.PROJECT_ID,
         A.PROJECT_NAME,
         A.WELL_COMMON_NAME,
         coalesce(A.A2_WELL_ID, A.WELL_ID),
         A.WELL_PURPOSE,
         A.WELL_PURPOSE_DESCRIPTION,
         A.WELL_TYPE,
         A.WELL_TYPE_DESCRIPTION,
         A.PARENT_DOMAIN_ID,
         A.PARENT_DOMAIN_NAME,
         A.DOMAIN_ID,
         A.DOMAIN_NAME,
         A.PROD_DATE_STR,
         A.WELL_CLASS,
         A.WELL_DEV_STATUS,
         A.WELL_DEV_STATUS_DESCRIPTION,
         A.PLAN_SUIT_PRODUCTIVITY,
         A.PRODUCTIVITY_CALIBRATE,
         A.LOC_STATE,
         A.LOC_STATE_NAME,
         A.LOC_CITY,
         A.LOC_CITY_NAME,
         A.LOC_COUNTY,
         A.LOC_COUNTY_NAME,
         A.WELL_SOURCE,
         A.RESERVOIR_ID,
         A.RESERVOIR_NAME,
         A.RESERVOIR_ALIAS_NAME,
         A.OPC_NAME_2,
         A.OPC_NAME_0,
         A.GEO_LONGITUDE,
         A.GEO_LATITUDE,
         A.ENERGY_TYPE,
         PPWVP.PLAN_GAS_PROD_MON,
         C.YEAR_MON STIM_MON,
         C.GAS_INCR_DAILY,
         C.GAS_PROD_INCR_MON,
         C.GAS_INCR_YEAR,
         C.WATER_INCR_DAILY,
         C.WATER_INCR_MON,
         C.WATER_INCR_YEAR,
         C.OIL_INCR_CAPA_DAILY,
         C.OIL_PROD_INCR_MON,
         C.OIL_INCR_YEAR,
         C.EFFECTIVE_DAYS,
         C.PRIMARY_STIM,
         D.EUR_DATE,
         D.OTEH_METHOD,
         D.TECHNICAL_METHOD,
         D.ECONOMIC_METHOD,
         F.PRODUCTIVITY_MON,
         A.PLAN_SUIT_PRODUCTIVITY_Y,
         A.SUIT_PRODUCTIVITY_Y,
         a.rq_m_str,
     --    (case when a.rq_m_str like '2025%' then H.ALLOC_VOL_MONTH else G.ALLOC_VOL_MONTH end ),
     --    (case when a.rq_m_str like '2025%' then H.SUM_ALLOC_VOL_MONTH else  G.SUM_ALLOC_VOL_MONTH end),
          H.ALLOC_VOL_MONTH,
          H.SUM_ALLOC_VOL_MONTH,
         a.ZB_FLAG,
         a.N_WELL_PURPOSE ,
         a.N_WELL_PURPOSE_DESCRIPTION ,
         a.N_WELL_TYPE,
         a.N_WELL_TYPE_DESCRIPTION ,
         a.N_WELL_ENERGYTYPE,
         a.N_WELL_ENERGYTYPE_DESCRIPTION ,
         a.N_WELL_CLASS ,
         a.N_WELL_CLASS_DESCRIPTION ,
         a.FIELD_ID  ,
         a.FIELD_NAME ,
         now(),
         PPWVD.MAX_CASING_PRES,
         PPWVD.MIN_CASING_PRES,
         a.domain_id_two,
         a.domain_name_two
    FROM (
        /**
        日历表与井信息表关联，扩展生产天数
        */
        select to_date(cast(a.date_time_id as varchar), 'yyyymmdd') rq,
                 substr(cast(a.date_time_id as varchar), 1, 6) rq_m_str,
                 b.*
            from postgres_mdb.dim.dim_date_time a
            left join postgres_mdb.dim.dim_cd_well b
              on 1 = 1
           where a.date_time_type = 'D'
             -- 取井投产当年开始的数据
             and year(to_date(cast(a.date_time_id as varchar), 'yyyymmdd'))  >=  year(to_date(cast(b.prod_date as varchar), 'yyyymmdd'))
             -- 20000101年之后的数据
             and date_time_id >= 20000101
             --当前日期后两年的数据
             and to_date(cast(a.date_time_id as varchar), 'yyyymmdd') <=
                 date_add('year', 2, CURRENT_DATE)
                 ) a
    left join ora_kfsc.PRODUCTION.PC_PROD_WELL_VOL_DAILY PPWVD
      on a.rq = PPWVD.PROD_DATE
     and a.a2_well_id = PPWVD.well_id
      AND PPWVD.PROD_DATE >= date_add('day', -50, CURRENT_DATE)
    left join (select PPWVP.WELL_id,
                      PPWVP.GAS_PLAN_DAILY,
                      PPWVP.PLAN_DATE,
                      PPWVP.GAS_PLAN_MONTH,
                      PPWVP.GAS_PLAN_YEAR,
                      sum(PPWVP.GAS_PLAN_DAILY) over(PARTITION by PPWVP.WELL_id, date_trunc('month', PPWVP.PLAN_DATE) order by PPWVP.PLAN_DATE) PLAN_GAS_PROD_MON
                 from ora_kfsc.PRODUCTION.PC_PLAN_WELL_VOL_DAILY PPWVP) PPWVP
      on a.rq = PPWVP.PLAN_DATE
     and a.well_id = PPWVP.well_id
     AND  PPWVP.PLAN_DATE >= date_add('day', -50, CURRENT_DATE)
    LEFT JOIN (SELECT YEAR_MON,
                      WELL_ID,
                      SUM(GAS_INCR_DAILY) GAS_INCR_DAILY,
                      SUM(GAS_PROD_INCR_MON) GAS_PROD_INCR_MON,
                      SUM(GAS_INCR_YEAR) GAS_INCR_YEAR,
                      SUM(WATER_INCR_DAILY) WATER_INCR_DAILY,
                      SUM(WATER_INCR_MON) WATER_INCR_MON,
                      SUM(WATER_INCR_YEAR) WATER_INCR_YEAR,
                      SUM(OIL_INCR_CAPA_DAILY) OIL_INCR_CAPA_DAILY,
                      SUM(OIL_PROD_INCR_MON) OIL_PROD_INCR_MON,
                      SUM(OIL_INCR_YEAR) OIL_INCR_YEAR,
                      MAX(EFFECTIVE_DAYS) EFFECTIVE_DAYS,
                      MAX(PRIMARY_STIM) PRIMARY_STIM
                 FROM (SELECT YEAR_MON,
                              WELL_ID,
                              GAS_INCR_DAILY,
                              GAS_PROD_INCR_MON,
                              GAS_INCR_YEAR,
                              WATER_INCR_DAILY,
                              WATER_INCR_MON,
                              WATER_INCR_YEAR,
                              OIL_INCR_CAPA_DAILY,
                              OIL_PROD_INCR_MON,
                              OIL_INCR_YEAR,
                              EFFECTIVE_DAYS,
                              PRIMARY_STIM,
                              row_number() over(partition by well_id, YEAR_MON order by GAS_PROD_INCR_MON desc) rn
                         FROM ora_ods.ODS_KFPT.YYQ_WELL_STIM_MON
                       UNION ALL
                       SELECT YEAR_MON,
                              WELL_ID,
                              GAS_INCR_DAILY,
                              GAS_PROD_INCR_MON,
                              GAS_INCR_YEAR,
                              0                   WATER_INCR_DAILY,
                              0                   WATER_INCR_MON,
                              0                   WATER_INCR_YEAR,
                              OIL_INCR_CAPA_DAILY,
                              OIL_PROD_INCR_MON,
                              OIL_INCR_YEAR,
                              EFFECTIVE_DAYS,
                              null                PRIMARY_STIM,
                              row_number() over(partition by well_id, YEAR_MON order by GAS_PROD_INCR_MON desc) rn
                         FROM ora_ods.ODS_KFPT.CGQ_WELL_STIM_MON)
                         where rn = 1 
                GROUP BY YEAR_MON, WELL_ID) C
      ON a.rq_m_str = C.YEAR_MON
     AND A.A2_WELL_id = C.WELL_ID
    LEFT JOIN (SELECT eur_date,
                      well_name,
                      sum(OTEH_METHOD) OTEH_METHOD,
                      sum(TECHNICAL_METHOD) TECHNICAL_METHOD,
                      sum(ECONOMIC_METHOD) ECONOMIC_METHOD
                 FROM (SELECT a.eur_date,
                              a.well_name,
                              (CASE
                                WHEN a.cal_method = '1800m技术' THEN
                                 a.value
                                ELSE
                                 0
                              END) OTEH_METHOD,
                              (CASE
                                WHEN a.cal_method = '技术' THEN
                                 a.value
                                ELSE
                                 0
                              END) TECHNICAL_METHOD,
                              (CASE
                                WHEN a.cal_method = '经济' THEN
                                 a.value
                                ELSE
                                 0
                              END) ECONOMIC_METHOD,
                              ROW_NUMBER() OVER(PARTITION BY a.eur_date, a.well_name ORDER BY a.update_time DESC) rn
                         FROM ora_ods.ODS_KFPT.xn_bi_well_eur a)
                WHERE rn = 1
                GROUP BY eur_date, well_name) D
      on a.WELL_COMMON_NAME = d.well_name
     and a.RQ = d.eur_date
    LEFT JOIN (select T.WELL_ID,
                      T.VERIFY_YEAR_MON,
                      T.IS_NEW,
                      T.PRODUCTIVITY_MON
                      -- ROW_NUMBER() OVER(PARTITION BY T.WELL_ID, T.VERIFY_YEAR_MON ORDER BY VERIFY_DATE desc) rn
                 FROM ORA_KFSC.PRODUCTION.PC_PRODUCT_VERIFY_MON T) F
      ON A.WELL_ID = F.WELL_ID
     AND a.rq_m_str = F.VERIFY_YEAR_MON
     AND F.IS_NEW  = 1 
     --AND F.rn = 1
   /* LEFT JOIN (SELECT ALLOC_YEAR_MON,WELL_ID,ALLOC_VOL_MONTH,SUM_ALLOC_VOL_MONTH FROM ORA_KFSC.PRODUCTION.PC_ALLOC_YEAR_RESULT 
                ) G
      on A.well_id = G.well_id
     and a.rq_m_str = G.ALLOC_YEAR_MON */
left join (select  cast( a.date_time_id as varchar) as ALLOC_YEAR_MON ,
CASE WHEN b.entity_id  = '3982007e8a544883b319c6860291a808' THEN '946edd6da1ad4c75bde208ce61c4c51d'
		  WHEN b.entity_id  = '3c213f4e78ae44299ec6fb59c27262a2' THEN '9d75af140f5944518fd8735f26ac4f2e'
			  WHEN b.entity_id  = '3e60fa9f5e6842d7891ff603c4345f43' THEN '3CBE4D36C85139AAE0634C16590B4650'
				 WHEN b.entity_id  = '898ddeafa94745a8a9fc813f834afc5b' THEN '3CBE4D36C84939AAE0634C16590B4650'
					WHEN b.entity_id  = 'a226a3bea60845d8aeda32b92aa95e0f' THEN '3CBE4D36C84D39AAE0634C16590B4650' 
					 WHEN b.entity_id  = 'df47e71f2085420b9c88df862ddb8dae' THEN 'a2e2dd7719444ed884fabec079b66638' ELSE b.entity_id  END entity_id ,

(case  
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '01' as decimal) then b.jan_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '02' as decimal) then b.feb_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '03' as decimal) then b.mar_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '04' as decimal) then b.apr_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '05' as decimal) then b.may_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '06' as decimal) then b.jun_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '07' as decimal) then b.jul_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '08' as decimal) then b.aug_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '09' as decimal) then b.sep_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '10' as decimal) then b.oct_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '11' as decimal) then b.nov_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy') || '12' as decimal) then b.dec_result 
end) as ALLOC_VOL_MONTH,

(case  
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '01' as decimal) then b.jan_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '02' as decimal) then b.feb_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '03' as decimal) then b.mar_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '04' as decimal) then b.apr_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '05' as decimal) then b.may_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '06' as decimal) then b.jun_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '07' as decimal) then b.jul_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '08' as decimal) then b.aug_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '09' as decimal) then b.sep_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '10' as decimal) then b.oct_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '11' as decimal) then b.nov_sum_result 
  when a.date_time_id = cast(to_char(current_date, 'yyyy')|| '12' as decimal) then b.dec_sum_result 
end) as SUM_ALLOC_VOL_MONTH 

from postgres_mdb.dim.dim_date_time a 
left join ora_kfsc.production.PC_PROD_SCHEME_DETAIL b 
on 1=1 
and SCHEME_ID  = (select  scheme_id
                   from  ora_kfsc.production.PC_PROD_SCHEME
                   where  create_user='王海丽' and  scheme_year_mon=(to_char(current_date, 'yyyy')|| '01')
                   LIMIT 1)
where a.date_time_type  ='M' and a.year_id =2025) H 
on A.well_id = H.entity_id 
and  a.rq_m_str = H.ALLOC_YEAR_MON
   where /*PPWVD.PROD_DATE is not null or PPWVP.PLAN_DATE is not null or
         G.well_id is not null)
     AND*/ A.RQ >= date_add('day', -35, CURRENT_DATE);
