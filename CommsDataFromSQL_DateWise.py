#!/usr/bin/env python
# coding: utf-8

#importing pacakges
import pandas as pd
import sqlalchemy
import json
import numpy as np
import time

#Variable Description:
#company_id = (str) pass the company id, eg. df518ad3-17ef-4e89-b16a-16f6da7fdf7f
#start_dt = (str) pass the starting date eg. 2022-03-01 (Inclusive RANGE)
#end_dt = (str) pass the end date eg. 2022-05-01 (Inclusive RANGE)
#engine = the connection engine
###########

#Creating engine
engine_redshift = sqlalchemy.create_engine(
    'postgresql://masteruser:NCJSZixbqr931-(.@de-data.cm8prxpm15xq.ap-south-1.redshift.amazonaws.com:5439/prodrs'
)

def getCommLanguage(x):
    if 'tamil' in x.lower():
        return 'ta'
    elif 'kannada' in x.lower():
        return 'kn'
    elif 'bengali' in x.lower():
        return 'be'
    elif 'telugunew' in x.lower():
        return 'te'
    else:
        return 'en'

#Function to send data to redshift
def writetoDB(engine, tablename, df, operation, schema_nm):
    df.to_sql(
        tablename,
        engine,
        schema = schema_nm,
        if_exists=operation,
        index=False,
    chunksize = 20000,
    method = 'multi'
    )


def msgSQLDataPull(company_id, type_of_comm, dt, end_dt):
    
    query = """ select communication.created,communication.company_id,
    type_of_comm,
    communication.loan_id,
    case when type_of_comm = 'sms' then comm_dict->>'sms_language'
         when type_of_comm = 'whatsapp' then comm_dict->>'template_name' else null end as comm_language,
    case when comm_dict->>'delivered_time' != '' then comm_dict->>'delivered_time' else null end as delivery_status,
    case when comm_dict->>'opened_time' != '' then comm_dict->>'opened_time' else null end as opened_status,
    comm_dict->> 'sms_verification_link_clicked_count' as click_count
    from communication
    where communication.company_id = '{company_id}'
    and communication.created::date>= '{dt}' and  communication.created::date<= '{end_dt}'
    and lower(type_of_comm) = '{comm_type}'""".format(company_id = company_id, 
                 comm_type = type_of_comm, dt = dt, end_dt=end_dt)
    
    engine = sqlalchemy.create_engine(
    'postgresql://piyush_read:fvhjdffegjd2wcddc@prod-recovery-read.cnzlch7wyuxy.ap-south-1.rds.amazonaws.com:5432/recovery_service_db'
#     'postgresql://prod_read:4fmgbkFQ4dVD@proddb-reader.cnzlch7wyuxy.ap-south-1.rds.amazonaws.com:5432/url_shortner'
, connect_args={'connect_timeout': 30000})  

    for chunks in pd.read_sql_query(sqlalchemy.text(query), con = engine,chunksize = 50000):
        chunks['company_id'] = chunks['company_id'].astype(str)
        time.sleep(3)
        if len(chunks)> 0 and chunks.type_of_comm.unique()[0] == 'whatsapp':
            chunks['comm_language'] = chunks['comm_language'].apply(lambda x: getCommLanguage(x))
        time.sleep(5)
        writetoDB(engine_redshift, 'msg_data', chunks, 'append', 'datascience_raw')
    
    if len(chunks) <= 0:
        return False
    engine.dispose()
    
    return True


def getSQLData_IVR(company_id, dt, type_of_comm, end_dt):
    query = """select
        loan_id
        ,comm_dict->>'dtmf_ivr_mobile' as mobile
        ,comm_dict->>'start_time' as start_time
        ,comm_dict->>'end_time' as end_time
        ,comm_dict->>'call_duration' as call_duration
        ,comm_dict->>'template_name' as template_name
        ,comm_dict->'ivr_response'->>'scenarioName' as scenario_name
        ,comm_dict->'error'->>'groupName' as error_nm
        ,comm_dict->'ivr_response'->>'collectedDtmfs' as collectedDtmfs
        ,comm_dict->'ivr_response'->>'collectedMappedDtmfs' as collectedMappedDtmfs
        ,created
        ,company_id
        ,type_of_comm

        from communication
        where allocation_month <> '' 
        and lower(type_of_comm) = '{type_of_comm}'
        and created::date >= '{dt}' and created::date <= '{end_dt}'
        and company_id = '{company_id}'""". format(company_id = company_id, dt = dt, 
                                                   type_of_comm=type_of_comm, end_dt = end_dt)
    
    engine = sqlalchemy.create_engine(
    'postgresql://piyush_read:fvhjdffegjd2wcddc@prod-recovery-read.cnzlch7wyuxy.ap-south-1.rds.amazonaws.com:5432/recovery_service_db'
, connect_args={'connect_timeout': 30000})
    
    for chunks in pd.read_sql_query(sqlalchemy.text(query), con = engine,chunksize = 50000):
        chunks['company_id'] = chunks['company_id'].astype(str)
        time.sleep(5)
        writetoDB(engine_redshift, 'ivr_data', chunks, 'append', 'datascience_raw')
    
    if len(chunks) <= 0:
        return False
    engine.dispose()
    
    return True

def getAllocationData(company_id, allocation_dt):
    query = """SELECT a.*, c.mob, c.applicant_contact_number FROM (
                (select *, additional_variables ->>'repayment_mode' as repayment_mode 
                from lending_default_details) a
                LEFT join
                (SELECT company_id, loan_id, 
                loan_details->>'mob' as mob,
                loan_details->>'applicant_contact_number' as applicant_contact_number
                FROM lending_loan_details) c
                ON a.loan_id = c.loan_id and 
                a.company_id = c.company_id  )
            where a.company_id = '{company_id}'
            and allocation_month = '{allocation_dt}'""".format(company_id = company_id, allocation_dt = allocation_dt)
    
    engine = sqlalchemy.create_engine(
    'postgresql://piyush_read:fvhjdffegjd2wcddc@prod-recovery-read.cnzlch7wyuxy.ap-south-1.rds.amazonaws.com:5432/recovery_service_db'
, connect_args={'connect_timeout': 36000})
    for chunks in pd.read_sql_query(sqlalchemy.text(query), con = engine,chunksize = 50000):
        #df_allocation = pd.concat([df_allocation,chunks], ignore_index=True)
        time.sleep(3)
        chunks = chunks.drop(['additional_variables'], axis = 1)
        time.sleep(3)
        chunks['amount_recovered_breakdown'] = chunks['amount_recovered_breakdown'].astype(str)
        time.sleep(3)
        chunks['company_id'] = chunks['company_id'].astype(str)
        time.sleep(3)
        writetoDB(engine_redshift, 'allocation_data', chunks, 'append', 'datascience_raw')
    
    if len(chunks) <= 0:
        return False
    engine.dispose()
    return True
    
    
def getTagInformation(company_id, allocation_dt):
    query = """select
        company_id, loan_id, tag_name, allocation_month
        from tags
        where allocation_month = '{allocation_dt}'
        and company_id = '{company_id}'""". format(company_id = company_id, allocation_dt= allocation_dt)
    
    engine = sqlalchemy.create_engine(
    'postgresql://piyush_read:fvhjdffegjd2wcddc@prod-recovery-read.cnzlch7wyuxy.ap-south-1.rds.amazonaws.com:5432/recovery_service_db'
, connect_args={'connect_timeout': 30000})
    df_tags = pd.DataFrame()
    for chunks in pd.read_sql_query(sqlalchemy.text(query), con = engine,chunksize = 50000):
        chunks['company_id'] = chunks['company_id'].astype(str)
        time.sleep(10)
        #print(chunks.dtypes)
        writetoDB(engine_redshift, 'tag_data', chunks, 'append', 'datascience_raw')
    
    if len(chunks) <= 0:
        return False
    engine.dispose()
    
    return True
    


#Creating engine
engine_redshift = sqlalchemy.create_engine(
    'postgresql://masteruser:NCJSZixbqr931-(.@de-data.cm8prxpm15xq.ap-south-1.redshift.amazonaws.com:5439/prodrs'
)


company_id = 'fd4b18f8-32b1-4dfc-b5b3-bddd7a6aa8b5'
dt = '2022-08-01'
end_dt = '2022-08-22'
allocation_dt = '2022-8-01'


getAllocationData(company_id, allocation_dt)
getTagInformation(company_id, allocation_dt)
getSQLData_IVR(company_id, dt, 'dtmf_ivr',end_dt)
msgSQLDataPull(company_id, 'whatsapp', dt, end_dt)
msgSQLDataPull(company_id, 'sms', dt,end_dt)


# ## Send Whatsapp Aggregated Data


query = """ INSERT INTO datascience_raw.msg_agg_data ( 
SELECT
loan_id, created, company_id,
max(comm_language) as comm_language,
sum(CASE WHEN delivery_status is NULL THEN 0 ELSE 1 end) as delivery_flag,
sum(CASE WHEN opened_status is NULL then 0 else 1 end) as opened_flag,
count(type_of_comm) as trigger_count ,
sum(click_count) as click_count,
'{comm_type}' as type_of_comm
from datascience_raw.msg_data
    where
     lower(type_of_comm) = '{comm_type}'
    and created::date >= '{dt}' and created::date <= '{end_dt}'
    and company_id = '{company_id}'
GROUP by
    loan_id, created, company_id)""".format(company_id = company_id, 
             comm_type = 'whatsapp', dt=dt, end_dt=end_dt)

engine_redshift.execute(query)


# ## Send SMS Aggregated Data



query = """ INSERT INTO datascience_raw.msg_agg_data ( 
SELECT
loan_id,created, company_id,
max(comm_language) as comm_language,
sum(CASE WHEN delivery_status is NULL THEN 0 ELSE 1 end) as delivery_flag,
sum(CASE WHEN opened_status is NULL then 0 else 1 end) as opened_flag,
count(type_of_comm)as trigger_count ,
sum(click_count) as click_count,
'{comm_type}' as type_of_comm
from datascience_raw.msg_data
    where
     lower(type_of_comm) = '{comm_type}'
    and created::date >= '{dt}' and created::date <= '{end_dt}'
    and company_id = '{company_id}'
GROUP by
    loan_id, created, company_id )""".format(company_id = company_id, 
             comm_type = 'sms', dt = dt, end_dt=end_dt)

engine_redshift.execute(query)


# ## Send IVR Aggregated Data

query = """ INSERT INTO datascience_raw.ivr_agg_data (
SELECT loan_id,
CASE WHEN (call_duration)::int > 0 THEN 1 ELSE 0 END as call_pick,
CASE WHEN (call_duration)::int >= 10 THEN 1 ELSE 0 END as call_resp,
CASE WHEN (collectedDtmfs = '{{}}' ) THEN 0 else 1 end as end_of_message_rate,
CASE WHEN (collectedMappedDtmfs ='{{}}' or (collectedMappedDtmfs) = '0') THEN -1
         WHEN (lower(collectedMappedDtmfs) like '%yes%') THEN 1
         WHEN (lower(collectedMappedDtmfs) like '%no%') THEN 2
         ELSE 0 END as ivr_resp_state,
CASE WHEN ivr_resp_state = -1 THEN 'EOM not reached'
WHEN ivr_resp_state = 1 Then 'responded yes'
WHEN ivr_resp_state = 2 Then 'responded no'
else 'Wrong input' end as explicit_response,
COUNT(type_of_comm) as trigger_count,
created, company_id, start_time,
'{comm_type}' as type_of_comm,
CASE WHEN lower(error_nm) = 'user_errors' or lower(error_nm) is null then 0 else 1 end as error_flag
from datascience_raw.ivr_data
where start_time::date >= '{dt}' and start_time::date <= '{end_dt}'
GROUP BY loan_id, created,start_time, company_id,type_of_comm, call_duration, collectedMappedDtmfs, collectedDtmfs,error_nm)""".format(company_id = company_id, 
             comm_type = 'dtmf_ivr', dt = dt, end_dt=end_dt)

engine_redshift.execute(sqlalchemy.text(query))


# ## Send Channel Agg Overview to DB


query = """INSERT INTO datascience_raw.channel_agg_overview (
SELECT company_id, allocation_month, type_of_comm, trunc(created) as "date" ,tag_name,
sum(trigger_count) as total_comms_triggered,
sum(delivery_flag) as total_comms_delivered,
round(1.0 * total_comms_delivered/total_comms_triggered,4) as delivery_percentage,
count(loan_id) as loan_id_count FROM (
SELECT a.*, b.tag_name, b.allocation_month  FROM 
datascience_raw.msg_agg_data a
JOIN
datascience_raw.tag_data b
on a.company_id = b.company_id
and a.loan_id = b.loan_id ) t
WHERE trunc(created) >= '{dt}' and created::date <= '{end_dt}' and company_id = '{company_id}'
group by tag_name, trunc(created), type_of_comm, allocation_month, company_id)""".format(dt=dt
                                                    ,company_id = company_id, end_dt = end_dt)

engine_redshift.execute(sqlalchemy.text(query))

query = """INSERT INTO datascience_raw.channel_agg_overview (
SELECT company_id, allocation_month, type_of_comm, trunc(start_time::date) as "date" ,tag_name,
sum(trigger_count) as total_comms_triggered,
sum(call_pick) as total_comms_delivered,
round(1.0 * total_comms_delivered/total_comms_triggered,4) as delivery_percentage,
count(loan_id) as loan_id_count FROM (
SELECT a.*, b.tag_name, b.allocation_month  FROM 
datascience_raw.ivr_agg_data a
JOIN
datascience_raw.tag_data b
on a.company_id = b.company_id
and a.loan_id = b.loan_id ) t
where start_time is not null
and trunc(created) >= '{dt}' and created::date <= '{end_dt}' and company_id = '{company_id}'
group by tag_name, trunc(start_time::date), type_of_comm, allocation_month, company_id  )""".format(dt=dt
                                                        ,company_id = company_id, end_dt = end_dt)

engine_redshift.execute(sqlalchemy.text(query))


# ## Send Datewise Channel Connectivity and Response to DB


query = """INSERT INTO datascience_raw.channel_connectivity_response (
SELECT company_id, allocation_month, trunc(created) as "date" ,tag_name, type_of_comm,
count(loan_id) as loan_id_count,
sum(delivery_flag) as delivery_flag_sum,
sum(trigger_count) as trigger_count_sum,
sum(opened_flag) as opened_flag_sum,
0.0 as response_percentage,
0.0 as eom_percentage,
sum(click_count) as click_flag_sum,
round(1.0 * sum(trigger_count)/max_triggered,4) as trigger_percentage,
round(1.0 * sum(delivery_flag)/sum(trigger_count),4) as delivery_percentage,
round(1.0 * sum(opened_flag)/sum(trigger_count),4) as react_percentage,
round(1.0 * sum(click_count) /sum(trigger_count),4) as explicit_response_percentage,
max_triggered
FROM (
SELECT a.*, b.tag_name, b.allocation_month, d.max_triggered FROM 
(SELECT loan_id, company_id, type_of_comm,
trunc(created) as created, max(delivery_flag) as delivery_flag,
max(opened_flag) as opened_flag, max(trigger_count) as trigger_count,
max(click_count) as click_count FROM datascience_raw.msg_agg_data
GROUP BY loan_id, company_id, type_of_comm,trunc(created)) a
LEFT JOIN
datascience_raw.tag_data b
on a.company_id = b.company_id
and a.loan_id = b.loan_id
LEFT JOIN
(SELECT company_id, count(loan_id) as max_triggered, tag_name, allocation_month
from datascience_raw.tag_data
group by tag_name, allocation_month, company_id) d
ON a.company_id = d.company_id
and b.tag_name = d.tag_name
and b.allocation_month = d.allocation_month
 ) t
 WHERE trunc(created) >= '{dt}' and created::date <= '{end_dt}' and company_id = '{company_id}'
group by tag_name,max_triggered, trunc(created),type_of_comm, allocation_month, company_id)""".format(dt=dt
                                            ,company_id = company_id, end_dt=end_dt)

engine_redshift.execute(sqlalchemy.text(query))

query = """INSERT INTO datascience_raw.channel_connectivity_response (
SELECT a.company_id, b.allocation_month, "date" ,b.tag_name, a.type_of_comm,
count( a.loan_id) as loan_id_count,
(sum(trigger_count) - sum(error_ct)) as delivery_flag_sum,
sum(trigger_count) as trigger_count_sum,
sum(call_pick) as opened_flag_sum,
round(1.0 * sum(call_resp)/sum(trigger_count),4) as response_percentage,
round(1.0 * sum(end_of_message_rate)/sum(trigger_count),4) as eom_percentage,
0.0 as click_flag_sum,
round(1.0 * sum(trigger_count)/d.max_triggered,4) as trigger_percentage,
round(1.0 * (sum(trigger_count) - sum(error_ct))/sum(trigger_count),4) as delivery_percentage,
round(1.0 * sum(call_pick)/sum(trigger_count),4) as react_percentage,
round(1.0 * sum(ivr_resp_state)/sum(trigger_count),4) as explicit_response_percentage,max_triggered
FROM 
(
SELECT loan_id, company_id, type_of_comm,
trunc(start_time::date) as "date", max(call_pick) as call_pick, 
max(call_resp) as call_resp, max(end_of_message_rate) as end_of_message_rate,
max(CASE WHEN ivr_resp_state = 1 then 1 else 0 end) as ivr_resp_state,max(trigger_count) as trigger_count,
min(error_flag) as error_flag, sum(error_flag) as error_ct
from datascience_raw.ivr_agg_data
where start_time is not null
GROUP BY loan_id, trunc(start_time::date), company_id,type_of_comm) a
LEFT JOIN
datascience_raw.tag_data b
on a.company_id = b.company_id
and a.loan_id = b.loan_id
LEFT JOIN
(SELECT company_id, count(loan_id) as max_triggered, tag_name, allocation_month
from datascience_raw.tag_data
group by tag_name, allocation_month, company_id) d
ON a.company_id = d.company_id
and b.tag_name = d.tag_name
and b.allocation_month = d.allocation_month
where "date" >= '{dt}' and "date" <= '{end_dt}' and a.company_id = '{company_id}'
GROUP BY b.tag_name,max_triggered,"date",a.type_of_comm, b.allocation_month,a.company_id )""".format(dt=dt
                                                ,company_id = company_id, end_dt = end_dt)

engine_redshift.execute(sqlalchemy.text(query))


# ## Create DateWise Channel response Table

query = """Insert INTO datascience_raw.datewise_channel_response (
SELECT company_id, allocation_month, trunc(created) as "date" ,tag_name, type_of_comm,
count(loan_id) as loan_id_count,
sum(delivery_flag) as delivery_flag_sum,
sum(trigger_count) as trigger_count_sum,
sum(opened_flag) as opened_flag_sum,
0.0 as response_percentage,
0.0 as eom_percentage,
sum(click_count) as click_flag_sum,
round(1.0 * sum(trigger_count)/max_triggered,4) as trigger_percentage,
round(1.0 * sum(delivery_flag)/sum(trigger_count),4) as delivery_percentage,
round(1.0 * sum(opened_flag)/sum(trigger_count),4) as react_percentage,
round(1.0 * sum(click_count) /sum(trigger_count),4) as explicit_response_percentage,
max_triggered
FROM (
SELECT a.*, b.tag_name, b.allocation_month,c.click_flag, d.max_triggered FROM 
(SELECT loan_id, company_id, type_of_comm,
trunc(created) as created, sum(delivery_flag) as delivery_flag,
sum(opened_flag) as opened_flag, sum(trigger_count) as trigger_count,
sum(click_count) as click_count
FROM datascience_raw.msg_agg_data
GROUP BY loan_id, company_id, type_of_comm,trunc(created)) a
LEFT JOIN
datascience_raw.tag_data b
on a.company_id = b.company_id
and a.loan_id = b.loan_id
LEFT JOIN
(SELECT company_id, loan_id, created,
CASE WHEN link_tag LIKE '%predue%_wa%' THEN 'whatsapp'
WHEN link_tag LIKE '%predue%_sms%' THEN 'sms' end as type_of_comm,
1 as click_flag from datascience_raw.click_data
where type_of_comm is not null) c
on a.company_id = c.company_id
and a.loan_id = c.loan_id
and extract(month from b.allocation_month::date) = extract(month from c.created)
and a.type_of_comm = c.type_of_comm
LEFT JOIN
(SELECT company_id, count(loan_id) as max_triggered, tag_name, allocation_month
from datascience_raw.tag_data
group by tag_name, allocation_month, company_id) d
ON a.company_id = d.company_id
and b.tag_name = d.tag_name
and b.allocation_month = d.allocation_month
 ) t
 where trunc(created) >= '{dt}' and created::date <= '{end_dt}' and company_id = '{company_id}'
group by tag_name,max_triggered, trunc(created),type_of_comm, allocation_month, company_id)""".format(dt=dt
                                                    ,company_id = company_id, end_dt = end_dt)

engine_redshift.execute(sqlalchemy.text(query))

query = """INSERT INTO datascience_raw.datewise_channel_response (
SELECT a.company_id, b.allocation_month, "date" ,b.tag_name, a.type_of_comm,
count( a.loan_id) as loan_id_count,
(sum(trigger_count) - sum(error_ct)) as delivery_flag_sum,
sum(trigger_count) as trigger_count_sum,
sum(call_pick) as opened_flag_sum,
round(1.0 * sum(call_resp)/sum(trigger_count),4)  as response_percentage,
round(1.0 * sum(end_of_message_rate)/sum(trigger_count),4)  as eom_percentage,
0.0 as click_flag_sum,
round(1.0 * sum(trigger_count)/d.max_triggered,4)  as trigger_percentage,
round(1.0 * (sum(trigger_count) - sum(error_ct))/sum(trigger_count),4)  as delivery_percentage,
round(1.0 * sum(call_pick)/sum(trigger_count),4)  as react_percentage,
round(1.0 * sum(ivr_resp_state)/sum(trigger_count),4)  as explicit_response_percentage,max_triggered
FROM 
(
SELECT loan_id, company_id, type_of_comm,
trunc(start_time::date) as "date", sum(call_pick) as call_pick, 
sum(call_resp) as call_resp, sum(end_of_message_rate) as end_of_message_rate,
sum(CASE WHEN ivr_resp_state = 1 then 1 else 0 end) as ivr_resp_state,sum(trigger_count) as trigger_count,
min(error_flag) as error_flag, sum(error_flag) as error_ct
from datascience_raw.ivr_agg_data
where start_time is not null
GROUP BY loan_id, trunc(start_time::date), company_id,type_of_comm) a
LEFT JOIN
datascience_raw.tag_data b
on a.company_id = b.company_id
and a.loan_id = b.loan_id
LEFT JOIN
(SELECT company_id, count(loan_id) as max_triggered, tag_name, allocation_month
from datascience_raw.tag_data
group by tag_name, allocation_month, company_id) d
ON a.company_id = d.company_id
and b.tag_name = d.tag_name
and b.allocation_month = d.allocation_month
where "date" >= '{dt}' and "date" <= '{end_dt}' and a.company_id = '{company_id}'
GROUP BY b.tag_name,max_triggered,"date" ,a.type_of_comm, b.allocation_month,a.company_id )""".format(dt=dt
                                                    ,company_id = company_id, end_dt=end_dt)

engine_redshift.execute(sqlalchemy.text(query))


# ## Create HourWise Channel response Table
query = """Insert INTO datascience_raw.hourwise_channel_response (
SELECT company_id, allocation_month, "date" , hours, tag_name, type_of_comm,
count(loan_id) as loan_id_count,
sum(delivery_flag) as delivery_flag_sum,
sum(trigger_count) as trigger_count_sum,
sum(opened_flag) as opened_flag_sum,
0.0 as response_percentage,
0.0 as eom_percentage,
sum(click_count) as click_flag_sum,
round(1.0 * sum(trigger_count)/max_triggered,4) as trigger_percentage,
round(1.0 * sum(delivery_flag)/sum(trigger_count),4) as delivery_percentage,
round(1.0 * sum(opened_flag)/sum(trigger_count),4) as react_percentage,
round(1.0 * sum(click_count) /sum(trigger_count),4) as explicit_response_percentage,
max_triggered
FROM (
SELECT a.*, b.tag_name, b.allocation_month, d.max_triggered FROM 
(SELECT loan_id, company_id, type_of_comm,
trunc(created) as "date", extract(hour from created) as hours, sum(delivery_flag) as delivery_flag,
sum(opened_flag) as opened_flag, sum(trigger_count) as trigger_count,
sum(click_count) as click_count FROM datascience_raw.msg_agg_data
GROUP BY loan_id, company_id, type_of_comm,trunc(created),extract(hour from created)) a
LEFT JOIN
datascience_raw.tag_data b
on a.company_id = b.company_id
and a.loan_id = b.loan_id
LEFT JOIN
(SELECT company_id, count(loan_id) as max_triggered, tag_name, allocation_month
from datascience_raw.tag_data
group by tag_name, allocation_month, company_id) d
ON a.company_id = d.company_id
and b.tag_name = d.tag_name
and b.allocation_month = d.allocation_month
 ) t
 where "date" >= '{dt}' and "date" <= '{end_dt}' and company_id = '{company_id}'
group by tag_name,max_triggered, "date", hours ,type_of_comm, allocation_month, company_id)""".format(dt=dt
                                                        ,company_id = company_id, end_dt=end_dt)

engine_redshift.execute(sqlalchemy.text(query))

query = """INSERT INTO datascience_raw.hourwise_channel_response (
SELECT a.company_id, b.allocation_month,"date", hours, b.tag_name, a.type_of_comm,
count( a.loan_id) as loan_id_count,
(sum(trigger_count) - sum(error_ct)) as delivery_flag_sum,
sum(trigger_count) as trigger_count_sum,
sum(call_pick) as opened_flag_sum,
round(1.0 * sum(call_resp)/sum(trigger_count),4) as response_percentage,
round(1.0 * sum(end_of_message_rate)/sum(trigger_count),4) as eom_percentage,
0.0 as click_flag_sum,
round(1.0 * sum(trigger_count)/d.max_triggered,4) as trigger_percentage,
round(1.0 * (sum(trigger_count) - sum(error_ct))/sum(trigger_count),4) as delivery_percentage,
round(1.0 * sum(call_pick)/sum(trigger_count),4) as react_percentage,
round(1.0 * sum(ivr_resp_state)/sum(trigger_count),4) as explicit_response_percentage,max_triggered
FROM 
(
SELECT loan_id, company_id, type_of_comm,
trunc(start_time::datetime) as "date", extract(hour from start_time::datetime) as hours, sum(call_pick) as call_pick, 
sum(call_resp) as call_resp, sum(end_of_message_rate) as end_of_message_rate,
sum(CASE WHEN ivr_resp_state = 1 then 1 else 0 end) as ivr_resp_state,sum(trigger_count) as trigger_count,
min(error_flag) as error_flag, sum(error_flag) as error_ct
from datascience_raw.ivr_agg_data
where start_time is not null
GROUP BY loan_id, trunc(start_time::datetime),extract(hour from start_time::datetime), company_id,type_of_comm) a
LEFT JOIN
datascience_raw.tag_data b
on a.company_id = b.company_id
and a.loan_id = b.loan_id
LEFT JOIN
(SELECT company_id, count(loan_id) as max_triggered, tag_name, allocation_month
from datascience_raw.tag_data
group by tag_name, allocation_month, company_id) d
ON a.company_id = d.company_id
and b.tag_name = d.tag_name
and b.allocation_month = d.allocation_month
WHERE "date" >= '{dt}' and "date" <= '{end_dt}' and a.company_id = '{company_id}'
GROUP BY b.tag_name,max_triggered,"date", hours,a.type_of_comm, b.allocation_month,a.company_id )""".format(dt=dt
                                                ,company_id = company_id, end_dt = end_dt)

engine_redshift.execute(sqlalchemy.text(query))
