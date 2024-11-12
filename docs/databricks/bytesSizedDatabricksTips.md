#Automate background performance improvements with Predictive Optimization
Settings -> Feature Enablement -> Predictive Optimization (Select 'Enabled')

And in notebook run below command : 
alter database dbdemos.fsi_credit_decisioning
enable predictive optimization;

Next to verify the same : 
select * from system.storage.predictive_optimization_operations_history


#Serverless for notebook
- provisioning is faster
- performance is better 
- cost optimization

python on serverless



Data Exploration : 
https://www.youtube.com/watch?v=I_J6-CIJEUY&list=PLTPXxbhUt-YWQ5hBZSMiovFZ0jxtDq47U&index=7
https://docs.databricks.com/en/genie/index.html


#How to enforce data quality across columns within a table in Databricks
Create or replace table fraud_detection_customer_dates(
last_activity_datetime TIMESTAMP, --original date columns
last_activity_date as DATE GENERATED ALWAYS AS (CAST (last_activity_datetime as DATE)), --going to be our new partition columns
last_activity_date_hour INT GENERATED ALWAYS AS (HOUR (last_activity_datetime)),
last_activity_date_day INT GENERATED ALWAYS AS (DAY (last_activity_datetime)),
last_activity_date_month INT GENERATED ALWAYS AS (MONTH (last_activity_datetime)),
last_activity_date_year INT GENERATED ALWAYS AS (YEAR (last_activity_datetime))
)
PARTITIONED BY (last_activity_date);

INSERT INTO fraud_detection_customer_dates
(last_activity_datetime)
select to_timestamp(last_activity_datetime, 'dd-MM-yyyy' HH:mm:ss') AS last_activity_datetime --only a single columns
from fraud_detection_customers;

select * from fraud_detection_customer_dates;

show partitions fraud_detection_customer_dates;

###ALWAYS ensure that each partition should be of 1 gigabyte in size

*** create or replace table fraud_detection_customer_emails (
email STRING,
username STRING GENERATED ALWAYS AS (substring (email,0,instr(email,"@")-1)),
mail_server STRING GENERATED ALWAYS AS (substring{(email, instr(email,"@")+1, instr(email,".")-instr(email,"@")-1)),
domain STRING GENERATED ALWAYS AS (substring{(email,   instr(email,".")+1,5))
)


insert into fraud_detection_customer_emails(email)
select email from  fraud_detection_customers;

#Make your records Unique with Generated Identity Columns
create or replace table id_always(
identity_always BIGINT GENERATED ALWAYS AS IDENTITY,
membership_no STRING,
country STRING,
age_group TINYINT
);

INSERT INTO id_always(membership_no, country, age_group)
select membership_no, country,age_group
from fraud_detection_customer_dates
where country = 'ALB' --went here for XMAS;

select * from id_always;


--
create or replace table id_default(
identity_default BIGINT GENERATED ALWAYS AS IDENTITY (START 
WITH 10000 INCREMENT BY 10),
membership_no STRING,
country STRING,
age_group TINYINT
);

#Create Amazing Databricks Demos with DB Demos

%pip install dbdemos
import dbdemos
dbdemos.list.demos()
dbdemos.install('llm-fine-tuning')

#Monitor Databricks Usage like a pro with System Tables

select cluster_id, driver_node_type, worker_node_type,
worker_count from system.compute.clusters
where dbr_version like "%gpu%";

or 

select g.cluster_id, sku_name, round(sum(b.usage_quantity),2) as DBUs from 
gpu_clusters g
inner join (select usage_metadata.cluster_id,
usage_quantity,
sku_name
from system.billing.usage_metadata 
where usage_date >= current_date()-30) b
on g.cluster_id = b.cluster_id

--save previous results as gpu_dbus

select cluster_id, DBUs, round(DBUs*default,2) as dollors
from gpu_dbus d
inner join (select sku_name, pricing.default from system.billing.list_prices
where price_end_time is null) p
on d.sku_name = p.sku_name
order by dollors desc

#Turn your strings into SQL with Execute Immediate
declare or replace sql_string string;
set var sql_string =
'select identifier(:column) from
demo_sample_data.lakeview.raw_claim
where claim_amount.total > :claim_amt
and suspicious_Activity = :sus
';

execute immediate sql_string
using ('policy_no' as column,
'true' as sus
, 10000 as claim_amt);


INTO example 
declare or replace priority_policy_no string;
declare or replace priority_claim_no string;
set var sql_string =
'select identifier(:column1), identifier(:column2) from
demo_sample_data.lakeview.raw_claim
where suspicious_Activity = :sus
order by claim_amount.total desc
limit 1
';

execute immediate sql_string
into priority_policy_no, priority_claim_no
using ('policy_no' as column1,
'claim_no' as column2,
'true' as sus);

values(priority_policy_no, priority_claim_no);

select * from demo_sample_data.lakeview.claim_policy
where policy_no = priority_policy_no
or claim_no = priority_claim_no;


#The easiest way to stream your data into Delta
(spark.readStream
.format("cloudFiles") #this is what makes this autoloader
.option("cloudFiles.format", "json") #specify file type
.option("cloudFiles.schemaLocation", "dbfs:/user/holly/raw")
.load("dbfs:/mnt/dbacademy-datasets/data-engineer-learning-path/v04/ecommerce/raw/events-kafka") #yes I borrowed this from training team
.writeStream
.option("checkpointLocation", "dbfs:/user/holly/raw_test")
# save to somewhere in case of failure
.toTable("autoloader.demo_db.raw_events") #catalog.db.table
)

create or refresh streaming table raw_events as 
select * from stream read_files('dbfs:/mnt/dbacademt-datasets/data-engineer-learning-path/v04/ecommerce/raw/events-kafka')

#How to be efficient processing arrays
select device_id, date, resting_heartrates_array,
count(resting_heartrate) as resting_heartrates_measurement
--this count causes a shuffle
from 
	(Select device_id, date, resting_heartrates_array,
	explode (resting_heartrates_array) as resting_heartrate
	-- explode create ones row per value
	from demo_sample_data.array.heartrates)
	group by 1,2,3 -- don't judge me 
	
INSTEAD DO THIS : 
select device_id, date, resting_heartrates_array,
aggregate(resting_heartrates_array, -- which col to aggregate 
0, --which array item to start at 
(acc,x) => (acc + 1) -- this is our lamda function
--acc is the accumulator
--x is our array item
-- we'are saying for each x, add 1 to our accumulator
) as resting_heartrates_measurements
from demo_sample_data.arrays.heartrates

	
OTHER MANIPULATIONS
select device_id, date, resting_heartrate_array,
filter(resting_heartrates_array, x -> x > 90) as high_heartrates,
exists (resting_heartrate_array, x -> x > 90 == 1) as 
high_heartrates_ind
from demo_sample_data.arrays.heartrates


#Increase your column sizes without rewriting the entire table
alter table hive_metastore.default.iot_gen set tblproperties
('delta.enableTypeWidening' = 'true')

alter table hive_metastore.default.iot_gen
alter column sku TYPE bigint;

alter table hive_metastore.default.iot_gen
alter column reading_0 type double;

alter table hive_metastore.default.iot_gen
alter column event_dt type timestamp_ntz;






