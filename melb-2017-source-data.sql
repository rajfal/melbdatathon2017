/*------------------------------------------
steps to extract data of interest

1. find out from transactions table which drugs have been purchased by patients, 
   that cost in excess of $200 per order
   
2. once purchasers of those expensive meds are found, also find their entire
   drug purchasing history - link that to location and some idea of pathology
 
3. look at a time series graph to follow patient's frequency, type and cost of
   med spending
   
4. where are the big spends likely to occur - it would be nice to do this by age
   and gender, but that appears to be speculative at best, given the inconsistent data
   
5. what pathologies are likely to require high drug expenditure; is that cost
   sustainable or are people quitting and/or switching to alternative meds.
   Admittedly, most of the cost is borne by taxpayers  

notes:
ensure that patients.patient_id and stores.store_id have same data types as in transactions table      
------------------------------------------*/

#----------------------- prep -------------------------------------------------
# create an index on this ~60M record table
CREATE INDEX patient_id_index ON transactions (Patient_ID);
CREATE INDEX drug_id_index ON transactions (Drug_ID);
#CREATE INDEX store_id_index ON transactions (Store_ID);
CREATE INDEX patient_id_index ON patients (Patient_ID);
CREATE INDEX store_id_index ON stores (Store_ID);
CREATE INDEX drug_id_index ON chronic_illness_lookup (MasterProductID);
## result: DONE

#----------------------- step 1 -------------------------------------------------
# get record id's only of meds actually sold
CREATE TABLE t_drugs_in_use
SELECT distinct Drug_ID
FROM transactions;
## result: 6690 records in 22sec

#----------------------- step 2 -------------------------------------------------
# add more info against each med actually sold
DROP TABLE t_drugs_in_use_info;
CREATE TABLE t_drugs_in_use_info
select distinct ifnull(k.ChronicIllness,'not specified') as Chronic_Illness, 
     c.ATCLevel1Name,c.ATCLevel2Name, c.ATCLevel3Name,c.ATCLevel4Name,c.ATCLevel5Name,d.* 
from atc_lookup as c
  right join t_drugs_in_use_details as d
  on c.ATCLevel5Code = d.ATCLevel5Code
  
  left join chronic_illness_lookup as k
  on d.MasterProductID = k.MasterProductID
  
  order by Chronic_Illness;
## result: 6690 records in 5.6sec  

#----------------------- step 2.1 -------------------------------------------------  
SELECT * FROM t_drugs_in_use_info INTO OUTFILE '/var/lib/mysql-files/t_drugs_in_use_info.txt'  FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
## result: 6690 records in 0.07sec

#----------------------- explore -------------------------------------------------
# create a basic frequency distribution table  
select  count(case when ChemistListPrice between 0 and 10 then 1 end) as '$0-10'
,       count(case when ChemistListPrice between 11 and 30 then 1 end) as '$11-30'
,       count(case when ChemistListPrice between 31 and 50 then 1 end) as '$31-50'
,       count(case when ChemistListPrice between 51 and 199 then 1 end) as '$51-199'
,       count(case when ChemistListPrice between 200 and 499 then 1 end) as '$200-499'
,       count(case when ChemistListPrice between 500 and 999 then 1 end) as '$500-999'
,       count(case when ChemistListPrice between 1000 and 4999 then 1 end) as '$1000-4999'
,       count(case when ChemistListPrice between 5000 and 9999 then 1 end) as '$5000-9999'
,       count(case when ChemistListPrice between 10000 and 99999 then 1 end) as '$10000-99999'
,       sum(case when ChemistListPrice between 0 and 99999 then 1 end) as 'total count'
,       sum(case when ChemistListPrice between 200 and 99999 then 1 end) as 'count hi-end drugs'
from    t_drugs_in_use_info;
## result: 1 record in 0.005sec 
  
#----------------------- step 3 -------------------------------------------------
# create a subset of drugs, where price per item >=$200
CREATE TABLE t_drugs_hi_end_info
select d.* 
from t_drugs_in_use_info as d  
  where d.ChemistListPrice > 199  
  order by d.MasterProductID;
## result: 419 records in 0.09sec   

#----------------------- step 3.1 -------------------------------------------------  
SELECT * FROM t_drugs_hi_end_info INTO OUTFILE '/var/lib/mysql-files/t_drugs_hi_end_info.txt'  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
## result: 419 records in 0.03sec

  
#----------------------- step 4 -------------------------------------------------
# select patients that have bought a high end medication, at least once 
CREATE TABLE t_drugs_hi_end_patients
select distinct t.Patient_ID
  from transactions as t
  inner join t_drugs_hi_end_info as x
  on t.Drug_ID = x.MasterProductID
  order by t.Patient_ID;
## result: 14007 records in 0.55sec   

#----------------------- explore -------------------------------------------------
# get no. transactions by patients in t_drugs_hi_end_patients   
select count(t.Patient_ID)
  from transactions as t
  inner join t_drugs_hi_end_patients as x
  on t.Patient_ID = x.Patient_ID;  
## result: 1759946 records in 0.37sec  
  
#----------------------- step 5 -------------------------------------------------
# select ALL of the transactions by patients that have bought a high end medication, at least once     

CREATE TABLE t_filled_prescriptions
select t.Patient_ID, Store_ID, Prescriber_ID, Drug_ID, SourceSystem_Code, Prescription_Week, Dispense_Week, Drug_Code, NHS_Code, IsDeferredScript, Script_Qty,
       Dispensed_Qty, MaxDispense_Qty, PatientPrice_Amt, WholeSalePrice_Amt, GovernmentReclaim_Amt, RepeatsTotal_Qty, RepeatsLeft_Qty, StreamlinedApproval_Code
  from transactions as t
  inner join t_drugs_hi_end_patients as x
  on t.Patient_ID = x.Patient_ID;  
## result: 1759946 records in 11.37sec  

#----------------------- step 6 -------------------------------------------------
# add postcode field to table 
ALTER TABLE t_drugs_hi_end_patients ADD postcode VARCHAR(255);  
## result: 0 records in 0.39sec 


#----------------------- step 7 -------------------------------------------------
# select ALL of the transactions by patients that have bought a high end medication, at least once 
# with attached state and postcodes for patient and pharmacy
DROP TABLE t_filled_prescriptions;
CREATE TABLE t_filled_prescriptions_exp
select t.Patient_ID, 
       p.postcode as Pt_postcode, 
       Prescriber_ID, Drug_ID, 
       t.Store_ID as Store_ID, 
       s.StateCode as Str_state, s.postcode as Str_postcode,
       SourceSystem_Code, Prescription_Week, Dispense_Week, Drug_Code, NHS_Code, IsDeferredScript, Script_Qty,
       Dispensed_Qty, MaxDispense_Qty, PatientPrice_Amt, WholeSalePrice_Amt, GovernmentReclaim_Amt, RepeatsTotal_Qty, RepeatsLeft_Qty, StreamlinedApproval_Code
  from t_filled_prescriptions as t
  
  inner join patients as p
  on t.Patient_ID = p.Patient_ID
  
  inner join stores as s
  on t.Store_ID = s.Store_ID;
  #where t.Patient_ID IN(13,27,71,157,163,183,227,237,262,291);
## result: 1759946 records in 23.37sec  

#----------------------- step 7.1 -------------------------------------------------  
SELECT * FROM t_filled_prescriptions_exp INTO OUTFILE '/var/lib/mysql-files/t_filled_prescriptions_exp.txt'  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
## result: 1759946 records in 2.7sec
  
  
  
#----------------------- to be refinded a bit more-------------------------------
#                     DEPRESSION RELATED RECORDS
#----------------------- step D1.0 -------------------------------------------------  
# list of drugs prescribed for Depression
DROP TABLE d_drug_list;
CREATE TABLE d_drug_list
select distinct MasterProductID from t_drugs_in_use_info
where Chronic_Illness like 'Dep%'
order by MasterProductID;
## result: 337 records in 0.054sec

#----------------------- step D2.0 -------------------------------------------------
# select ALL of the transactions related to anti-Depression medication,
# with attached state and postcodes for patient and pharmacy
DROP TABLE d_filled_prescriptions;

#----------------------- step D2.1 -------------------------------------------------
CREATE TABLE d_filled_prescriptions_1
select t.Patient_ID, 
       #p.postcode as Pt_postcode, 
       Prescriber_ID, Drug_ID, 
       t.Store_ID as Store_ID, 
       #s.StateCode as Str_state, s.postcode as Str_postcode,
       SourceSystem_Code, Prescription_Week, Dispense_Week, Drug_Code, NHS_Code, IsDeferredScript, Script_Qty,
       Dispensed_Qty, MaxDispense_Qty, PatientPrice_Amt, WholeSalePrice_Amt, GovernmentReclaim_Amt, RepeatsTotal_Qty, RepeatsLeft_Qty, StreamlinedApproval_Code
  from transactions as t
  
  inner join d_drug_list as d
  on t.Drug_ID = d.MasterProductID
## result: 4439029 records in 28.74sec  

#test counts  
Select distinct Drug_ID from d_filled_prescriptions_1; #337 records
Select distinct Patient_ID from d_filled_prescriptions_1; #191338 records
Select distinct Patient_ID from d_patients; #191338 records

#----------------------- step D2.2 -------------------------------------------------

CREATE INDEX patient_id_index ON d_filled_prescriptions_1 (Patient_ID);

CREATE TABLE d_filled_prescriptions_2
select t.Patient_ID, 
       p.postcode as Pt_postcode, 
       Prescriber_ID, Drug_ID, 
       t.Store_ID as Store_ID, 
       #s.StateCode as Str_state, s.postcode as Str_postcode,
       SourceSystem_Code, Prescription_Week, Dispense_Week, Drug_Code, NHS_Code, IsDeferredScript, Script_Qty,
       Dispensed_Qty, MaxDispense_Qty, PatientPrice_Amt, WholeSalePrice_Amt, GovernmentReclaim_Amt, RepeatsTotal_Qty, RepeatsLeft_Qty, StreamlinedApproval_Code
  from d_filled_prescriptions_1 as t
   
  inner join d_patients as p
  on t.Patient_ID = p.Patient_ID
## result: 4439029 records in 30.02sec
  
Select count(*) from d_filled_prescriptions_2; #337 records  
Select * from d_filled_prescriptions_2
Limit 100;  

#----------------------- step D2.3 -------------------------------------------------

CREATE TABLE d_filled_prescriptions
select t.Patient_ID, 
       Pt_postcode, 
       Prescriber_ID, Drug_ID, 
       t.Store_ID as Store_ID, 
       s.StateCode as Str_state, s.postcode as Str_postcode,
       SourceSystem_Code, Prescription_Week, Dispense_Week, Drug_Code, NHS_Code, IsDeferredScript, Script_Qty,
       Dispensed_Qty, MaxDispense_Qty, PatientPrice_Amt, WholeSalePrice_Amt, GovernmentReclaim_Amt, RepeatsTotal_Qty, RepeatsLeft_Qty, StreamlinedApproval_Code
  from d_filled_prescriptions_2 as t
  
  inner join stores as s
  on t.Store_ID = s.Store_ID
## result: 4439029 records in 27.54sec

Select count(*) from d_filled_prescriptions; #337 records  
Select * from d_filled_prescriptions
Limit 100;  

#----------------------- step D3.0 -------------------------------------------------  
SELECT * FROM d_filled_prescriptions INTO OUTFILE '/var/lib/mysql-files/d_filled_prescriptions.txt'  FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
## result: 4439029 records in 7.7sec
  

DROP TABLE d_filled_prescriptions_1;
DROP TABLE d_filled_prescriptions_2;











## ----- SCRAP THIS: ---------------------------------------------------------------


#----------------------- step D2.0 -------------------------------------------------
# select patients that have bought an anti-Depression medication, at least once 
DROP TABLE d_patients
CREATE TABLE d_patients
select distinct t.Patient_ID, y.postcode
  from transactions as t
  inner join d_drug_list as x
  on t.Drug_ID = x.MasterProductID
  inner join patients as y
  on t.Patient_ID = y.Patient_ID
  
  order by t.Patient_ID;
## result: 191338 records in 27.43sec     

#----------------------- step D2.0 -------------------------------------------------
CREATE INDEX patient_id_index ON d_patients (Patient_ID);
## result: 191338 records in 27.43sec

#----------------------- step D3.0 -------------------------------------------------
# select ALL of the transactions by patients that have bought anti-Depression medication, at least once 
# with attached state and postcodes for patient and pharmacy
DROP TABLE d_filled_prescriptions;
CREATE TABLE d_filled_prescriptions
select t.Patient_ID, 
       p.postcode as Pt_postcode, 
       Prescriber_ID, Drug_ID, 
       t.Store_ID as Store_ID, 
       s.StateCode as Str_state, s.postcode as Str_postcode,
       SourceSystem_Code, Prescription_Week, Dispense_Week, Drug_Code, NHS_Code, IsDeferredScript, Script_Qty,
       Dispensed_Qty, MaxDispense_Qty, PatientPrice_Amt, WholeSalePrice_Amt, GovernmentReclaim_Amt, RepeatsTotal_Qty, RepeatsLeft_Qty, StreamlinedApproval_Code
  from transactions as t
  
  inner join d_patients as p
  on t.Patient_ID = p.Patient_ID
  
  inner join stores as s
  on t.Store_ID = s.Store_ID
  
  INTO OUTFILE '/var/lib/mysql-files/d_filled_prescriptions.txt'  FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
  #where t.Patient_ID IN(13,27,71,157,163,183,227,237,262,291);
## result: 22105190 records in 45.37sec 
## CRASH CRASH CRASH when exporting data....REVIEW>>


#----------------------- step D4.0 -------------------------------------------------
# select ALL of the transactions by patients that have bought anti-Depression medication, at least once 
# these records contain only the patients on anti-depressants that may have also bought other medication
create table d_all_patient_transactions 
select i.* from d_illness_specific_transactions as i inner join d_patients as d on i.Patient_ID=d.Patient_ID;

#Query OK, 12385123 rows affected (3 min 38.83 sec)
#Records: 12385123  Duplicates: 0  Warnings: 0

#----------------------- step D5.0 -------------------------------------------------
# summarize all prescription for the period by patient, chronic_illness and count
create table d_all_patient_trans_summary select Patient_ID, Chronic_Illness, count(Chronic_Illness) as No_prescriptions  from d_all_patient_transactions  where (Dispense_Year between 2010 and 2015)  group by Patient_ID, Chronic_Illness ;
#Query OK, 542915 rows affected (1 min 16.38 sec)
#Records: 542915  Duplicates: 0  Warnings: 0


#----------------------- step D6.0 -------------------------------------------------
# summarize all prescription use by patient and individual chronic_illness count
create table d_patient_illness_summary 
select c.Patient_ID, count(*) as trans_all, sum(case when c.Chronic_Illness='Hypertension' then 1 else 0 end) as HPT, sum(case when c.Chronic_Illness='Lipids' then 1 else 0 end) as LPD, 
sum(case when c.Chronic_Illness='Diabetes' then 1 else 0 end) as DBT, sum(case when c.Chronic_Illness='Epilepsy' then 1 else 0 end) as EPL, 
sum(case when c.Chronic_Illness='Depression' then 1  else 0 end) as DEP, sum(case when c.Chronic_Illness='Osteoporosis' then 1 else 0 end) as OST, 
sum(case when c.Chronic_Illness='Heart Failure' then 1 else 0 end) as HFR, sum(case when c.Chronic_Illness='Anti-Coagulant' then 1 else 0 end) as ACG, 
sum(case when c.Chronic_Illness='Immunology' then 1 else 0 end) as IMM, sum(case when c.Chronic_Illness='Urology' then 1 else 0 end) as URO, 
sum(case when c.Chronic_Illness='Chronic Obstructive Pulmonary Disease (COPD)' then 1 else 0 end) as CPD from d_all_patient_trans_summary as c group by 1;
#Query OK, 190551 rows affected (3.93 sec)


#----------------------- step D7.0 -------------------------------------------------
# summarize all prescription use by anti-depressant and any other associated medication
create table d_med_use_patterns select Patient_ID, (case when DEP>0 then 1 else 0 end) as DEP, (case when HPT>0 then 1 else 0 end) as HPT, 
(case when LPD>0 then 1 else 0 end) as LPD, (case when DBT>0 then 1 else 0 end) as DBT,  (case when EPL>0 then 1 else 0 end) as EPL, 
(case when OST>0 then 1 else 0 end) as OST, (case when HFR>0 then 1 else 0 end) as HFR, (case when ACG>0 then 1 else 0 end) as ACG, 
(case when IMM>0 then 1 else 0 end) as IMM,(case when URO>0 then 1 else 0 end) as URO, (case when CPD>0 then 1 else 0 end) as CPD from d_patient_illness_summary;
#Query OK, 190551 rows affected (1.26 sec)
#Records: 190551  Duplicates: 0  Warnings: 0


select distinct count(*) as No_occurrences, DEP, HPT, LPD, DBT, EPL, OST, HFR, ACG, IMM, URO, CPD 
from d_med_use_patterns where DEP>0
group by DEP, HPT, LPD, DBT, EPL, OST, HFR, ACG, IMM, URO, CPD 
order by 1 DESC
limit 50;

select distinct count(*) as No_occurrences, (case when DEP=1 then 'x' else 0 end) as DEP, (case when HPT=1 then 'x' else '' end) as  HPT, 
(case when LPD=1 then 'x' else '' end) as LPD, (case when DBT=1 then 'x' else '' end) as DBT, (case when EPL=1 then 'x' else '' end) as EPL, 
(case when OST=1 then 'x' else '' end) as OST, (case when HFR=1 then 'x' else '' end) as HFR 
from d_med_use_patterns where DEP>0 group by DEP, HPT, LPD, DBT, EPL, OST, HFR 
order by 1 DESC Limit 30;

select distinct ((count(*)/190551)*100) as No_occurrences, (case when DEP=1 then 'x' else 0 end) as DEP, (case when HPT=1 then 'x' else '' end) as  HPT, 
(case when LPD=1 then 'x' else '' end) as LPD, (case when DBT=1 then 'x' else '' end) as DBT, (case when EPL=1 then 'x' else '' end) as EPL, 
(case when OST=1 then 'x' else '' end) as OST, (case when HFR=1 then 'x' else '' end) as HFR 
from d_med_use_patterns where DEP>0 group by DEP, HPT, LPD, DBT, EPL, OST, HFR 
order by 1 DESC Limit 20;

#remove OST
#=>>> summ across categories to see what is commonly associated with DEP

+----------------+-----+-----+-----+-----+-----+-----+
|  %_occurrences | DEP | HPT | LPD | DBT | EPL | HFR |
+----------------+-----+-----+-----+-----+-----+-----+
|        20.5782 | x   |     |     |     |     |     |
|        11.9097 | x   |     | x   |     |     |     |
|         8.4114 | x   | x   | x   |     |     |     |
|         6.2960 | x   | x   |     |     |     |     |
|         5.4851 | x   |     | x   |     |     | x   |
|         5.2133 | x   |     |     |     | x   |     |
|         5.1582 | x   | x   | x   |     |     | x   |
|         3.8315 | x   |     | x   | x   |     |     |
|         3.7570 | x   | x   | x   | x   |     |     |
|         2.8638 | x   |     |     |     |     | x   |
|         2.8570 | x   | x   | x   | x   |     | x   |
|         2.7741 | x   |     | x   |     | x   |     |
|         2.1559 | x   |     | x   | x   |     | x   |
|         1.9496 | x   | x   |     |     |     | x   |
|         1.8447 | x   | x   | x   |     | x   |     |
|         1.3225 | x   |     | x   |     | x   | x   |
|         1.2779 | x   | x   |     |     | x   |     |
|         1.2516 | x   | x   | x   |     | x   | x   |
|         1.1955 | x   |     |     | x   |     |     |
|         1.0512 | x   |     | x   | x   | x   |     |
+----------------+-----+-----+-----+-----+-----+-----+
20 rows in set, 6 warnings (0.18 sec)

# ________________________________________INCOMPLETE_____________________________ 
  
  select count(*) from d_patients;
select * from drug_lookup
order by MasterProductID
limit 10;
  select * from t_drugs_hi_end_patients
  limit 100;


CREATE INDEX patient_id_idx ON patients (Patient_ID);

Select * from patients as p
inner join t_drugs_hi_end_patients  as t
on p.Patient_ID = t.Patient_ID;  
  
  
  
SET SQL_SAFE_UPDATES=0;
UPDATE t_drugs_hi_end_patients  as t, patients as p
SET t.postcode = p.postcode
WHERE p.Patient_ID = t.Patient_ID;



SET SQL_SAFE_UPDATES=0;
update t_drugs_hi_end_patients t
join patients p on t.Patient_ID=p.Patient_ID
set t.postcode = p.postcode

