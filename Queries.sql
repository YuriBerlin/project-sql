#1085 cases of ICU stays
SELECT COUNT(DISTINCT s.subject_id||s.stay_id) as cnt
FROM `physionet-data.mimiciv_hosp.diagnoses_icd` d1
INNER JOIN `physionet-data.mimiciv_icu.icustays` s
ON d1.subject_id = s.subject_id AND d1.hadm_id = s.hadm_id
WHERE (d1.icd_code LIKE '200%' OR d1.icd_code LIKE '201%' OR d1.icd_code LIKE '202%'
OR d1.icd_code LIKE 'C81%' OR d1.icd_code LIKE 'C82%'
OR d1.icd_code LIKE 'C83%' OR d1.icd_code LIKE 'C84%'
OR d1.icd_code LIKE 'C85%' OR d1.icd_code LIKE 'C86%'
OR d1.icd_code LIKE 'C88.4%');


#exploring ICU admisison source
WITH sub_subquery AS #before getting row_numbers, we should eliminate duplicated rows
(
SELECT DISTINCT tr.subject_id, tr.hadm_id, tr.eventtype, tr.careunit, tr.intime, tr.outtime
FROM `physionet-data.mimiciv_hosp.transfers` tr
INNER JOIN `physionet-data.mimiciv_hosp.diagnoses_icd` d1
  ON tr.subject_id = d1.subject_id
    AND tr.hadm_id = d1.hadm_id
INNER JOIN `physionet-data.mimiciv_icu.icustays` s
  ON d1.subject_id = s.subject_id AND d1.hadm_id = s.hadm_id
WHERE 
(   d1.icd_code LIKE '200%' OR d1.icd_code LIKE '201%' OR d1.icd_code LIKE '202%'   OR d1.icd_code LIKE 'C81%' OR d1.icd_code LIKE 'C82%'   OR d1.icd_code LIKE 'C83%' OR d1.icd_code LIKE 'C84%'   OR d1.icd_code LIKE 'C85%' OR d1.icd_code LIKE 'C86%'   OR d1.icd_code LIKE 'C88.4%' )
),
subquery AS
(
SELECT sub_subquery.*,
  ROW_NUMBER() OVER
    (
    PARTITION BY subject_id, hadm_id  
    ORDER BY intime
    ) AS row_number
FROM sub_subquery
)
SELECT sub.*, s.intime as icu_intime, s.outtime as icu_outtime,
CASE
  WHEN sub.row_number = 1 AND s.intime IS NOT NULL
  THEN 'direct admission'
  WHEN lower(sub2.careunit) LIKE '%emergency department%' AND s.intime IS NOT NULL
  THEN 'after ED'
  WHEN lower(sub2.careunit) LIKE '%hematology/oncology%' AND s.intime IS NOT NULL
  THEN 'after hem/onc dep'
  WHEN
    (
    LOWER(sub2.careunit) LIKE '%intensive care unit%' OR LOWER(sub2.careunit) LIKE '%coronary care unit%' OR LOWER(sub2.careunit) LIKE '%trauma sicu%'
    )
    AND s.intime IS NOT NULL
  THEN 'after another ICU'
  WHEN sub2.careunit IS NOT NULL AND s.intime IS NOT NULL
  THEN 'after another department'
  ELSE NULL
END AS admitted_from
FROM subquery sub
LEFT JOIN `physionet-data.mimiciv_icu.icustays` s
  ON sub.subject_id = s.subject_id AND sub.hadm_id = s.hadm_id AND sub.intime = s.intime
LEFT JOIN subquery sub2
  ON sub.subject_id = sub2.subject_id AND sub.hadm_id = sub2.hadm_id AND sub2.row_number = sub.row_number-1
ORDER BY sub.subject_id, sub.hadm_id, sub.intime;

WITH sub_subquery AS #before getting row_numbers, we should eliminate duplicated rows
(
SELECT DISTINCT tr.subject_id, tr.hadm_id, tr.eventtype, tr.careunit, tr.intime, tr.outtime
FROM `physionet-data.mimiciv_hosp.transfers` tr
INNER JOIN `physionet-data.mimiciv_hosp.diagnoses_icd` d1
  ON tr.subject_id = d1.subject_id
    AND tr.hadm_id = d1.hadm_id
INNER JOIN `physionet-data.mimiciv_icu.icustays` s
  ON d1.subject_id = s.subject_id AND d1.hadm_id = s.hadm_id
WHERE  
( d1.icd_code LIKE '200%' OR d1.icd_code LIKE '201%' OR d1.icd_code LIKE '202%'   OR d1.icd_code LIKE 'C81%' OR d1.icd_code LIKE 'C82%'   OR d1.icd_code LIKE 'C83%' OR d1.icd_code LIKE 'C84%'   OR d1.icd_code LIKE 'C85%' OR d1.icd_code LIKE 'C86%'   OR d1.icd_code LIKE 'C88.4%')
),
subquery AS
(
SELECT sub_subquery.*,
  ROW_NUMBER() OVER
    (
    PARTITION BY subject_id, hadm_id  
    ORDER BY intime
    ) AS row_number
FROM sub_subquery
),
main_query AS
(
SELECT
s.subject_id,
s.stay_id,
s.intime,
CASE
  WHEN sub.row_number = 1 AND s.intime IS NOT NULL
  THEN 'direct admission'
  WHEN lower(sub2.careunit) LIKE '%emergency department%' AND s.intime IS NOT NULL
  THEN 'after ED'
  WHEN lower(sub2.careunit) LIKE '%hematology/oncology%' AND s.intime IS NOT NULL
  THEN 'after hem/onc dep'
  WHEN
    (
    LOWER(sub2.careunit) LIKE '%intensive care unit%' OR LOWER(sub2.careunit) LIKE '%coronary care unit%' OR LOWER(sub2.careunit) LIKE '%trauma sicu%'
    )
    AND s.intime IS NOT NULL
  THEN 'after another ICU'
  WHEN sub2.careunit IS NOT NULL AND s.intime IS NOT NULL
  THEN 'after another department'
  ELSE NULL
END AS admitted_from
FROM subquery sub
LEFT JOIN `physionet-data.mimiciv_icu.icustays` s
  ON sub.subject_id = s.subject_id AND sub.hadm_id = s.hadm_id AND sub.intime = s.intime
LEFT JOIN subquery sub2
  ON sub.subject_id = sub2.subject_id AND sub.hadm_id = sub2.hadm_id AND sub2.row_number = sub.row_number-1
)
SELECT admitted_from, COUNT(*)
FROM main_query
WHERE subject_id IS NOT NULL
GROUP BY admitted_from
ORDER BY COUNT(*) DESC
;

#intermediate table creation - icu stay id to icu admission source
#DROP TABLE `icustay_admissionsource`
CREATE TABLE `icustay_admissionsource`
AS
(
  WITH sub_subquery AS #before getting row_numbers, we should eliminate duplicated rows
(
SELECT DISTINCT tr.subject_id, tr.hadm_id, tr.eventtype, tr.careunit, tr.intime, tr.outtime
FROM `physionet-data.mimiciv_hosp.transfers` tr
INNER JOIN `physionet-data.mimiciv_hosp.diagnoses_icd` d1
  ON tr.subject_id = d1.subject_id
    AND tr.hadm_id = d1.hadm_id
INNER JOIN `physionet-data.mimiciv_icu.icustays` s
  ON d1.subject_id = s.subject_id AND d1.hadm_id = s.hadm_id
WHERE
(   d1.icd_code LIKE '200%' OR d1.icd_code LIKE '201%' OR d1.icd_code LIKE '202%'   OR d1.icd_code LIKE 'C81%' OR d1.icd_code LIKE 'C82%'   OR d1.icd_code LIKE 'C83%' OR d1.icd_code LIKE 'C84%'   OR d1.icd_code LIKE 'C85%' OR d1.icd_code LIKE 'C86%'   OR d1.icd_code LIKE 'C88.4%')
),
subquery AS
(
SELECT sub_subquery.*,
  ROW_NUMBER() OVER
    (
    PARTITION BY subject_id, hadm_id  
    ORDER BY intime
    ) AS row_number
FROM sub_subquery
),
main_query AS
(
SELECT
s.subject_id,
s.stay_id,
s.intime,
CASE
  WHEN sub.row_number = 1 AND s.intime IS NOT NULL
  THEN 'direct admission'
  WHEN lower(sub2.careunit) LIKE '%emergency department%' AND s.intime IS NOT NULL
  THEN 'after ED'
  WHEN lower(sub2.careunit) LIKE '%hematology/oncology%' AND s.intime IS NOT NULL
  THEN 'after hem/onc dep'
  WHEN
    (
    LOWER(sub2.careunit) LIKE '%intensive care unit%' OR LOWER(sub2.careunit) LIKE '%coronary care unit%' OR LOWER(sub2.careunit) LIKE '%trauma sicu%'
    )
    AND s.intime IS NOT NULL
  THEN 'after another ICU'
  WHEN sub2.careunit IS NOT NULL AND s.intime IS NOT NULL
  THEN 'after another department'
  ELSE NULL
END AS admitted_from
FROM subquery sub
LEFT JOIN `physionet-data.mimiciv_icu.icustays` s
  ON sub.subject_id = s.subject_id AND sub.hadm_id = s.hadm_id AND sub.intime = s.intime
LEFT JOIN subquery sub2
  ON sub.subject_id = sub2.subject_id AND sub.hadm_id = sub2.hadm_id AND sub2.row_number = sub.row_number-1
)
SELECT subject_id, stay_id, admitted_from
FROM main_query
WHERE subject_id IS NOT NULL
);



#dead by admission category:
SELECT admitted_from, COUNT(*) from my_staging.icustay_admissionsource adms
  INNER JOIN`physionet-data.mimiciv_icu.icustays` s
    ON adms.subject_id = s.subject_id AND adms.stay_id = s.stay_id    
  INNER JOIN`physionet-data.mimiciv_hosp.patients` p
    ON s.subject_id = p.subject_id
AND DATE_DIFF(dod,CAST(SUBSTRING(CAST (outtime as STRING), 1, 10) AS DATE), day) <1
AND CAST(SUBSTRING(CAST (outtime as STRING), 1, 10) AS DATE)>=dod
GROUP BY admitted_from
ORDER BY COUNT(*) DESC;
