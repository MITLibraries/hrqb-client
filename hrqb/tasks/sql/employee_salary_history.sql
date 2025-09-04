/*
Query for Employee Salary History data.

Notes:
    - column HR_APPT_TX_KEY: unique identifier for Quickbase 'Employee Salary History' table
    - limited to related employee appointments ending on or after 2019-01-01

CHANGELOG
    - 2024-05-31 Query created and added
    - 2024-06-03 Date limit to appointments ending after 2019-01-01
    - 2024-07-24 Added appointment begin/end date to help match appointments
    - 2024-09-18 Omit any HR_PERSONNEL_ACTION_TYPE rows where type is "Salary Supplement"
    - 2025-02-05 Remove 2019-01-01 date cutoff entirely
    - 2025-09-04 Omit any HR_ACTION_REASON rows where reason is "Lump Sum"
*/

select distinct
    a.HR_APPT_KEY,
    a.HR_APPT_TX_KEY,
    a.MIT_ID,
    j.JOB_ID,
    p.POSITION_ID,
    appt.APPT_BEGIN_DATE,
    appt.APPT_END_DATE,
    a.APPT_TX_BEGIN_DATE as START_DATE,
    a.APPT_TX_END_DATE as END_DATE,
    at.HR_PERSONNEL_ACTION_TYPE_KEY,
    at.HR_PERSONNEL_ACTION,
    at.HR_ACTION_REASON,
    a.ORIGINAL_BASE_AMOUNT,
    a.ORIGINAL_HOURLY_RATE,
    a.ORIGINAL_EMPLOYMENT_PERCENT as ORIGINAL_EFFORT,
    a.TEMP_CHANGE_BASE_AMOUNT,
    a.TEMP_CHANGE_HOURLY_RATE,
    a.TEMP_CHANGE_EMPLOYMENT_PERCENT as TEMP_EFFORT,
    case
        when a.TEMP_CHANGE_BASE_AMOUNT > 0 then
            round(a.TEMP_CHANGE_BASE_AMOUNT / a.ORIGINAL_BASE_AMOUNT, 2) - 1.0
        else 0
    end as TEMP_BASE_CHANGE_PERCENT,
    case
        when j.JOB_ID = '00000000' then 'Y'
        else 'N'
    end as SPECIAL_ONETIME_PAY
from HR_APPT_TX_DETAIL a
inner join HR_APPOINTMENT_DETAIL appt on appt.HR_APPT_KEY = a.HR_APPT_KEY
left join HR_PERSONNEL_ACTION_TYPE at on at.HR_PERSONNEL_ACTION_TYPE_KEY = a.HR_PERSONNEL_ACTION_TYPE_KEY
left join HR_JOB j on j.HR_JOB_KEY = a.HR_JOB_KEY
left join HR_POSITION p on p.HR_POSITION_KEY = a.HR_POSITION_KEY
where at.HR_PERSONNEL_ACTION not in ('Salary Supplement')
and at.HR_ACTION_REASON not in ('Lump Sum')
order by a.MIT_ID, a.APPT_TX_BEGIN_DATE, a.APPT_TX_END_DATE
