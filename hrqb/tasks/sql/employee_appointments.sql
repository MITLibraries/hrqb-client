/*
Query for Employee Appointment data.

This query will also provide unique values for lookup tables like Job Titles and Job
Positions as examples.

Notes:
    - column HR_APPT_KEY: unique identifier for Quickbase 'Employee Appointments' table

CHANGELOG
    - 2024-05-13 Query created and added
    - 2024-06-03 Limit rows to appointments that end on or before 2019-01-01
    - 2024-06-05 Do not filter on benefits types
*/

select distinct
    a.HR_APPT_KEY,
    a.MIT_ID,
    ou.HR_ORG_UNIT_TITLE,
    a.APPT_BEGIN_DATE,
    a.APPT_END_DATE,
    a.IS_MOST_RECENT_APPT,
    j.HR_JOB_KEY,
    j.JOB_ID,
    j.JOB_TITLE_LONG,
    j.EMPLOYEE_GROUP,
    case
        WHEN j.EMPLOYEE_GROUP = 'Exempt' THEN 'E'
        WHEN j.EMPLOYEE_GROUP = 'Non-Exempt' THEN 'NE'
        ELSE 'TBD'
    end as EXEMPT,
    j.JOB_FAMILY,
    j.JOB_SUBFAMILY,
    CASE
       WHEN REGEXP_LIKE (j.JOB_TRACK, 'individual', 'i') THEN 'Individual'
       WHEN REGEXP_LIKE (j.JOB_TRACK, 'management', 'i') THEN 'Management'
    END AS JOB_TRACK,
    j.PAY_GRADE,
    p.HR_POSITION_KEY,
    p.POSITION_ID,
    p.POSITION_TITLE_LONG,
    p.PERSONNEL_SUBAREA as EMPLOYEE_TYPE,
    case
        when p.PERSONNEL_SUBAREA = 'Service AFSCME' then 'AFSCME'
        when p.PERSONNEL_SUBAREA = 'Inst Off-NonFac' then null
        when p.PERSONNEL_SUBAREA = 'Spon Res-Adm' then null
        else p.PERSONNEL_SUBAREA
    end as UNION_NAME,
    case
        when (
            a.APPT_END_DATE > (SELECT TRUNC(SYSDATE) FROM dual)
            and a.APPT_END_DATE < TO_DATE('2999-12-30', 'YYYY-MM-DD')
        ) then 'Term'
        else 'Permanent'
    end as TERM_OR_PERM,
    bec.ELIG_PROGRAM_GROUP_1 as BENEFITS_GROUP_TYPE
from HR_APPOINTMENT_DETAIL a
inner join HR_ORG_UNIT ou on ou.HR_ORG_UNIT_KEY = a.HR_ORG_UNIT_KEY
left join HR_JOB j on j.HR_JOB_KEY = a.HR_JOB_KEY
left join HR_POSITION p on p.HR_POSITION_KEY = a.HR_POSITION_KEY
left join BENEFITS_ELIGIBILITY_CURRENT bec on
    bec.MIT_ID = a.MIT_ID
    and a.PERSONNEL_KEY = bec.PERSONNEL_KEY
where a.APPT_END_DATE >= TO_DATE('2019-01-01', 'YYYY-MM-DD')
order by a.MIT_ID, a.APPT_BEGIN_DATE, a.APPT_END_DATE