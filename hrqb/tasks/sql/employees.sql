/*
Query for Employee data.

CHANGELOG
    - 2024-05-13 Query created and added
    - 2024-07-09 Added CTEs to gather last appointment transaction, to get termination
        or retirement reason
    - 2024-10-07 Use table HR_APPT_ACTION_DETAIL vs HR_APPT_TX_DETAIL for termination
        details
    - 2024-10-07 Add employee ethnicity to query
    - 2024-10-08
        - reworked CTEs to provide details about last library appointment only
        - move > 2019 filtering to HR_PERSON_EMPLOYEE in ordered_lib_appt MIT_IDs
    - 2025-02-05 Remove 2019-01-01 date cutoff entirely
    - 2025-02-05 Add CTE 'last_appt_termination_txn' to ensure only the last termination
        reason is included to avoid duplicating employee rows
    - 2025-03-25 Include Union Seniority Date and Gender in query
*/

-- get all library appointments for employee, ordered
with ordered_lib_appt as (
    select
        HR_APPT_KEY,
        HR_POSITION_KEY,
        MIT_ID,
        APPT_BEGIN_DATE,
        APPT_END_DATE,
        row_number() over (
            partition by MIT_ID
            order by APPT_BEGIN_DATE desc, APPT_END_DATE desc
        ) as appt_row_num
    from HR_APPOINTMENT_DETAIL
),
-- select only the last / current appointment for employee
last_lib_appt as (
    select *
    from ordered_lib_appt
    where appt_row_num = 1
),
-- get all appointment actions that are related to retirement or termination
appt_termination_txns as (
    select
        ad.MIT_ID,
        ad.HR_POSITION_KEY,
        at.HR_PERSONNEL_ACTION,
        at.HR_ACTION_REASON as TERMINATION_REASON,
        row_number() over (
            partition by MIT_ID
            order by APPT_BEGIN_DATE desc, APPT_END_DATE desc
        ) as termination_txn_row_num
    from HR_APPT_ACTION_DETAIL ad
    left join HR_PERSONNEL_ACTION_TYPE at on at.HR_PERSONNEL_ACTION_TYPE_KEY = ad.HR_PERSONNEL_ACTION_TYPE_KEY
    where at.HR_PERSONNEL_ACTION in ('Retirement','Termination')
),
last_appt_termination_txn as (
    select * from appt_termination_txns
    where termination_txn_row_num = 1
),
-- combine CTEs above to get last / current appointment end date and termination reason
last_lib_appt_details as (
    select
    lla.MIT_ID,
    lla.APPT_END_DATE as LAST_LIB_APPT_END_DATE,
    att.TERMINATION_REASON
    from last_lib_appt lla
    left join last_appt_termination_txn att on (
        att.MIT_ID = lla.MIT_ID
        and att.HR_POSITION_KEY = lla.HR_POSITION_KEY
    )
)
select
    e.MIT_ID,
    e.FIRST_NAME,
    e.LAST_NAME,
    e.NAME_KNOWN_BY as PREFERRED_NAME,
    e.DATE_OF_BIRTH,
    e.ORIGINAL_HIRE_DATE AS MIT_HIRE_DATE,
    e.UNION_SENIORITY_DATE,
    e.CURRENT_EMPLOYMENT_DATE as MIT_LIB_HIRE_DATE,
    llad.LAST_LIB_APPT_END_DATE,
    e.HOME_ADDR_STREET1,
    e.HOME_ADDR_STREET2,
    e.HOME_ADDR_CITY,
    e.HOME_ADDR_STATE,
    e.HOME_ADDR_ZIP,
    e.HOME_ADDR_COUNTRY,
    e.EMAIL_ADDRESS as MIT_EMAIL_ADDRESS,
    e.OFFICE_ADDRESS,
    e.OFFICE_PHONE,
    e.HOME_PHONE,
    e.ETHNIC_ORIGIN,
    e.GENDER,
    e.EMERGENCY_CONTACT_NAME,
    e.EMERGENCY_CONTACT_RELATION,
    e.EMERGENCY_CONTACT_EMAIL,
    e.EMERGENCY_HOME_PHONE,
    e.EMERGENCY_WORK_PHONE,
    e.EMERGENCY_CELL_PHONE,
    e.HIGHEST_DEGREE_TYPE,
    e.HIGHEST_DEGREE_YEAR,
    e.RESIDENCY_STATUS,
    ROUND(MONTHS_BETWEEN(SYSDATE, e.ORIGINAL_HIRE_DATE) / 12, 2) AS YRS_OF_MIT_SERV,
    e.YRS_OF_SERVICE as YRS_OF_PROF_EXPR,
    e.I9_FORM_EXPIRATION_DATE,
    e.RESIDENCY_STATUS,
    llad.TERMINATION_REASON
from HR_PERSON_EMPLOYEE e
inner join last_lib_appt_details llad on llad.MIT_ID = e.MIT_ID
where e.MIT_ID in (select MIT_ID from last_lib_appt)
order by LAST_NAME