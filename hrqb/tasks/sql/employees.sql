/*
Query for Employee data.

CHANGELOG
    - 2024-05-13 Query created and added
    - 2024-07-09 Added CTEs to gather last appointment transaction, to get termination
        or retirement reason
*/

with ordered_appt_txn as (
    select
        a.MIT_ID,
        at.HR_PERSONNEL_ACTION,
        at.HR_ACTION_REASON,
        row_number() over (
            partition by a.MIT_ID
            order by a.APPT_TX_BEGIN_DATE desc, a.APPT_TX_END_DATE desc
        ) as txn_row_num
    from HR_APPT_TX_DETAIL a
    left join HR_PERSONNEL_ACTION_TYPE at on at.HR_PERSONNEL_ACTION_TYPE_KEY = a.HR_PERSONNEL_ACTION_TYPE_KEY
    where at.HR_PERSONNEL_ACTION in ('Termination','Retirement')
),
last_appt_txn as (
    select
        MIT_ID,
        HR_PERSONNEL_ACTION,
        HR_ACTION_REASON
    from ordered_appt_txn
    where txn_row_num = 1
)
select
    e.MIT_ID,
    e.FIRST_NAME,
    e.LAST_NAME,
    e.NAME_KNOWN_BY as PREFERRED_NAME,
    e.DATE_OF_BIRTH,
    e.ORIGINAL_HIRE_DATE AS MIT_HIRE_DATE,
    e.CURRENT_EMPLOYMENT_DATE as MIT_LIB_HIRE_DATE,
    e.APPOINTMENT_END_DATE,
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
    lat.HR_PERSONNEL_ACTION as TERMINATION_ACTION,
    lat.HR_ACTION_REASON as TERMINATION_REASON
from HR_PERSON_EMPLOYEE e
left join last_appt_txn lat on lat.MIT_ID = e.MIT_ID
where e.MIT_ID in (
    select
        a.MIT_ID
    from HR_APPOINTMENT_DETAIL a
    where a.APPT_END_DATE >= TO_DATE('2019-01-01', 'YYYY-MM-DD')
)
order by LAST_NAME