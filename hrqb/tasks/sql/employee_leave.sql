/*
Query for Employee Leaves.

CHANGELOG
    - 2024-05-13 Query created and added
    - 2024-07-23 Added HR_POSITION.POSITION ID to select
*/

select distinct
    appt.MIT_ID,
    appt.APPT_BEGIN_DATE,
    appt.APPT_END_DATE,
    p.POSITION_ID,
    appt.HR_APPT_KEY,
    abse.ABSENCE_DATE,
    at.ABSENCE_TYPE,
    at.ABSENCE_TYPE_CODE,
    abse.ACTUAL_ABSENCE_HOURS,
    abse.ACTUAL_ABSENCE_DAYS,
    case
        when at.ABSENCE_TYPE = 'Leave Without Pay' then 'N'
        else 'Y'
    end as PAID_LEAVE
from HR_ABSENCE_DETAIL abse
inner join HR_APPOINTMENT_DETAIL appt on abse.MIT_ID = appt.MIT_ID
inner join HR_ABSENCE_TYPE at on at.HR_ABSENCE_TYPE_KEY = abse.HR_ABSENCE_TYPE_KEY
left join HR_POSITION p on p.HR_POSITION_KEY = appt.HR_POSITION_KEY
where (
    appt.APPT_END_DATE >= TO_DATE('2019-01-01', 'YYYY-MM-DD')
    and abse.ABSENCE_DATE between appt.APPT_BEGIN_DATE and appt.APPT_END_DATE
)
order by appt.MIT_ID, abse.ABSENCE_DATE