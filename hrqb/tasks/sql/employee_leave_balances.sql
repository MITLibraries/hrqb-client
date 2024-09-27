/*
Query for Employee Leave Balances.

CHANGELOG
    - 2024-09-26 Query created and added
    - 2024-09-26 Filter to employees with appointment end dates >= 2019
*/

select
    b.MIT_ID,
    bt.ABSENCE_BALANCE_TYPE as BALANCE_TYPE,
    b.BEGINNING_BALANCE_HOURS,
    b.DEDUCTED_HOURS,
    b.ENDING_BALANCE_HOURS,
    b.BEGINNING_BALANCE_DAYS,
    b.DEDUCTED_DAYS,
    b.ENDING_BALANCE_DAYS,
    b.ABSENCE_BALANCE_BEGIN_DATE,
    b.ABSENCE_BALANCE_END_DATE
from HR_ABSENCE_BALANCE b
inner join HR_ABSENCE_BALANCE_TYPE bt on b.HR_ABSENCE_BALANCE_TYPE_KEY = bt.HR_ABSENCE_BALANCE_TYPE_KEY
where bt.ABSENCE_BALANCE_TYPE != 'N/A'
and b.MIT_ID in (
    select
        a.MIT_ID
    from HR_APPOINTMENT_DETAIL a
    where a.APPT_END_DATE >= TO_DATE('2019-01-01', 'YYYY-MM-DD')
)