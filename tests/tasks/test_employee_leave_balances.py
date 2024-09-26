from hrqb.utils import md5_hash_from_values


def test_extract_dw_employee_leave_balances_load_sql_query(
    task_extract_dw_employee_leave_balances_complete,
):
    assert (
        task_extract_dw_employee_leave_balances_complete.sql_file
        == "hrqb/tasks/sql/employee_leave_balances.sql"
    )
    assert task_extract_dw_employee_leave_balances_complete.sql_query is not None


def test_task_transform_employee_leave_balances_key_expected_from_row_data(
    task_transform_employee_leave_balance_complete,
):
    row = task_transform_employee_leave_balance_complete.get_dataframe().iloc[0]
    assert row["Key"] == md5_hash_from_values(
        [
            row["MIT ID"],
            row["Balance Type"],
        ]
    )


def test_task_load_employee_leave_balances_explicit_properties(
    task_load_employee_leave_balances,
):
    assert task_load_employee_leave_balances.merge_field == "Key"
    assert (
        task_load_employee_leave_balances.input_task_to_load
        == "TransformEmployeeLeaveBalances"
    )
