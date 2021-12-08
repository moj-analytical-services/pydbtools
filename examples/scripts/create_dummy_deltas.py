import pandas as pd


def get_dummy_deltas(employees_path):
    employees = pd.read_csv(employees_path, nrows=10)

    # Create deleted flag
    employees["record_deleted"] = False
    employees["record_deleted"] = employees["record_deleted"].astype(pd.BooleanDtype())

    # Cast to new int cols
    for col in ["employee_id", "department_id", "manager_id"]:
        employees[col] = employees[col].astype(pd.Int64Dtype())

    # Cast to new str cols
    for col in ["sex", "forename", "surname"]:
        employees[col] = employees[col].astype(pd.StringDtype())


    # Let's split up the data and make some changes
    day1 = employees[employees.employee_id.isin([1,2,3,4,5])].reset_index(drop=True)

    day2 = employees[employees.employee_id.isin([5,6,7])].reset_index(drop=True)
    day2.loc[0, "department_id"] = 2
    day2.loc[0, "manager_id"] = 18

    day3 = employees[employees.employee_id.isin([1,7,9,10,11])].reset_index(drop=True)
    day3.department_id = 2
    day3.manager_id = 5

    # Reset this persons values for clarity
    day3.loc[0, "record_deleted"] = True
    day3.loc[0, "department_id"] = 1
    day3.loc[0, "manager_id"] = 17
    
    deltas = {
        "day1": day1,
        "day2": day2,
        "day3": day3
    }

    return deltas