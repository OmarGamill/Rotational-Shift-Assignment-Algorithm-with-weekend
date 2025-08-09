import pandas as pd

from ProsessData import load_data, transfrom_data, sort_dataset, process_and_export_schedule
from Utilis import get_experience_shift_counts, print_data as pr_data ,calculate_number_of_employees_per_shift
from Utilis import increase_total_shifts, schedule_alg_run, calculate_numEmp_req_experience_shift
from Utilis import calculate_number_of_employees_per_shift_weekend, schedule_alg_run,schedule_alg_run_weekend
from ProsessData import process_and_export_schedule
from writeHere import days, Shifts, table, Path_to_dataset, save_path,Number_of_employees,Percentage_all
from writeHere import Weekend_days


def pipline(path_to_dataset=Path_to_dataset, save_path=save_path, print_data=True, save_data=True):

    dataset = load_data(path_to_dataset)

    dataset = transfrom_data(dataset=dataset)
    experience_counts = get_experience_shift_counts(dataset)
    num_of_employees_per_shift_experience_weekend = calculate_numEmp_req_experience_shift(dataset,flag_weekend=True)
    num_of_employees_per_shift= calculate_number_of_employees_per_shift(Number_of_employees,Percentage_all)
    num_of_employees_per_shift_weekend = calculate_number_of_employees_per_shift_weekend(num_of_employees_per_shift)

    num_of_employees_per_shift_experience = calculate_numEmp_req_experience_shift(
        dataset, exp_level=5)
    

    for num_day, day in enumerate(days):
        if day in Weekend_days:
            if num_day==6:
                num_of_employees_per_shift_experience_weekend = calculate_numEmp_req_experience_shift(dataset,flag_weekend=True,day=num_day)
            schedule_alg_run_weekend(dataset,Shifts,num_of_employees_per_shift_experience_weekend,experience_counts,day,table)

        else:
            increase_total_shifts(dataset, 0, dataset.shape[0] - 1)
            schedule_alg_run(dataset,Shifts,num_of_employees_per_shift_experience,experience_counts,day,table)
        
        dataset = sort_dataset(dataset)

    if print_data:
        pr_data(table)

    output_table = process_and_export_schedule(table, save_path, save_data)
