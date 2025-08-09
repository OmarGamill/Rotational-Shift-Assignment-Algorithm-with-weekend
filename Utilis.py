import pandas as pd
import numpy as np
from pandas.api.types import CategoricalDtype
from writeHere import columns_name

def calculate_number_of_employees_per_shift(Number_of_employees ,percentage):
    """
    percentage: List of percentages for each shift.
    Calculate the number of employees required for a given shift based on the percentage.
    """
    employees_per_shift = []
    for shift, percent in zip(['Morning', 'Afternoon', 'Overnight'], percentage):
        if shift == 'Morning':
            num_employees = round(Number_of_employees * percent)
        elif shift == 'Afternoon':
            num_employees = round(Number_of_employees * percent)
        else:
            num_employees = int(Number_of_employees * percent)
        employees_per_shift.append(num_employees)

    return employees_per_shift

def calculate_number_of_employees_per_shift_weekend(employees_per_shift):
    employees_per_shift = [int(x * 0.5) for x in employees_per_shift]
    return employees_per_shift


def calculate_numEmp_req_experience_shift(df,exp_level=5,flag_weekend=False,day=1):
    numEmp_req_experience_shift = {
        "Morning": [],
        "Afternoon": [], 
        "Overnight": []
    }
    for i in range(1, exp_level + 1):
        cnt= df[df['Experience level num'] == i].shape[0]
        if flag_weekend:
            cnt=int(cnt /2)
        
        #print(f"Experience Level {i}: {cnt} employees")
        if cnt %3 == 0:
            numEmp_req_experience_shift['Morning'].append(cnt // 3)
            numEmp_req_experience_shift['Afternoon'].append(cnt // 3)
            numEmp_req_experience_shift['Overnight'].append(cnt // 3)
        elif cnt %3 ==cnt and cnt%2 == 0:
            numEmp_req_experience_shift['Morning'].append(cnt//2)
            numEmp_req_experience_shift['Afternoon'].append(cnt // 2)
            numEmp_req_experience_shift['Overnight'].append(0)
        elif cnt %3 ==cnt and cnt%2 != 0:
            if day==6:
                numEmp_req_experience_shift['Morning'].append(1)
                numEmp_req_experience_shift['Afternoon'].append(0)
                numEmp_req_experience_shift['Overnight'].append(1)
            else:
                numEmp_req_experience_shift['Morning'].append(1)
                numEmp_req_experience_shift['Afternoon'].append(0)
                numEmp_req_experience_shift['Overnight'].append(0)
        else:
            k =cnt - (cnt % 3)-1 
            numEmp_req_experience_shift['Morning'].append(k)
            numEmp_req_experience_shift['Afternoon'].append((cnt - k) // 2)
            numEmp_req_experience_shift['Overnight'].append((cnt - k) // 2)
    return numEmp_req_experience_shift

def get_experience_shift_counts(df, shift_col='1st Preferred shift', exp_col='Experience level num'): 
    """ 
    Returns a DataFrame counting how many agents from each experience level  
    are assigned to each shift. 
 
    Parameters: 
        df (pd.DataFrame): DataFrame containing at least shift and experience level columns 
        shift_col (str): Column name for shift assignment 
        exp_col (str): Column name for experience level (e.g., 1 to 5) 
 
    Returns: 
        pd.DataFrame: Table with counts of shifts per experience level 
    """ 
    # Group by experience level first, then shift
    pivot_df = df.groupby([exp_col, shift_col]).size().unstack(fill_value=0).reset_index()
    
    # Optional: Sort experience levels
    pivot_df = pivot_df.sort_values(by=exp_col).reset_index(drop=True)

    return pivot_df


def get_count_and_index(df, shift, exp_level):
    """
    Returns:
    - The number of employees for a given shift and experience level.
    - The index (position) of the first employee in that shift and experience category
      by summing all previous cells in the table.

    Parameters:
        df (pd.DataFrame): DataFrame with experience level as rows and shifts as columns.
        shift (str): Target shift name (e.g., 'Morning', 'Afternoon', 'Overnight').
        exp_level (int): Experience level number (1 to 5).

    Returns:
        (int, int): Tuple of (count, index)
    """
    index = 0

    # Loop through rows (experience levels)
    for i, row in df.iterrows():
        current_exp = row['Experience level num']
        
        for current_shift in df.columns[1:]:  # Skip the first column (exp level)
            count = row[current_shift]
            #print(f"Checking: Experience {current_exp}, Shift {current_shift}, Count {count}")
            if current_exp == exp_level and current_shift == shift:
                
                return count,index
                
            index += count

    return 0, index  # if not found


def split_into_equal_parts(data, num_parts):
    """
    Splits a list into `num_parts` equal parts (as equal as possible).
    
    Parameters:
        data (list): The list to split.
        num_parts (int): Number of parts to split into.
    
    Returns:
        List[List]: A list containing `num_parts` sublists.
    """
    k, m = divmod(len(data), num_parts)
    return [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(num_parts)]


def increase_total_shifts(df, index, count):
    df.loc[index:index+count, ['Total Shifts']] += 1

def increase_shift_night(df, index, count):
    df.loc[index:index+count, ['Shift Night']] += 1

def is_not_iterable(obj):
    try:
        iter(obj)
        return False  # It's iterable
    except TypeError:
        return True   # Not iterable
    
def insert_emp_in_table_(data,shift,day,table):
    if is_not_iterable(data):
        for row in data:
            row = list(row)
            row.insert(0, day)
            row.insert(1, shift)
            table.append(row)
        
    else:
        data.insert(0, day)
        data.insert(1, shift)
        table.append(data)

def insert_emp_in_table(data,shift,day,table):
    for row in data:
        row = list(row)
        row.insert(0, day)
        row.insert(1, shift)
        table.append(row)

def employee_schedule(df,req_emp_in_shift, count_emp_in_exp, index,name_shift,day,table):
   
    if count_emp_in_exp > req_emp_in_shift:
        data = df[index:index + req_emp_in_shift].values.tolist()
        shape_data = df[index + req_emp_in_shift:index + count_emp_in_exp].shape[0]
        
        insert_emp_in_table(data,name_shift,day,table)

        if shape_data==1:
            df.at[df.index[index + req_emp_in_shift], 'Shift Night'] += 1
            data_dis = df[index + req_emp_in_shift:index + count_emp_in_exp].values.tolist()
            insert_emp_in_table(data_dis,shift='Overnight',day=day,table=table)
        else:
            i=(index+req_emp_in_shift)+(count_emp_in_exp-req_emp_in_shift)//2
            i_i =(index+req_emp_in_shift)+((count_emp_in_exp-req_emp_in_shift)//2)+1
            j=index+count_emp_in_exp
            if shape_data %2==0:
                df.loc[i :j, ['Shift Night']]+= 1
            else:
                df.loc[i_i :j, ['Shift Night']]+= 1 
            data = split_into_equal_parts(df[index+req_emp_in_shift:index+count_emp_in_exp].values.tolist(),2)
            insert_emp_in_table(list(data[0]),shift='Afternoon',day=day,table=table)
            insert_emp_in_table(list(data[1]),shift='Overnight',day=day,table=table)
                

    else:

        insert_emp_in_table(data=df[index:index + count_emp_in_exp].values.tolist(),shift=name_shift,day=day,table=table)

def employee_schedule_weekend(df,req_emp_in_shift, count_emp_in_exp, index,name_shift,day,table):
   
    increase_total_shifts(df,index,req_emp_in_shift) 

    data = df[index:index + req_emp_in_shift].values.tolist()
    insert_emp_in_table(data,name_shift,day,table)
    for j in range(index + req_emp_in_shift,index+count_emp_in_exp):
        df.iloc[j, df.columns.get_loc('Weekend')] += 1
        data = list(df.iloc[j])
        insert_emp_in_table_(data,'Weekend',day,table)

    
        

def schedule_alg_run(df,Shifts,num_of_employees_per_shift_experience,experience_counts,day,table):
    for num_shift, shift in enumerate(Shifts):
        
        if num_shift == 0:
            for exp_level,req_in_shift in enumerate(num_of_employees_per_shift_experience[shift]):
                
                cnt, index = get_count_and_index(experience_counts, shift=shift, exp_level=exp_level + 1)
                
                employee_schedule(df,req_in_shift,cnt,index,shift,day,table)
            
                

        elif num_shift == 1:
            for exp_level, req_in_shift in enumerate(num_of_employees_per_shift_experience[shift]):
                cnt, index = get_count_and_index(experience_counts, shift=shift, exp_level=exp_level + 1)
                employee_schedule(df,req_in_shift,cnt,index,shift,day,table)
                

        else:
            for exp_level, req_in_shift in enumerate(num_of_employees_per_shift_experience[shift]):
                cnt, index = get_count_and_index(experience_counts, shift=shift, exp_level=exp_level + 1)
                employee_schedule(df,req_in_shift,cnt,index,shift,day,table)

def schedule_alg_run_weekend(df,Shifts,num_of_employees_per_shift_experience_Weekend,experience_counts,day,table):
    for num_shift, shift in enumerate(Shifts):
        
        for exp_level,req_in_shift in enumerate(num_of_employees_per_shift_experience_Weekend[shift]):
                cnt, index = get_count_and_index(experience_counts, shift=shift, exp_level=exp_level + 1)
                
                employee_schedule_weekend(df,req_in_shift,cnt,index,shift,day,table)


def print_data(table):
    for i, row in enumerate(table):
        print(i, " ", row)
