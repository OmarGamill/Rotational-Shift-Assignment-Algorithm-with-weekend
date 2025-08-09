import pandas as pd
import numpy as np
from pandas.api.types import CategoricalDtype
from writeHere import columns_name


def load_data(file_path):
    """
    Load data from a CSV file into a pandas DataFrame.

    Parameters:
    file_path (str): The path to the CSV file.

    Returns:
    pd.DataFrame: DataFrame containing the loaded data.
    """
    try:
        data = pd.read_excel(file_path)
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def sort_dataset(dataset):
    if dataset is not None:
        dataset = dataset.sort_values(
            by=['Experience level num', '1st Preferred shift',
                'Total Shifts', 'Shift Night',  'Weekend'],
            ascending=[True, True, False, False, True]
        ).reset_index(drop=True)
        return dataset
    else:
        raise ValueError(
            "Failed to load dataset. Please check if 'dataset.xlsx' exists and is accessible.")


def transfrom_data(dataset):
    # Step 1: Fill missing values in '2nd Preferred Shift' with 'Not Ava'
    dataset['2nd Preferred Shift'] = dataset['2nd Preferred Shift'].fillna(
        'Not Ava')
    # Step 2: Add 'Experience level num' column after 'Experience level'
    dataset.insert(
        dataset.columns.get_loc('Experience level') + 1,
        'Experience level num',
        dataset['Experience level'].str.extract(r'IC\s*(\d)').astype(int)
    )

    # Step 3: Add 3 new columns after '1st Preferred shift'
    insert_loc = dataset.columns.get_loc('1st Preferred shift') + 1
    dataset.insert(insert_loc, 'Total Shifts', 0)
    dataset.insert(insert_loc + 1, 'Shift Night', 0)
    dataset.insert(insert_loc + 2, 'Weekend', 0)

    # Step 4 : Sort Datasrt
    dataset = sort_dataset(dataset)

    return dataset


def process_and_export_schedule(table, save_path, save_data):
    output_df = pd.DataFrame(table, columns=['Day', 'Shift'] + columns_name)
    day_order = [ 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday','Sunday','Monday']
    shift_order = ['Morning', 'Afternoon', 'Overnight']

    # Convert 'Day' column to categorical with custom order
    day_type = CategoricalDtype(categories=day_order, ordered=True)
    shift_type = CategoricalDtype(categories=shift_order, ordered=True)

    output_df['Day'] = output_df['Day'].astype(day_type)
    output_df['Shift'] = output_df['Shift'].astype(shift_type)


    # Step 1: Sort
    output_df = output_df.sort_values(by=['Day', 'Shift', 'Experience level num']).reset_index(drop=True)

    # Step 2: Initialize 'Notes' column
    output_df['Notes'] = ''

    # Step 3: Apply conditional logic
    # 1. Weekend > 0 → 'Weekend'
    output_df.loc[output_df['Weekend'] > 0, 'Notes'] = 'Weekend'

    # 2. Shift Name == 'Afternoon' → 'peak coverage' (don't overwrite existing notes)
    output_df.loc[(output_df['Shift'] == 'Afternoon') & (output_df['Notes'] == ''), 'Notes'] = 'peak coverage'

    # 3. Overnight + Morning preference → 'Overnight'
    overnight_mask = (
        (output_df['Shift'] == 'Overnight') &
        (output_df['1st Preferred shift'] == 'Morning') &
        (output_df['Notes'] == '')
    )
    output_df.loc[overnight_mask, 'Notes'] = 'Overnight'

    # 4. Match between shift name and preferred shift
    match_mask = (
        (output_df['Shift'] == output_df['1st Preferred shift']) &
        (output_df['Notes'] == '')
    )
    output_df.loc[match_mask, 'Notes'] = 'matched preferred shift'

  

    # Step 4: Select final columns
    final_columns = ['Day', 'Name','Shift','1st Preferred shift', 'Experience level', 'Total Shifts', 'Shift Night', 'Weekend', 'Notes']
    final_df = output_df[final_columns]

    # Step 5: Save to Excel
    final_df.to_excel(save_path, index=False)

    return final_df

