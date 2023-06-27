import pandas as pd
from difflib import SequenceMatcher
import json
from datetime import date
import os

# get all file names in folder
def file_finder(path):
    file_names = []
    for file_name in os.listdir(path):
        # ignore .DS_Store which contains view settings of folder
        if file_name == '.DS_Store':
            continue
        file_names.append(file_name)
    return file_names

def ask_open(file):
    file_type = ''
    
    if 'csv' in file:
        file_type = 'csv'

        ''' some csv statements have inconsistent number of columns, for example TD will produce 
        a statement with 3 column names, but provide 5 columns of data for each row. This inconsistency causes 
        read_csv to have an error and quit'''

        # open the file as a text file to count the number of , in each row to determine the max column number
        max_col = 1 # count starts at 1 since you need to include first column that doesn't need a comma
        with open(file) as f:
            for line in f:
                col = 1
                for char in line:
                    if char == ',':
                        col += 1
                if col > max_col:
                    max_col = col

        col_names = [i for i in range(max_col)]

    elif 'xls' in file:
        file_type = 'xls'
        
    else:
        print(file)
        print('file type not supported')
        quit()

    # read file

    if file_type == 'csv':
        raw_df = pd.read_csv(file,header=None,index_col=None, names=col_names)
    elif file_type == 'xls':
        raw_df = pd.read_excel(file,header=None,index_col=None)

    return raw_df

def drop_rows(df):
    print('---------PRINTING DATA (FIRST 30 ROWS)---------')
    print(df.head(30))
    loc_header = int(input('Which row does the data start (excl. column names), as a number from the printed table? '))
    # slice from header position to bottom
    dropped_df = df.iloc[loc_header:]
    dropped_df = dropped_df.reset_index(drop=True)
    print('---------PRINTING SLICED DATA (FIRST 5 ROWS)---------')
    print(dropped_df.head())
    return dropped_df

def col_id(df):
    date_col = int(input('Which column contains the date data?: ')) 
    merch_col = int(input('Which column contains the merchant name?: '))
    spend_col = int(input('Which column contains the spend data?: '))
    return merch_col, spend_col, date_col

def spend_clean(df, spend_col):
    # set as all string for consistency to prevent different data types per row
    df.iloc[:, spend_col] = df.iloc[:, spend_col].astype(str)
    # remove $ and , symbols which some data sources have
    df[df.columns[spend_col]] = df[df.columns[spend_col]].str.replace('$', '', regex=True)
    df[df.columns[spend_col]] = df[df.columns[spend_col]].str.replace(',', '', regex=True)
    # convert or reconvert to float from str
    df[df.columns[spend_col]] = df[df.columns[spend_col]].astype(float)
    return df

def data_style(df, spend_col):
    # check if spend col uses -ve or is standard
    check = ''
    while check not in ['y', 'n']:
        check = input('Is the spend column in negatives? [y/n]: ')
    if check == 'y':
        df.iloc[:, spend_col] = df.iloc[:, spend_col]*-1
        print('---------PRINTING CONVERTED DATA FROM NEGATIVE TO POSITIVE---------')
        print(df)
    # drop negative columns
    return df

# compares % similarity of two strings
def sim_check(value1, value2):
    return SequenceMatcher(None, value1, value2).ratio()

# create new merchant in cat_mem if merchant doesn't exist yet in memory
def append_cat_mem(merch_data, cat_dict):
    unique_categories = set(category for category in cat_dict.values())
    print('Existing categories: ')
    for existing_cat in unique_categories:
        print(existing_cat)

    cat_new_merch = str.lower(input('New merchant: [' + merch_data + '] please provide a category: '))

    cat_dict[merch_data] = cat_new_merch

    # save the dictionary
    with open('cat_mem.json', 'w') as f:
        json.dump(cat_dict, f)

    return cat_dict

# loop through cat_dict and check if a similar merchant exists, then set return the category stored in memory
def merch_checker(category, merch_data, cat_dict):
    for merchant in cat_dict.keys():
        if sim_check(merch_data, merchant) >= 0.8:
            category = cat_dict[merchant]
    return category

# categorize the merchant
def catagorizer(merch_data, cat_dict):
    # find the category merchant belongs to
    category = ''
    while category == '':
        category = merch_checker(category, merch_data, cat_dict)
        if category == '':
            cat_dict = append_cat_mem(merch_data, cat_dict)
    return category

# open memory of expense categories
with open('cat_mem.json') as f:
    cat_dict = json.load(f)

for statement in file_finder('statements_to_analyze'):
    print('Analyzing: '+ statement)
    raw_df = ask_open('statements_to_analyze/' + statement)
    sliced_df = drop_rows(raw_df)
    merch_col, spend_col, date_col = col_id(sliced_df)
    clean_df = spend_clean(sliced_df, spend_col)
    clean_df = data_style(clean_df, spend_col)
    # add category column
    clean_df['Category'] = clean_df.apply(lambda row: catagorizer(row.iloc[merch_col], cat_dict), axis=1)
    # make sure date column is in the correct format
    clean_df.iloc[:,date_col] = pd.to_datetime(clean_df.iloc[:,date_col]).dt.strftime('%d/%m/%Y')
    # create the processed data frame
    # select relevant cols
    rel_col = [date_col, merch_col, spend_col, len(clean_df.columns) - 1]
    processed_df = clean_df.iloc[:, rel_col]
    processed_df = processed_df.sort_values(by=processed_df.columns[date_col])
    # append this month's expenses to master csv of expenses
    with open('saved_expense_data.csv', mode='a', newline='') as f:
        processed_df.to_csv(f, header=f.tell()==0, index=False)
    print('Successfully analyzed "' + statement +'"')

