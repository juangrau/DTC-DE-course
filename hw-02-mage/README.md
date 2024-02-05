# Week 2 Homework

## Mage

### Questions

### Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

- 266,855 rows x 20 columns
- 544,898 rows x 18 columns
- 544,898 rows x 20 columns
- 133,744 rows x 20 columns

**Answer**

After loading the dataset, the number of rows is the first choice: 266,855 rows x 20 columns

The python code for this block is available in the file **green_api_to_pandas.py**


### Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 and the trip distance is greater than zero, how many rows are left?

- 544,897 rows
- 266,855 rows
- 139,370 rows
- 266,856 rows

**Answer**

After transforming the dataset, the number of rows available is **139,370**

The python code for this block is available in the file **transform_greencab_data.py**

### Question 3. Data Transformation

Which of the following creates a new column lpep_pickup_date by converting lpep_pickup_datetime to a date?

- data = data['lpep_pickup_datetime'].date
- data('lpep_pickup_date') = data['lpep_pickup_datetime'].date
- data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
- data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()

**Answer**

The right answer is => *data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date*


### Question 4. Data Transformation

What are the existing values of VendorID in the dataset?

- 1, 2, or 3
- 1 or 2
- 1, 2, 3, 4
- 1

**Answer**

The existing values are **1 or 2**. This was obtained printing the unique values of that column


### Question 5. Data Transformation

How many columns need to be renamed to snake case?

- 3
- 6
- 2
- 4

**Answer**

Checking the column names, the right answer is **4**


### Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

- 96
- 56
- 67
- 108

**Answer**

Once exported, there are 96 partitions in Google Cloud. 

You can find all the pipeline python blocks in this folder of the repository.
