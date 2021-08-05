from numpy import nan
import pandas as pd
import time

#TODO Create a function that shows each tip
#TODO Note how to generalize concepts

#Column Renaming
def rename_columns(data : pd.DataFrame, column_names : dict = {}):
    print(data.head())
    if len(column_names) != 0:
        data = data.rename(column_names, axis = 1)
    else:
        data.columns = data.columns.str.replace("_", " ")
    print(data.head())
    
#Row and Column Reversing
def reverse_data(data : pd.DataFrame, reverse_rows : bool = False, reverse_columns : bool = False):
    print(data.head())
    if reverse_rows:
        data = data.loc[::-1].reset_index(drop = True)
    if reverse_columns:
        data = data.loc[:,::-1]
    print(data.head())

#Selecting by Data Type and by Column
def select_by_data_type(data: pd.DataFrame, data_type : list):
    data = data.select_dtypes(data_type)
    print(data)

#Using Pickles
def load_data_as_pickle():
    try:
        data = pd.read_pickle("uscounties.pkl")
        return data
    except:
        data_csv = pd.read_csv("uscounties.csv")
        pd.to_pickle(data_csv, "uscounties.pkl")
        return load_data_as_pickle()

#Filtering by Categories
def filter_data_by_multiple_categories(data : pd.DataFrame):
    print(data[data.county.isin(["Los Angeles", "San Diego"])].reset_index(drop = True))
    
    print(data[~data.county.isin(["Los Angeles", "San Diego"])].reset_index(drop = True))

#Creating new Categories with binning
def create_bins_for_data(data : pd.DataFrame):
    labels = ["small", "medium", "large"]
    bins = [0, 50000, 250000, data.population.max()]
    data['population_category'] = pd.cut(data.population, bins = bins, labels = labels)
    print(data)
    print(data[data.population_category.isin(['medium'])])

#Filter by N Largest
def filter_by_most_common_values(data : pd.DataFrame):
    counts = data.state_id.value_counts()
    data = data[data.state_id.isin(counts.nlargest(20).index)]
    print(data)

#Summerize Missing Data
def summerize_missing_data(data : pd.DataFrame):
    data.state_id = data.state_id.replace("TX", nan)
    print(data.isna().sum())
    print(data.isna().mean())

#Multiple Aggregation
def doing_multiple_aggregations(data : pd.DataFrame):
    print(data.groupby('state_name').population.agg(['sum', 'mean']))

#Transform Aggregation into DF
def implement_aggregation_into_data(data : pd.DataFrame):
    sum_of_population = data.groupby('state_name').population.transform('sum')
    total_counties = data.groupby('state_name').county.transform('count')
    mean_county_population = data.groupby('state_name').population.transform('mean')
    data['total_population'] = sum_of_population
    data['total_counties'] = total_counties
    data['average_county_population'] = mean_county_population
    print(data.head())

#Simplify Aggregation to be Interactable
def improve_aggregation_interactability(data : pd.DataFrame):
    data = data.groupby(['state_name','county']).population.mean().unstack() #THIS IS AMAZING FOR MELTING ANNUAL DATA
    print(data)

#Create a Pivot Table
def create_pivot_table(data : pd.DataFrame):
    print(data.pivot_table(index = 'state_name', columns = 'county', values = 'population', aggfunc = 'sum'))

#Default HTML dashboard
def create_dashboard_of_data(data : pd.DataFrame):
    import pandas_profiling

    report = pandas_profiling.ProfileReport(data, title = "Demonstration")
    report.to_file("report.html")


if __name__ == "__main__":
    #Import Data
    data = pd.read_csv("uscounties.csv")

    ##### START DEMONSTRATION CODE #####
    start = time.perf_counter()

    #Rename Columns
    #new_columns = {"county" : "A Very Different Name"}
    #rename_columns(data)

    #Changing Data Order
    #reverse_data(data, True, True)

    #Selecting by Data Type
    #https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.select_dtypes.html
    #select_by_data_type(data, ['int64'])

    #Using less memory 
    #data_2 = pd.read_csv("uscounties.csv")
    #data_2 = pd.read_csv("uscounties.csv", usecols = ['county', 'state_name', 'population'])

    #Loading data much quicker
    #data_2 = pd.read_csv("uscounties.csv")
    #data_2 = load_data_as_pickle()

    #Cutting by multiple conditions
    #filter_data_by_multiple_categories(data)

    #Creating Categories from Continuous Data "Binning"
    #create_bins_for_data(data)

    #Finding the most common numbers and 
    #filter_by_most_common_values(data)

    #Summerize missing data
    #summerize_missing_data(data)

    #Produce several aggregations at once
    #doing_multiple_aggregations(data)

    #Add aggregations to data
    #implement_aggregation_into_data(data)

    #Make aggregation easier to work with
    #improve_aggregation_interactability(data)

    #Create a Pivot Table
    #create_pivot_table(data)

    #Create Dashboard
    create_dashboard_of_data(data)

    end = time.perf_counter()
    total_time = end - start
    print(f"Time Needed for Task: {total_time:0.4f} Seconds")
    ##### END DEMONSTRATION CODE #####
