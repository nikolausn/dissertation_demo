#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import abydos
from abydos import distance


# # General Instruction
# 
# - Cleanup dataset based on the information that is given:
# You need to clean the dataset according to the information that is given to you. This means that there are problems with the dataset that need to be fixed, and you should use the information given to you to determine what those problems are and how to fix them.
# 
# - Each case has different data quality problems, there will be hint and additional information that can help you understand the problem:
# Each row in the dataset may have different data quality problems. There will be hints and additional information provided to help you understand what the specific problem is with each row.
# 
# - You can do any approach on cleaning the data, but you should clean the instructed column only:
# You have the freedom to use any approach to clean the data, but you should only clean the instructed column. This means that you should not modify any other columns in the dataset, or add or remove any rows.
# 
# - Do not create new column or remove any column. Also do not create new row, or remove any row:-
# You are not allowed to create new columns or remove any columns from the dataset. You are also not allowed to add or remove any rows.
# 
# - Each column will have a flag column something equivalent to <column\_name>\_flag. This column can be used to flag the row if you want to not include it to the downstream task. 0: safe_flag, 1: delete_flag, 2: null_flag (if you want to still include the row with null treatment). You can also add a new category but please add justification and explanation of the new category, there are three categories you can use:
# safe_flag (0): this row is safe to use in downstream tasks
# delete_flag (1): this row should be deleted and not used in downstream tasks
# null_flag (2): this row can be included in downstream tasks but with null treatment.
# You can also add a new category, but you need to provide a justification and an explanation for the new category. It is worth to note that the completeness of the dataset is also matter, so try not to flag to many things, and do your best to clean the values.
# 
# - For each data cleaning task, we have provided a function that represents the goal of the cleaning. For example, clean_duplicate_id(df) is the function for removing duplicate ID values. These functions take a DataFrame as input and return the cleaned version of the DataFrame.
# 
#     In each chunk of data cleaning task, you will see the following three parts:
# 
#     1. The clean_<name> function that performs the specific cleaning task.
#     2. The execution of the cleaning function on the DataFrame.
#     3. A checking part to help you evaluate the effectiveness of the cleaning.
#     
#   While you can create new cells and add additional code, the cleaning must be performed through the provided cleaning functions. You can adjust the order of the cleaning steps, but please try to move the whole chunks of code to avoid any errors.
# 
# The cleaning task will be considered complete if this notebook can be run sequentially by executing "restart and runall"
# 
# 
# 

# # Purpose
# The purpose of this dataset is to conduct exploratory analysis of the listings and create a prediction model for listing price using some columns from the dataset. This means that the dataset is intended to be used to explore the characteristics and features of the listings, and to build a model that can predict the price of a listing based on certain variables in the dataset. The goal is to gain insights into the factors that influence the price of a listing and to develop a model that can accurately predict listing prices based on those factors.

# # Columns and Dataset Description
# - id: a unique identifier for each listing.
# - name: the name or title of the listing, as provided by the host.
# - host_id: a unique identifier for each host.
# - host_name: the name of the host who listed the property.
# - neighbourhood_group: the larger geographic area in which the listing is located (e.g. a borough or group of neighborhoods).
# - neighbourhood: the specific neighborhood in which the listing is located.
# - latitude: the latitude coordinate of the listing.
# - longitude: the longitude coordinate of the listing.
# - room_type: the type of space that is being listed (e.g. an entire apartment, a private room, a shared room).
# - price: the nightly price of the listing, in the currency specified in the dataset.
# - minimum_nights: the minimum number of nights that a guest must book the listing for.
# - number_of_reviews: the total number of reviews that the listing has received.
# - last_review: the date of the most recent review of the listing.
# - reviews_per_month: the average number of reviews per month that the listing has received.
# - calculated_host_listings_count: the total number of listings that the host has on Airbnb.
# - availability_365: the number of days per year that the listing is available for booking.
# - number_of_reviews_ltm: the total number of reviews that the listing has received in the last 12 months.
# - license: a license number for the listing, if applicable (this column may not be present in all versions of the dataset).
# 
# Besides the columns above, there are columns pre-defined for flagging the rows based on particular data cleaning context:
# - id_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the id column (duplicate).
# - host_id_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the host_id column.
# - neighbourhood_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the neighbourhood column.
# - latitude_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the latitude column.
# - longitude_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the longitude column.
# - minimum_nights_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the minimum_nights column.
# - number_of_reviews_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the number_of_reviews column.
# - last_review_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the last_review column.
# - room_type_flag: a flag column indicating whether a given row should be included in downstream analysis or not based on data quality issues related to the room_type column.

# # Load Data

# In[2]:


def airbnb_curator_c1(airbnb_pd):


    # In[3]:


    airbnb_pd.columns


    # In[4]:


    airbnb_pd


    # # cleanup duplicate id
    # The ID column must contain unique values. If there are any duplicate values in this column, you will need to take action to ensure that each ID is unique. You can do this by either fixing the duplicates (if you want to keep them) or by flagging them for removal (1) using the id_flag column.

    # In[5]:


    def clean_duplicate_id(df):
        #raise Exception("not yet have implementation")
        # do something here
    #     df = df[df.duplicated(['id'])]
        df['id_flag'] = df.duplicated(['id'])
        df['id_flag'] = df['id_flag'].map({False: 0, True: 1})
        return df


    # In[6]:


    airbnb_pd = clean_duplicate_id(airbnb_pd)
    airbnb_pd


    # # Duplicate IDS checking 
    # To ensure that all ID values in the dataset are unique, you should check for duplicate IDs. When you run the query to check for duplicates, there should be no rows returned, indicating that there are no duplicate ID values present in the dataset.

    # In[7]:


    dup_ids = airbnb_pd[airbnb_pd.id_flag==0]
    dup_ids = dup_ids.groupby("id").count()[["name"]].reset_index()
    dup_ids = dup_ids[dup_ids.name>1]
    dup_ids


    # # cleanup inconsistent host id
    # Each host_id value in the dataset should be associated with only one host_name. However, there may be inconsistencies in the dataset where a host_id is associated with different host_name values.
    # 
    # To clean this up, you can either change the host_name value to a consistent value based on information in the dataset, or flag the host_id_flag column to indicate that the row should be removed from downstream tasks.
    # 
    # For example, if you find that a host_id is associated with multiple host_name values, you may want to investigate further to determine which host_name is correct. If one of the host_name values is clearly incorrect (e.g., a misspelling or a name that does not match the owner of the property), you could update the host_name value to the correct value.
    # 
    # Alternatively, if you cannot determine the correct host_name value, or if you want to exclude the row from downstream tasks for other reasons, you can flag the host_id_flag column with a value of 1 to indicate that the row should be removed.

    # In[8]:


    def clean_host_id(df):
        #raise Exception("not yet have implementation")
        # do something here
        grouped = df.groupby('host_id')['host_name'].nunique().reset_index(name='count')
        duplicates = grouped[grouped['count'] > 1]['host_id'].tolist()

        for idx, row in df.iterrows():
            if row['host_id'] in duplicates:
                df.loc[idx, 'host_id_flag'] = 1
        return df


    # In[9]:


    airbnb_pd = clean_host_id(airbnb_pd)


    # # Inconsistent Host ID checking 
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[10]:


    dup_host_id = airbnb_pd[airbnb_pd.host_id_flag==0]
    dup_host_id = dup_host_id.groupby(["host_id","host_name"]).count()[["id"]].reset_index()
    dup_host_id = dup_host_id.groupby("host_id").count()["id"].reset_index()
    dup_host_id[dup_host_id["id"]>1]


    # # cleanup neighbourhood
    # The neighbourhood column in the dataset should contain values that match the neighbourhoods defined in the official neighbourhood_list. However, there may be some values in the neighbourhood column that are incorrect due to errors or noise in the data.
    # 
    # To clean up the neighbourhood column, you can try to match each value in the column to a valid neighbourhood in the neighbourhood_list using a string distance function such as abydos. If you can successfully match a value in the neighbourhood column to a neighbourhood in the neighbourhood_list, you can replace the value in the dataset with the correct neighbourhood name.
    # 
    # However, if you are unsure about how to clean up a particular value in the neighbourhood column, or if you cannot match the value to a valid neighbourhood in the neighbourhood_list, you can flag the row for deletion by setting the neighbourhood_flag column to a value of 1. If the value in the neighbourhood column is null and you cannot make a determination based on other information in the dataset, you can set the neighbourhood_flag column to a value of 2 to indicate that the row should be included but the neighbourhood value is null.
    # 
    # You can also use the latitude and longitude columns in the dataset to help match values in the neighbourhood column to valid neighbourhoods in the neighbourhood_list. However, you should be aware that the latitude and longitude values may also contain errors or noise, so you should exercise caution when using these columns to clean up the neighbourhood column.

    # In[11]:


    neighbourhood_list = [ 'Hyde Park', 'West Town', 'Lincoln Park', 'Near West Side', 'Lake View',    'Dunning', 'Rogers Park', 'Logan Square', 'Uptown', 'Edgewater',    'North Center', 'Albany Park', 'West Ridge', 'Pullman', 'Irving Park',    'Beverly', 'Lower West Side', 'Near South Side', 'Near North Side',    'Grand Boulevard', 'Bridgeport', 'Humboldt Park', 'Chatham', 'Kenwood',    'Loop', 'West Lawn', 'Lincoln Square', 'Woodlawn', 'Avondale',    'Forest Glen', 'Portage Park', 'East Garfield Park', 'Washington Park',    'North Lawndale', 'Armour Square', 'South Lawndale', 'South Shore',    'Morgan Park', 'South Deering', 'West Garfield Park', 'Hermosa',    'Mckinley Park', 'Douglas', 'Hegewisch', 'West Elsdon', 'Norwood Park',    'Garfield Ridge', 'Austin', 'Belmont Cragin', 'Jefferson Park', 'Ashburn',    'Greater Grand Crossing', 'North Park', 'Oakland', 'Archer Heights',    'Edison Park', 'Englewood', 'Ohare', 'Brighton Park', 'Chicago Lawn',    'New City', 'South Chicago', 'Mount Greenwood', 'Montclare', 'Roseland',    'West Englewood', 'Calumet Heights', 'Auburn Gresham', 'Fuller Park',    'Avalon Park', 'Burnside', 'Clearing', 'Gage Park', 'West Pullman',    'Washington Heights', 'East Side']
    print(neighbourhood_list)


    # In[12]:


    from Levenshtein import distance


    # In[13]:


    airbnb_pd


    # In[14]:


    def find_closest_match(string):
        closest_match = ""
        min_distance = float('inf')
        if pd.isna(string):
            return string
        else:
            for term in neighbourhood_list:
                d = distance(string, term)
                if d < min_distance:
                    closest_match = term
                    min_distance = d
            return closest_match

    def clean_neighbourhood(df):
        #raise Exception("not yet have implementation")
        df['neighbourhood'] = df['neighbourhood'].apply(find_closest_match)
        df.loc[~df['neighbourhood'].isin(neighbourhood_list), 'neighbourhood_flag'] = 1
        df.loc[df['neighbourhood'].isna(), 'neighbourhood_flag'] = 2
        return df


    # In[15]:


    airbnb_pd = clean_neighbourhood(airbnb_pd)


    # In[16]:


    airbnb_pd[airbnb_pd['neighbourhood_flag']==2]


    # In[17]:


    airbnb_pd[airbnb_pd['neighbourhood']==2]


    # # Neighbourhood checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[18]:


    neighbourhood_check = airbnb_pd[airbnb_pd.neighbourhood_flag==0]
    neighbourhood_check = neighbourhood_check[neighbourhood_check.neighbourhood.apply(lambda x:x not in neighbourhood_list)]
    neighbourhood_check[["id","neighbourhood"]]


    # # cleanup latitude and longitude
    # The latitude and longitude values in the dataset must fall within the range of -90 to +90 for latitude and -180 to +180 for longitude to ensure that they meet the criteria for analysis. We have provided a check number function to validate the latitude and longitude columns. Any values outside of these ranges should be cleaned to meet the criteria.
    # 
    # If you are unsure what to do with a value or if it is a null value, you can flag the row for deletion by setting latitude_flag or longitude_flag to 1 or 2, respectively.

    # In[19]:


    def check_number(x,start=-90,end=90):
        try:
            temp_x = float(x)
            if start <= temp_x <= end:
                return True
        except:
            return False


    # In[20]:


    import re
    def clean_latitude(df):
        #raise Exception("not yet have implementation")
        # do something here
        lat_mask = df['latitude'].apply(lambda x: not check_number(x, -90, 90))
        df.loc[lat_mask, 'latitude_flag'] = 1
    #     df.loc[lat_mask, 'delete_flag'] = 1
        df.loc[df['latitude'].isna(), 'latitude_flag'] = 2
        # nikolaus changed clean_latitude implementation to be
        df.loc[df['latitude_flag']==1, 'latitude'] = df.loc[df['latitude_flag']==1, 'latitude'].apply(lambda x: clean_latitude_pls(x))
        # convert the latitude_flag back
        df.loc[df['latitude_flag']==1, 'latitude_flag'] = 0
        lat_mask = df['latitude'].apply(lambda x: not check_number(x, -90, 90))
        df.loc[lat_mask, 'latitude_flag'] = 1    
        return df


    # In[21]:


    def clean_latitude_pls(latitude_string):
        # nikolaus: add minus sign (since range can be - to +)
        latitude_string = re.sub("[^0-9.\-]", "", latitude_string)
        # convert the cleaned latitude string to a float data type
        latitude = float(latitude_string)
        # if the latitude is greater than 90 (the maximum latitude value), set it to NaN
        if latitude > 90:
            latitude = float('nan')
        return latitude


    # In[22]:


    # apply the clean_latitude function to the 'latitude' column
    airbnb_pd = clean_latitude(airbnb_pd)
    # airbnb_pd.loc[airbnb_pd['latitude_flag']==1, 'latitude']
    #airbnb_pd.loc[airbnb_pd['latitude_flag']==1, 'latitude'] = airbnb_pd.loc[airbnb_pd['latitude_flag']==1, 'latitude'].apply(lambda x: clean_latitude_pls(x))


    # In[23]:


    airbnb_pd.loc[airbnb_pd['latitude_flag']==1, 'latitude']


    # # Latitude checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[24]:


    lat_check_pd = airbnb_pd[airbnb_pd.latitude_flag==0]
    lat_check_pd = lat_check_pd[lat_check_pd.latitude.apply(lambda x:check_number(x,-90,90))==False]
    lat_check_pd[["id","latitude"]]


    # In[25]:


    def clean_longitude(df):
        #raise Exception("not yet have implementation")
        # do something here
        # validate and clean the longitude column
        lon_mask = df['longitude'].apply(lambda x: not check_number(x, -180, 180))
        df.loc[lon_mask, 'longitude_flag'] = 1
        df.loc[df['longitude'].isna(), 'longitude_flag'] = 2

        return df


    # In[26]:


    def clean_longi_pls(longi_string):
        # nikolaus: add - on the regex since it can be positive or negative
        longi_string = re.sub("[^0-9.\-]", "", longi_string)
        longi = float(longi_string)
        return longi


    # In[27]:


    airbnb_pd = clean_longitude(airbnb_pd)
    airbnb_pd[airbnb_pd['longitude_flag']==1]['longitude']


    # In[28]:


    airbnb_pd.loc[airbnb_pd['longitude_flag']==1, 'longitude'] = airbnb_pd.loc[airbnb_pd['longitude_flag']==1, 'longitude'].apply(lambda x: clean_longi_pls(x))
    airbnb_pd.loc[airbnb_pd['longitude_flag']==1, 'longitude']
    # nikolaus change longitude_flag back to 0
    airbnb_pd.loc[airbnb_pd['longitude_flag']==1, 'longitude_flag'] = 0
    airbnb_pd = clean_longitude(airbnb_pd) 


    # # Longitude checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[29]:


    lon_check_pd = airbnb_pd[airbnb_pd.longitude_flag==0]
    lon_check_pd = lon_check_pd[lon_check_pd.longitude.apply(lambda x:check_number(x,-180,180))==False]
    lon_check_pd[["id","longitude"]]


    # # cleanup room type
    # The "room_type" column in the dataset should contain one of the values defined in the list of allowed_room_type provided by the authority: ['Entire home/apt', 'Private room', 'Shared room', 'Hotel room']. Any value outside of this list needs to be adjusted to one of the allowed values.
    # 
    # If you are unsure about how to adjust the value or cannot find a suitable value, you can flag the row for deletion by setting the value of room_type_flag to 1. If the "room_type" column has a null value and you cannot decide on an appropriate value, you can set the value of room_type_flag to 2.

    # In[30]:


    allowed_room_type = ['Entire home/apt', 'Private room', 'Shared room', 'Hotel room']


    # In[31]:


    def clean_room_type(df):
        #raise Exception("not yet have implementation")
        # do something here
        df.loc[~df['room_type'].isin(allowed_room_type), 'room_type_flag'] = 1
        df.loc[df['room_type'].isna(), 'room_type_flag'] = 2 
        return df


    # In[32]:


    airbnb_pd = clean_room_type(airbnb_pd)
    res = airbnb_pd[airbnb_pd['room_type_flag']==1].groupby('room_type').count()
    airbnb_pd['room_type'] = airbnb_pd['room_type'].replace({'Entire home': 'Entire home/apt'})
    airbnb_pd[airbnb_pd['room_type_flag']==0]['room_type']
    # airbnb_pd[airbnb_pd['room_type_flag']==1]['room_type'] = 'Entire home/apt'
    # nikolaus change room_type_flag to 0
    airbnb_pd.loc[airbnb_pd['room_type_flag']==1, 'room_type_flag'] = 0
    airbnb_pd = clean_room_type(airbnb_pd)


    # # room_type checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[33]:


    room_type_pd = airbnb_pd[airbnb_pd.room_type_flag==0]
    room_type_pd = room_type_pd[room_type_pd.room_type.apply(lambda x: x not in allowed_room_type)]
    room_type_pd[["id","room_type"]]


    # # cleanup minimum_nights and number_of_reviews
    # 
    # The columns "minimum_nights" and "number_of_reviews" should both be integer values. "minimum_nights" should be a value between 1 and the number of days in a year (365), while "number_of_reviews" should be a value between 0 and 999999.
    # 
    # To check if these columns meet the criteria, we have provided a "check_integer" function. Any values that do not meet the criteria should be cleaned to meet the criteria for analysis.
    # 
    # If you are unsure what to do with a value or if it is a null value, you can flag the row for deletion by setting "minimum_nights_flag" or "number_of_reviews_flag" to 1 or 2, respectively.

    # In[34]:


    def check_integer(x,start=0,end=365):
        try:
            temp_x = int(x)
            if start <= temp_x <= end:
                return True
        except:
            return False


    # In[35]:


    def clean_minimum_nights(df):
        #raise Exception("not yet have implementation")
        # do something here
        nights_mask = df['minimum_nights'].apply(lambda x: not check_integer(x))
        df.loc[nights_mask, 'minimum_nights_flag'] = 1
        df.loc[df['minimum_nights'].isna(), 'minimum_nights_flag'] = 2
        return df


    # In[36]:


    airbnb_pd = clean_minimum_nights(airbnb_pd)
    airbnb_pd[airbnb_pd['minimum_nights_flag']==1]['minimum_nights']


    # In[37]:


    def clean_min_nights_pls(nights_string):
        nights_string = re.sub("[^0-9.]", "", nights_string)
        nights = int(nights_string)
        return nights


    # In[38]:


    airbnb_pd.loc[airbnb_pd['minimum_nights_flag']==1, 'minimum_nights'] = airbnb_pd.loc[airbnb_pd['minimum_nights_flag']==1, 'minimum_nights'].apply(lambda x: clean_min_nights_pls(x))
    airbnb_pd.loc[airbnb_pd['minimum_nights_flag']==1, 'minimum_nights']
    # nikolaus change minimum_nights_flag 1 to 0
    airbnb_pd.loc[airbnb_pd['minimum_nights_flag']==1, 'minimum_nights_flag'] = 0
    airbnb_pd = clean_minimum_nights(airbnb_pd)


    # # Minimum nights checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[39]:


    min_check_pd = airbnb_pd[airbnb_pd.minimum_nights_flag==0]
    min_check_pd = min_check_pd[min_check_pd.minimum_nights.apply(lambda x:check_integer(x,0,365))==False]
    min_check_pd[["id","minimum_nights"]]


    # In[40]:


    def clean_number_of_reviews(df):
        #raise Exception("not yet have implementation")
        # do something here
        reviews_mask = df['number_of_reviews'].apply(lambda x: not check_integer(x,0,999999))
        df.loc[reviews_mask, 'number_of_reviews_flag'] = 1
        df.loc[df['number_of_reviews'].isna(), 'number_of_reviews_flag']=2
        return df


    # In[41]:


    airbnb_pd = clean_number_of_reviews(airbnb_pd)


    # In[42]:


    airbnb_pd[airbnb_pd['number_of_reviews_flag']==1]['number_of_reviews']


    # In[43]:


    airbnb_pd.loc[airbnb_pd['number_of_reviews_flag']==1, 'number_of_reviews'] = airbnb_pd.loc[airbnb_pd['number_of_reviews_flag']==1, 'number_of_reviews'].apply(lambda x: clean_min_nights_pls(x))
    airbnb_pd.loc[airbnb_pd['number_of_reviews_flag']==1, 'number_of_reviews']
    # nikolaus change number_of_reviews_flag to 0
    airbnb_pd.loc[airbnb_pd['number_of_reviews_flag']==1, 'number_of_reviews_flag'] = 0
    airbnb_pd = clean_number_of_reviews(airbnb_pd)


    # # Clean number of reviews checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[44]:


    min_check_pd = airbnb_pd[airbnb_pd.number_of_reviews_flag==0]
    min_check_pd = min_check_pd[min_check_pd.number_of_reviews.apply(lambda x:check_integer(x,1,999999))==False]
    min_check_pd[["id","number_of_reviews"]]


    # # cleanup last_review
    # 
    # The "last_review" column should be in the format of ISO-date (yyyy-mm-dd). We have provided a "check_date" function to verify the date format.
    # 
    # If a value is outside the date format or is null and you are unsure how to handle it, you can flag the row for deletion by setting the "last_review_flag" to 1 or 2.
    # 

    # In[45]:


    from datetime import datetime
    def check_date(x,fmt="%Y-%m-%d"):
        try:
            datetime.strptime(x,fmt)
            return True
        except:
            return False


    # In[46]:


    def clean_last_reviews(df):
        #raise Exception("not yet have implementation")
        # do something here
        last_mask = df['last_review'].apply(lambda x: not check_date(x))
        df.loc[last_mask, 'last_review_flag'] = 1
        df.loc[df['last_review'].isna(), 'last_review_flag'] = 2
        return df


    # In[47]:


    def clean_date(date_str):
        # Convert the date string to a datetime object
        try:
            dt = datetime.strptime(date_str, '%B %d, %Y')
        except:
            return date_str
        # Convert the datetime object to an ISO-format string
        iso_date_str = dt.date().isoformat()
        print(iso_date_str)
        return iso_date_str


    # In[48]:


    airbnb_pd = clean_last_reviews(airbnb_pd)
    airbnb_pd.loc[airbnb_pd['last_review_flag']==1,'last_review']=airbnb_pd.loc[airbnb_pd['last_review_flag']==1,'last_review'].apply(clean_date)
    airbnb_pd[airbnb_pd['last_review_flag']==1]['last_review']
    # nikolaus change last_review_flag back to 0
    airbnb_pd.loc[airbnb_pd['last_review_flag']==1, 'last_review_flag'] = 0
    airbnb_pd = clean_last_reviews(airbnb_pd)


    # # Last Review checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[49]:


    last_review_check_pd = airbnb_pd[airbnb_pd.last_review_flag==0]
    last_review_check_pd = last_review_check_pd[last_review_check_pd.last_review.apply(lambda x:check_date(x))==False]
    last_review_check_pd[["id","last_review"]]


    # # save the dataset to csv

    # In[50]:


    return airbnb_pd

