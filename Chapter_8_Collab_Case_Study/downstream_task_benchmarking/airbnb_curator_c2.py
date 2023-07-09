#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import abydos
from abydos import distance
import re
import math


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


def airbnb_curator_c2(airbnb_pd):

    # In[3]:


    airbnb_pd


    # In[4]:


    airbnb_pd.columns


    # In[5]:


    airbnb_pd.head()


    # # cleanup duplicate id
    # The ID column must contain unique values. If there are any duplicate values in this column, you will need to take action to ensure that each ID is unique. You can do this by either fixing the duplicates (if you want to keep them) or by flagging them for removal (1) using the id_flag column.

    # In[6]:


    def clean_duplicate_id(df):
        dup_ids = df[df.id_flag==0]
        dup_ids = dup_ids.groupby("id").count()[["name"]].reset_index()
        dup_ids = dup_ids[dup_ids.name>1]

        for id_ in list(dup_ids["id"]):
            temp_df=df[df["id"]==id_]

            try:
                id_=list(temp_df[temp_df.duplicated()].index)[0]  ## two rows are completely the same
                df.loc[id_,"id_flag"]=1
            except:
                index_ls=list(temp_df.index)   ## one row combine with another row equals to one of the original row
                temp_row_1=temp_df.loc[index_ls[0]]
                temp_row_2=temp_df.loc[index_ls[1]]
                row_comb=temp_df.loc[index_ls[0]].combine_first(temp_df.loc[index_ls[1]])
                try:
                    comp_result_1=list(set(list(temp_row_1.fillna(0)==row_comb.fillna(0))))[0]
                    comp_result_2=list(set(list(temp_row_2.fillna(0)==row_comb.fillna(0))))[0]
                    if comp_result_1==True:
                        df.loc[index_ls[1],"id_flag"]=1
                    elif comp_result_2==True:
                        df.loc[index_ls[0],"id_flag"]=1
                    else:
                        df.loc[index_ls[0],"id_flag"]=2
                        df.loc[index_ls[1],"id_flag"]=2  ##mark types which not belongs to the types mentioned above
                except:
                    print(id_)
        return df


    # In[7]:


    airbnb_pd = clean_duplicate_id(airbnb_pd)


    # # Duplicate IDS checking 
    # To ensure that all ID values in the dataset are unique, you should check for duplicate IDs. When you run the query to check for duplicates, there should be no rows returned, indicating that there are no duplicate ID values present in the dataset.

    # In[8]:


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

    # In[9]:


    def clean_host_id(df):
        dup_host_id = df[df.host_id_flag==0]
        dup_host_id = dup_host_id.groupby(["host_id","host_name"]).count()[["id"]].reset_index()
        dup_host_id = dup_host_id.groupby("host_id").count()["id"].reset_index()
        dup_host_id_df=dup_host_id[dup_host_id["id"]>1]
        dup_host_id_ls=list(dup_host_id_df["host_id"])
        for dup_host_id in dup_host_id_ls:
            temp_df=df[df["host_id"]==dup_host_id]
            temp_id_ls=list(temp_df.index)

            temp_match_ls=[]
            for id_ in temp_id_ls:
                string=temp_df.loc[id_,"host_name"]
                pattern = r"^[A-Z][a-z0-9_-]{3,19}$"  # here is only a simple way to clean the host_id
                match=re.match(pattern, string)
                if match:
                    temp_match_ls.append(string)
                else:
                    df.loc[id_,"host_id_flag"]=1  # while this method is also not perfect, since under same host_id, the host_name could be all wrong

            if len(set(temp_match_ls))!=1:
                for id_ in temp_id_ls:
                     df.loc[id_,"host_id_flag"]=2
        return df


    # In[10]:


    airbnb_pd = clean_host_id(airbnb_pd)


    # # Inconsistent Host ID checking 
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[11]:


    dup_host_id = airbnb_pd[airbnb_pd.host_id_flag==0]
    dup_host_id = dup_host_id.groupby(["host_id","host_name"]).count()[["id"]].reset_index()
    dup_host_id = dup_host_id.groupby("host_id").count()["id"].reset_index()
    dup_host_id[dup_host_id["id"]>1]


    # In[12]:


    airbnb_pd[airbnb_pd.host_id==488113016][["host_id","host_name"]]


    # In[13]:


    airbnb_pd[airbnb_pd.host_id.apply(lambda x: x in dup_host_id.host_id.values.tolist())].sort_values(["host_id","host_name"]).head(50)


    # In[14]:


    ## I have identified other issues, 1) the two names are pretty close to each other (which is very hard to detect 2) we can choose the right name based on the frequency


    # # cleanup neighbourhood
    # The neighbourhood column in the dataset should contain values that match the neighbourhoods defined in the official neighbourhood_list. However, there may be some values in the neighbourhood column that are incorrect due to errors or noise in the data.
    # 
    # To clean up the neighbourhood column, you can try to match each value in the column to a valid neighbourhood in the neighbourhood_list using a string distance function such as abydos. If you can successfully match a value in the neighbourhood column to a neighbourhood in the neighbourhood_list, you can replace the value in the dataset with the correct neighbourhood name.
    # 
    # However, if you are unsure about how to clean up a particular value in the neighbourhood column, or if you cannot match the value to a valid neighbourhood in the neighbourhood_list, you can flag the row for deletion by setting the neighbourhood_flag column to a value of 1. If the value in the neighbourhood column is null and you cannot make a determination based on other information in the dataset, you can set the neighbourhood_flag column to a value of 2 to indicate that the row should be included but the neighbourhood value is null.
    # 
    # You can also use the latitude and longitude columns in the dataset to help match values in the neighbourhood column to valid neighbourhoods in the neighbourhood_list. However, you should be aware that the latitude and longitude values may also contain errors or noise, so you should exercise caution when using these columns to clean up the neighbourhood column.

    # In[15]:


    neighbourhood_list = [ 'Hyde Park', 'West Town', 'Lincoln Park', 'Near West Side', 'Lake View', 'Dunning', 'Rogers Park', 'Logan Square', 'Uptown', 'Edgewater',    'North Center', 'Albany Park', 'West Ridge', 'Pullman', 'Irving Park',    'Beverly', 'Lower West Side', 'Near South Side', 'Near North Side',    'Grand Boulevard', 'Bridgeport', 'Humboldt Park', 'Chatham', 'Kenwood',    'Loop', 'West Lawn', 'Lincoln Square', 'Woodlawn', 'Avondale',    'Forest Glen', 'Portage Park', 'East Garfield Park', 'Washington Park',    'North Lawndale', 'Armour Square', 'South Lawndale', 'South Shore',    'Morgan Park', 'South Deering', 'West Garfield Park', 'Hermosa',    'Mckinley Park', 'Douglas', 'Hegewisch', 'West Elsdon', 'Norwood Park',    'Garfield Ridge', 'Austin', 'Belmont Cragin', 'Jefferson Park', 'Ashburn',    'Greater Grand Crossing', 'North Park', 'Oakland', 'Archer Heights',    'Edison Park', 'Englewood', 'Ohare', 'Brighton Park', 'Chicago Lawn',    'New City', 'South Chicago', 'Mount Greenwood', 'Montclare', 'Roseland',    'West Englewood', 'Calumet Heights', 'Auburn Gresham', 'Fuller Park',    'Avalon Park', 'Burnside', 'Clearing', 'Gage Park', 'West Pullman',    'Washington Heights', 'East Side']
    print(neighbourhood_list)


    # In[16]:


    def clean_neighbourhood(df):
        neighbourhood_check = df[df.neighbourhood_flag==0]
        neighbourhood_check = neighbourhood_check[neighbourhood_check.neighbourhood.apply(lambda x:x not in neighbourhood_list)]
        neigh_id_df=neighbourhood_check[["id","neighbourhood"]]
        neigh_ind_ls=list(neigh_id_df.index)

        for neigh_ind in neigh_ind_ls:
            sus_neigh_ipt=df.loc[neigh_ind,"neighbourhood"]

            try:

                temp_ls=[]
                for neighbour in neighbourhood_list:
                    temp_ls.append(distance.sim(sus_neigh_ipt, neighbour))
                if max(temp_ls)>0.5:
                    df.loc[neigh_ind,"neighbourhood"]=neighbourhood_list[temp_ls.index(max(temp_ls))]
                else:
                    df.loc[neigh_ind,"neighbourhood_flag"]=1
            except:
                df.loc[neigh_ind,"neighbourhood_flag"]=2

        return df


    # In[17]:


    airbnb_pd = clean_neighbourhood(airbnb_pd)


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
            return (start <= temp_x <= end)
        except:
            return False


    # In[35]:


    def clean_latitude(df):

        lat_check_pd = df[df.latitude_flag==0]
        lat_check_pd = lat_check_pd[lat_check_pd.latitude.apply(lambda x:check_number(x,-90,90))==False]
        wrong_lat_df=lat_check_pd[["id","latitude"]]
        lat_ind_ls=list(wrong_lat_df.index)

        lat_true = df[df.latitude_flag==0]
        lat_true = lat_true[lat_true.latitude.apply(lambda x:check_number(x,-90,90))==True]
        right_lati_ls=list(lat_true["latitude"])
        #print(right_lati_ls)
        avg_lati=sum([float(x) for x in right_lati_ls])/len(right_lati_ls)
        uni_period_loc=str(avg_lati).index(".")
        print(uni_period_loc)


        for lat_id in lat_ind_ls:
            pre_lati=df.loc[lat_id,"latitude"]
            if pd.isnull(pre_lati):
                df.loc[lat_id,"latitude_flag"]=2
            else:
                try:
                    temp=[]
                    period_loc=0
                    for c in str(pre_lati):
                        # nikolaus fixed the isdigit calling function and added checking for "-" sign 
                        if c.isdigit:                    
                        #if c.isdigit() or c=="-":
                            temp.append(c)
                            # nikolaus fixed missplaced =+1 shoud be +=1 (for iteration)
                            period_loc=+1                        
                            #period_loc+=1
                        #print(uni_period_loc,period_loc)
                        if period_loc==uni_period_loc:
                            # nikolaus added implementation for "." (based on the intention)
                            temp.append(c)
                            #temp.append(".")
                    #print(temp)
                    revised=float("".join(temp))
                    if check_number(revised)==True:
                        df.loc[lat_id,"latitude"]=revised
                    else:
                        df.loc[lat_id,"latitude_flag"]=1
                except:
                    print(lat_id)
                    df.loc[lat_id,"latitude_flag"]=1
        return df


    # In[36]:


    airbnb_pd = clean_latitude(airbnb_pd)


    # # Latitude checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[37]:


    lat_check_pd = airbnb_pd[airbnb_pd.latitude_flag==0]
    lat_check_pd = lat_check_pd[lat_check_pd.latitude.apply(lambda x:check_number(x,-90,90))==False]
    lat_check_pd[["id","latitude"]]


    # In[38]:


    def clean_longitude(df):
        lon_check_pd = airbnb_pd[airbnb_pd.longitude_flag==0]
        lon_check_pd = lon_check_pd[lon_check_pd.longitude.apply(lambda x:check_number(x,-180,180))==False]
        wrong_long_df=lon_check_pd[["id","longitude"]]
        long_ind_ls=list(wrong_long_df.index)


        long_true = df[df.longitude_flag==0]
        long_true = long_true[long_true.longitude.apply(lambda x:check_number(x,-180,180))==True]
        right_long_ls=list(long_true["longitude"])

        avg_long=sum([float(x) for x in right_long_ls])/len(right_long_ls)
        uni_period_loc=str(avg_long).index(".")

        for long_id in long_ind_ls:
            pre_long=df.loc[long_id,"longitude"]
            if pre_long==np.nan:
                df.loc[long_id,"longitude_flag"]=2
            else:
                try:
                    temp=[]
                    period_loc=0
                    for c in str(pre_long):
                        # nikolaus fixed the isdigit calling function and added checking for "-" sign 
                        if c.isdigit:
                        #if c.isdigit() or c=="-":
                            temp.append(c)
                            # nikolaus fixed missplaced =+1 shoud be +=1 (for iteration)
                            period_loc=+1
                            #period_loc+=1
                        if period_loc==uni_period_loc:
                            # nikolaus added implementation for "." (based on the intention)
                            temp.append(c)
                            #temp.append(".")
                    #print(temp)
                    revised=float("".join(temp))
                    if check_number(revised,-180,180)==True:
                        df.loc[long_id,"longitude"]=revised
                    else:
                        df.loc[long_id,"longitude_flag"]=1

                except:
                    print(long_id)
                    df.loc[long_id,"longitude_flag"]=1

        return df


    # In[39]:


    airbnb_pd = clean_longitude(airbnb_pd)


    # # Longitude checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[40]:


    lon_check_pd = airbnb_pd[airbnb_pd.longitude_flag==0]
    lon_check_pd = lon_check_pd[lon_check_pd.longitude.apply(lambda x:check_number(x,-180,180))==False]
    lon_check_pd[["id","longitude"]]


    # # cleanup room type
    # The "room_type" column in the dataset should contain one of the values defined in the list of allowed_room_type provided by the authority: ['Entire home/apt', 'Private room', 'Shared room', 'Hotel room']. Any value outside of this list needs to be adjusted to one of the allowed values.
    # 
    # If you are unsure about how to adjust the value or cannot find a suitable value, you can flag the row for deletion by setting the value of room_type_flag to 1. If the "room_type" column has a null value and you cannot decide on an appropriate value, you can set the value of room_type_flag to 2.

    # In[41]:


    allowed_room_type = ['Entire home/apt', 'Private room', 'Shared room', 'Hotel room']


    # In[42]:


    def clean_room_type(df):
        room_type_pd = df[df.room_type_flag==0]
        room_type_pd = room_type_pd[room_type_pd.room_type.apply(lambda x: x not in allowed_room_type)]
        room_type_pd[["id","room_type"]]
        room_id_ls=list(room_type_pd[["id","room_type"]].index)

        for room_ind in room_id_ls:
            sus_room_ipt=df.loc[room_ind,"room_type"]

            try:
                temp_ls=[]
                for room in allowed_room_type:
                    temp_ls.append(distance.sim(sus_room_ipt, room))
                if max(temp_ls)>0.5:
                    df.loc[room_ind,"room_type"]=allowed_room_type[temp_ls.index(max(temp_ls))]
                else:
                    df.loc[room_ind,"room_type_flag"]=1
            except:
                df.loc[room_ind,"room_type_flag"]=2

        return df


    # In[43]:


    airbnb_pd = clean_room_type(airbnb_pd)


    # # room_type checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[44]:


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

    # In[45]:


    def check_integer(x,start=0,end=365):
        try:
            temp_x = int(x)
            return (start <= temp_x <= end)
        except:
            return False


    # In[46]:


    def clean_minimum_nights(df):
        min_check_pd = df[df.minimum_nights_flag==0]
        min_check_pd = min_check_pd[min_check_pd.minimum_nights.apply(lambda x:check_integer(x,0,365))==False]
        wrong_min_ng= min_check_pd[["id","minimum_nights"]]
        ng_ind_ls=list(wrong_min_ng.index)

        for ng_id in ng_ind_ls:
            pre_ng=df.loc[ng_id,"minimum_nights"]

            try:
                if pre_ng==np.nan or math.isnan(pre_ng):
                    # print("here:" +str(ng_id))
                    df.loc[ng_id,"minimum_nights_flag"]=2
            except:
                try:
                    revised=int(''.join(c for c in pre_ng if c.isdigit()))
                    if check_integer(revised)==True:
                        df.loc[ng_id,"minimum_nights"]=revised
                    else:
                        df.loc[ng_id,"minimum_nights_flag"]=1

                except:
                    print(ng_id)

        return df


    # In[47]:


    airbnb_pd = clean_minimum_nights(airbnb_pd)


    # # Minimum nights checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[48]:


    min_check_pd = airbnb_pd[airbnb_pd.minimum_nights_flag==0]
    min_check_pd = min_check_pd[min_check_pd.minimum_nights.apply(lambda x:check_integer(x,0,365))==False]
    min_check_pd[["id","minimum_nights"]]


    # In[49]:


    def clean_number_of_reviews(df):
        min_check_pd = df[df.number_of_reviews_flag==0]
        min_check_pd = min_check_pd[min_check_pd.number_of_reviews.apply(lambda x:check_integer(x,0,999999))==False]
        wrong_min_nr=  min_check_pd[["id","number_of_reviews"]]
        nr_ind_ls=list(wrong_min_nr.index)

        for nr_id in nr_ind_ls:
            pre_nr=df.loc[nr_id,"number_of_reviews"]
            try:
                if pre_nr==np.nan or math.isnan(pre_nr):
                    df.loc[nr_id,"number_of_reviews_flag"]=2
            except:
                try:
                    revised=int(''.join(c for c in pre_nr if c.isdigit()))
                    if check_integer(revised)==True:
                        df.loc[nr_id,"number_of_reviews"]=revised
                    else:
                        df.loc[nr_id,"number_of_reviews_flag"]=1

                except:
                    print(nr_id)
        return df


    # In[50]:


    airbnb_pd = clean_number_of_reviews(airbnb_pd)


    # # Clean number of reviews checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[51]:


    min_check_pd = airbnb_pd[airbnb_pd.number_of_reviews_flag==0]
    min_check_pd = min_check_pd[min_check_pd.number_of_reviews.apply(lambda x:check_integer(x,0,999999))==False]
    min_check_pd[["id","number_of_reviews"]]


    # # cleanup last_review
    # 
    # The "last_review" column should be in the format of ISO-date (yyyy-mm-dd). We have provided a "check_date" function to verify the date format.
    # 
    # If a value is outside the date format or is null and you are unsure how to handle it, you can flag the row for deletion by setting the "last_review_flag" to 1 or 2.
    # 

    # In[52]:


    from datetime import datetime
    def check_date(x,fmt="%Y-%m-%d"):
        try:
            datetime.strptime(x,fmt)
            return True
        except:
            return False


    # In[53]:


    def clean_last_reviews(df):
        last_review_check_pd = df[df.last_review_flag==0]
        last_review_check_pd = last_review_check_pd[last_review_check_pd.last_review.apply(lambda x:check_date(x))==False]
        wrong_lr=last_review_check_pd[["id","last_review"]]
        lr_ind_ls=list(wrong_lr.index)

        for lr_id in lr_ind_ls:
            pre_lr=df.loc[lr_id,"last_review"]
            try:
                if pre_lr==np.nan or math.isnan(pre_lr):
                    df.loc[lr_id,"last_review_flag"]=2
            except:
                pre_lr=pre_lr.replace("/", " ")
                mon_ele=pre_lr.split()[0]
                date_ele=pre_lr.split()[1]
                year_ele=pre_lr.split()[2]
                revised_date="20"+year_ele+"-"+mon_ele+"-"+date_ele
                if check_date(revised_date)==True:
                    df.loc[lr_id,"last_review"]=revised_date
                else:
                    df.loc[lr_id,"last_review_flag"]=1
        return df


    # In[54]:


    airbnb_pd = clean_last_reviews(airbnb_pd)


    # # Last Review checking
    # 
    # This query should return zero rows once you implement the cleaning process

    # In[55]:


    last_review_check_pd = airbnb_pd[airbnb_pd.last_review_flag==0]
    last_review_check_pd = last_review_check_pd[last_review_check_pd.last_review.apply(lambda x:check_date(x))==False]
    last_review_check_pd[["id","last_review"]]


    # # save the dataset to csv

    # In[56]:


    return airbnb_pd
