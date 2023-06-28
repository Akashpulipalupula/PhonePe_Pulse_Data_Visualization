import pandas as pd
import streamlit as st
import plotly.express as px
import os
import json
import matplotlib.pyplot as plt
import numpy as np
import mysql.connector as sql
import seaborn as sns

mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = '12345',
        database = 'phonepe')

cursor = mydb.cursor()

# Extracting the data for state wise aggregated tractions

path ="E:/Data Science Course - GUVI/Phone Pe Pulse/pulse-master/data/aggregated/transaction/country/india/state/"
aggregated_trans_list = os.listdir(path)

columns = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
            'Transaction_amount': []}
for state in aggregated_trans_list:
    current_state = path + state + "/"
    aggregated_year_list = os.listdir(current_state)
    
    for year in aggregated_year_list:
        current_year = current_state + year + "/"
        aggregated_file_list = os.listdir(current_year)
        
        for file in aggregated_file_list:
            current_file = current_year + file
            data = open(current_file, 'r')
            year_trans_data = json.load(data)
            
            for i in year_trans_data['data']['transactionData']:
                name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                columns['Transaction_type'].append(name)
                columns['Transaction_count'].append(count)
                columns['Transaction_amount'].append(amount)
                columns['State'].append(state)
                columns['Year'].append(year)
                columns['Quarter'].append(int(file.strip('.json')))
                
df_aggregated_trans = pd.DataFrame(columns)

df_aggregated_trans['State'] = df_aggregated_trans['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh', 'arunachal-pradesh':'Arunanchal Pradesh',
       'assam':'Assam', 'bihar':'Bihar', 'chandigarh':'Chandigarh', 'chhattisgarh':'Chhattisgarh',
       'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 'delhi': 'Delhi', 'goa':'Goa', 'gujarat': 'Gujarat',
       'haryana':'Haryana','himachal-pradesh':'Himachal Pradesh', 'jammu-&-kashmir':'Jammu & Kashmir', 'jharkhand':'Jharkhand',
       'karnataka':'Karnataka', 'kerala':'Kerala', 'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 'madhya-pradesh':'Madhya Pradesh',
       'maharashtra':'Maharashtra', 'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 'nagaland':'Nagaland',
       'odisha':'Odisha', 'puducherry':'Puducherry', 'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
       'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana',
'tripura':'Tripura', 'uttar-pradesh':'Uttar Pradesh',
       'uttarakhand':'Uttarakhand', 'west-bengal':'West Bengal'})

df_aggregated_trans

df_aggregated_trans.to_csv('aggregated_transactions.csv',index=False)

#Creating aggreatedga_transaction table

cursor.execute("create table aggregated_transaction (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount double)")

for i,row in df_aggregated_trans.iterrows():
    sql = "INSERT INTO aggregated_transaction VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    mydb.commit()

# Extracting the data for state wise aggregated users

path = "E:/Data Science Course - GUVI/Phone Pe Pulse/pulse-master/data/aggregated/user/country/india/state/"

aggregated_user_list = os.listdir(path)

columns = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [],
            'Percentage': []}
for state in aggregated_user_list:
    current_state = path + state + "/"
    aggregated_year_list = os.listdir(current_state)
    
    for year in aggregated_year_list:
        current_year = current_state + year + "/"
        aggregated_file_list = os.listdir(current_year)

        for file in aggregated_file_list:
            current_file = current_year + file
            data = open(current_file, 'r')
            year_user_data = json.load(data)
            try:
                for i in year_user_data["data"]["usersByDevice"]:
                    brand_name = i["brand"]
                    counts = i["count"]
                    percents = i["percentage"]
                    columns["Brands"].append(brand_name)
                    columns["Count"].append(counts)
                    columns["Percentage"].append(percents)
                    columns["State"].append(state)
                    columns["Year"].append(year)
                    columns["Quarter"].append(int(file.strip('.json')))
            except:
                pass
df_aggregated_user = pd.DataFrame(columns)
df_aggregated_user['State'] = df_aggregated_user['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh', 'arunachal-pradesh':'Arunanchal Pradesh',
       'assam':'Assam', 'bihar':'Bihar', 'chandigarh':'Chandigarh', 'chhattisgarh':'Chhattisgarh',
       'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 'delhi': 'Delhi', 'goa':'Goa', 'gujarat': 'Gujarat',
       'haryana':'Haryana','himachal-pradesh':'Himachal Pradesh', 'jammu-&-kashmir':'Jammu & Kashmir', 'jharkhand':'Jharkhand',
       'karnataka':'Karnataka', 'kerala':'Kerala', 'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 'madhya-pradesh':'Madhya Pradesh',
       'maharashtra':'Maharashtra', 'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 'nagaland':'Nagaland',
       'odisha':'Odisha', 'puducherry':'Puducherry', 'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
       'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana',
'tripura':'Tripura', 'uttar-pradesh':'Uttar Pradesh',
       'uttarakhand':'Uttarakhand', 'west-bengal':'West Bengal'})
 
df_aggregated_user

df_aggregated_user.to_csv('aggregated_user.csv',index=False)

#Creating aggregated_user table

cursor.execute("create table aggregated_user (State varchar(100), Year int, Quarter int, Brands varchar(100), Count int, Percentage double)")

for i,row in df_aggregated_user.iterrows():
    sql = "INSERT INTO aggregated_user VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    mydb.commit()

# Extracting the data for state wise map transactions
path = "E:/Data Science Course - GUVI/Phone Pe Pulse/pulse-master/data/map/transaction/hover/country/india/state/"

map_trans_list = os.listdir(path)

columns = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [],
            'Amount': []}

for state in map_trans_list:
    current_state = path + state + "/"
    map_year_list = os.listdir(current_state)
    
    for year in map_year_list:
        current_year = current_state + year + "/"
        map_file_list = os.listdir(current_year)
        
        for file in map_file_list:
            current_file = current_year + file
            data = open(current_file, 'r')
            map_trans_data = json.load(data)
            
            for i in map_trans_data["data"]["hoverDataList"]:
                district = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns["District"].append(district)
                columns["Count"].append(count)
                columns["Amount"].append(amount)
                columns['State'].append(state)
                columns['Year'].append(year)
                columns['Quarter'].append(int(file.strip('.json')))
                
df_map_trans = pd.DataFrame(columns)
df_map_trans['State'] = df_map_trans['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh', 'arunachal-pradesh':'Arunanchal Pradesh',
       'assam':'Assam', 'bihar':'Bihar', 'chandigarh':'Chandigarh', 'chhattisgarh':'Chhattisgarh',
       'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 'delhi': 'Delhi', 'goa':'Goa', 'gujarat': 'Gujarat',
       'haryana':'Haryana','himachal-pradesh':'Himachal Pradesh', 'jammu-&-kashmir':'Jammu & Kashmir', 'jharkhand':'Jharkhand',
       'karnataka':'Karnataka', 'kerala':'Kerala', 'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 'madhya-pradesh':'Madhya Pradesh',
       'maharashtra':'Maharashtra', 'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 'nagaland':'Nagaland',
       'odisha':'Odisha', 'puducherry':'Puducherry', 'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
       'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana',
'tripura':'Tripura', 'uttar-pradesh':'Uttar Pradesh',
       'uttarakhand':'Uttarakhand', 'west-bengal':'West Bengal'})
df_map_trans

df_map_trans.to_csv('map_transaction.csv',index=False)

#Creating map_trans table

cursor.execute("create table map_trans (State varchar(100), Year int, Quarter int, District varchar(100), Count int, Amount double)")

for i,row in df_map_trans.iterrows():
    sql = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    mydb.commit()

# Extracting the state wise map user data

path = "E:/Data Science Course - GUVI/Phone Pe Pulse/pulse-master/data/map/user/hover/country/india/state/"

map_user_list = os.listdir(path)

columns = {"State": [], "Year": [], "Quarter": [], "District": [],
            "RegisteredUser": [], "AppOpens": []}

for state in map_user_list:
    current_state = path + state + "/"
    map_year_list = os.listdir(current_state)
    
    for year in map_year_list:
        current_year = current_state + year + "/"
        map_file_list = os.listdir(current_year)
        
        for file in map_file_list:
            current_file = current_year + file
            data = open(current_file, 'r')
            map_user_data = json.load(data)
            
            for i in map_user_data["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appOpens = i[1]['appOpens']
                columns["District"].append(district)
                columns["RegisteredUser"].append(registereduser)
                columns["AppOpens"].append(appOpens)
                columns['State'].append(state)
                columns['Year'].append(year)
                columns['Quarter'].append(int(file.strip('.json')))
                
df_map_user = pd.DataFrame(columns)
df_map_user['State'] = df_map_user['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh', 'arunachal-pradesh':'Arunanchal Pradesh',
       'assam':'Assam', 'bihar':'Bihar', 'chandigarh':'Chandigarh', 'chhattisgarh':'Chhattisgarh',
       'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 'delhi': 'Delhi', 'goa':'Goa', 'gujarat': 'Gujarat',
       'haryana':'Haryana','himachal-pradesh':'Himachal Pradesh', 'jammu-&-kashmir':'Jammu & Kashmir', 'jharkhand':'Jharkhand',
       'karnataka':'Karnataka', 'kerala':'Kerala', 'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 'madhya-pradesh':'Madhya Pradesh',
       'maharashtra':'Maharashtra', 'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 'nagaland':'Nagaland',
       'odisha':'Odisha', 'puducherry':'Puducherry', 'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
       'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana',
'tripura':'Tripura', 'uttar-pradesh':'Uttar Pradesh',
       'uttarakhand':'Uttarakhand', 'west-bengal':'West Bengal'})
df_map_user

df_map_user.to_csv('map_user.csv',index=False)

#Creating map_user table

cursor.execute("create table map_user (State varchar(100), Year int, Quarter int, District varchar(100), Registered_user int, App_opens int)")

for i,row in df_map_user.iterrows():
    sql = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    mydb.commit()

#Creating map_user table

cursor.execute("create table map_user (State varchar(100), Year int, Quarter int, District varchar(100), Registered_user int, App_opens int)")

for i,row in df_map_user.iterrows():
    sql = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    mydb.commit()

df_top_trans.to_csv('top_transactions.csv',index=False)

# Creating top_trans table

cursor.execute("create table top_trans (State varchar(100), Year int, Quarter int, Pincode int, Transaction_count int, Transaction_amount double)")

for i,row in df_top_trans.iterrows():
    sql = "INSERT INTO top_trans VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    mydb.commit()

#Dataframe of top users

path = "E:/Data Science Course - GUVI/Phone Pe Pulse/pulse-master/data/top/user/country/india/state/"
top_user_list = os.listdir(path)
columns = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
            'RegisteredUsers': []}

for state in top_user_list:
    current_state = path + state + "/"
    top_year_list = os.listdir(current_state)
    
    for year in top_year_list:
        current_year = current_state + year + "/"
        top_file_list = os.listdir(current_year)
        
        for file in top_file_list:
            current_file = current_year + file
            data = open(current_file, 'r')
            top_user_data = json.load(data)
            
            for i in top_user_data['data']['pincodes']:
                name = i['name']
                registeredUsers = i['registeredUsers']
                columns['Pincode'].append(name)
                columns['RegisteredUsers'].append(registeredUsers)
                columns['State'].append(state)
                columns['Year'].append(year)
                columns['Quarter'].append(int(file.strip('.json')))
df_top_user = pd.DataFrame(columns)  
df_top_user['State'] = df_top_user['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh', 'arunachal-pradesh':'Arunanchal Pradesh',
       'assam':'Assam', 'bihar':'Bihar', 'chandigarh':'Chandigarh', 'chhattisgarh':'Chhattisgarh',
       'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 'delhi': 'Delhi', 'goa':'Goa', 'gujarat': 'Gujarat',
       'haryana':'Haryana','himachal-pradesh':'Himachal Pradesh', 'jammu-&-kashmir':'Jammu & Kashmir', 'jharkhand':'Jharkhand',
       'karnataka':'Karnataka', 'kerala':'Kerala', 'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 'madhya-pradesh':'Madhya Pradesh',
       'maharashtra':'Maharashtra', 'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 'nagaland':'Nagaland',
       'odisha':'Odisha', 'puducherry':'Puducherry', 'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
       'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana',
'tripura':'Tripura', 'uttar-pradesh':'Uttar Pradesh',
       'uttarakhand':'Uttarakhand', 'west-bengal':'West Bengal'})
df_top_user 

df_top_user.to_csv('top_user.csv',index=False)

#Creating top_user table

cursor.execute("create table top_user (State varchar(100), Year int, Quarter int, Pincode int, Registered_users int)")

for i,row in df_top_user.iterrows():
    sql = "INSERT INTO top_user VALUES (%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    mydb.commit()

import csv

csv_file_path  = r'C:\Users\P AKASH\Downloads\Longitude_Latitude_State_Table.csv'

# Read the CSV file and insert data into MySQL table
with open(csv_file_path, 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip the header row if present
    for row in csv_data:
        query = f"INSERT INTO {'lat_long'} VALUES ({','.join(['%s'] * len(row))})"
        cursor.execute(query, tuple(row))

# Commit the changes and close the connection
mydb.commit()
cursor.close()

df