# PhonePe_Pulse_Data_Visualization

# Problem Statement:
The Phonepe pulse Github repository contains a large amount of data related to various metrics and statistics. The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.

# Steps:
# 1) Cloning the Phonepe data from the github
Resoure Link:  https://github.com/PhonePe/pulse#readme

# 2) Importing the Required Libraries
import pandas as pd
import streamlit as st
import plotly.express as px
import os
import json
import matplotlib.pyplot as plt
import numpy as np
import mysql.connector as sql
import seaborn as sns

# 3) Extraing the data from the various file

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

# 4) Uploading the data in MySQL database by cretaing the relavent databases
   Creating aggreatedga_transaction table

cursor.execute("create table aggregated_transaction (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount double)")

for i,row in df_aggregated_trans.iterrows():
    sql = "INSERT INTO aggregated_transaction VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    mydb.commit()
    
# 5) Creating the dashboard with various visuls using the streamlit application.
   st.set_page_config(layout = 'wide')
st.title("Phone Pe Pulse Data Visulaization")
st.markdown("Selct the filter to visualize various charts")
col1,col2,col3 = st.columns(3)
with col1:
        State_selected = st.selectbox('Select the State', ('Andaman & Nicobar Island','Andhra Pradesh','Arunanchal Pradesh','Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh','Dadra and Nagar Haveli and Daman and Diu','Delhi','Goa','Gujarat','Haryana','Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala', 'Ladakh','Lakshadweep','Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry', 'Punjab','Rajasthan', 'Sikkim','Tamil Nadu','Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal'))
with col2:
        Year_selected = st.selectbox('Select the Year', ('2018','2019','2020','2021','2022',))
with col3:
        Criteria_selected = st.selectbox('Select the criteria',('Transactions','Users'))
if Criteria_selected == 'Transactions':
    col1,col2 = st.columns(2)
    #Visual 1
    
    query1 = "SELECT State, Year, Quarter, Transaction_type, SUM(Transaction_count), SUM(Transaction_amount) FROM phonepe.aggregated_transaction WHERE State = %s AND Year = %s GROUP BY Quarter, Transaction_type ORDER BY Quarter"
    cursor.execute(query1, (State_selected, Year_selected))
    vis1 = cursor.fetchall()
    vis1 = pd.DataFrame(vis1)
    vis1.columns = ['State','Year','Quarter','Transaction_type','Transaction_count','Transaction_amount']

    with col1:
        fig1 = px.line(vis1, x = 'Quarter', y = 'Transaction_count', color="Transaction_type", title = 'Variation of Transaction_count for the selected Year')
        st.plotly_chart(fig1)
    with col2:
        fig1 = px.line(vis1, x = 'Quarter', y = 'Transaction_amount', color="Transaction_type",title = 'Variation of Transaction_amount for the selected Year')
        st.plotly_chart(fig1)
