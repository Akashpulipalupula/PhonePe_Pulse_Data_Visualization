import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import mysql.connector
import plotly.express as px
import json
import numpy as np


mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = '12345',
        database = 'phonepe')

cursor = mydb.cursor()


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
    #Visual 2
    query2 = "SELECT State, Year, Quarter, District, sum(Count), sum(Amount) FROM phonepe.map_trans where State = %s AND Year = %s GROUP BY Quarter, District ORDER BY Quarter"
    cursor.execute(query2, (State_selected, Year_selected))
    vis2 = cursor.fetchall()
    vis2 = pd.DataFrame(vis2)
    vis2.columns = ['State','Year','Quarter','District','Count','Amount']
    
    with col1:
        fig3 = px.bar(vis2, x='District', y='Count', color = 'Quarter',text_auto = '.2s', title = 'District wise Total number of Transaction Counts',)
        fig3.update_layout(xaxis={'categoryorder':'total descending'})
        st.plotly_chart(fig3)
    with col2:
        fig4 = px.bar(vis2, x='District', y='Amount', color = 'Quarter', text_auto = True, title = 'District wise Total of Transaction Amounts')
        fig4.update_layout(xaxis={'categoryorder':'total descending'})
        st.plotly_chart(fig4)
    #Visual3
    # Load India states GeoJSON data
    india_states = json.load(open(r"E:\Data Science Course - GUVI\Phone Pe Pulse\states_india.geojson", "r"))
    # Mapping state names to state codes
    state_id_map = {}
    for feature in india_states["features"]:
        feature["id"] = feature["properties"]["state_code"]
        state_id_map[feature["properties"]["st_nm"]] = feature["id"]

    query3 = "SELECT a.State, SUM(a.Transaction_count) AS Total_Transaction_count, SUM(a.Transaction_amount) AS Total_Transaction_amount,b.Latitude, b.Longitude FROM phonepe.top_trans AS a JOIN phonepe.lat_long AS b ON a.State = b.State WHERE a.Year = %s GROUP BY a.State, b.Latitude, b.Longitude ORDER BY a.State"
    cursor.execute(query3, (Year_selected,))
    vis3 = cursor.fetchall()
    vis3 = pd.DataFrame(vis3, columns=['State', 'Transaction_count', 'Transaction_amount', 'Latitude', 'Longitude'])
    vis3['Transaction_count'] = vis3['Transaction_count'].astype(int)
    vis3["id"] = vis3["State"].apply(lambda x: state_id_map.get(x))
    with col1:
        fig5 = px.choropleth_mapbox(vis3,geojson=india_states,locations="id",color="Transaction_count",
                    hover_name="State",hover_data=["Transaction_count"],title="Geovisual of the Transaction Count",
                    mapbox_style="carto-positron",zoom=2,center={"lat": 20.5937, "lon": 78.9629},
                    opacity=0.5,color_continuous_scale="Inferno")
        fig5.update_geos(
        lonaxis_range=[vis3['Longitude'].min(), vis3['Longitude'].max()],
        lataxis_range=[vis3['Latitude'].min(), vis3['Latitude'].max()],
        projection_type="orthographic")
        st.plotly_chart(fig5)
    with col2:
        fig6 = px.choropleth_mapbox(vis3,geojson=india_states,locations="id",color="Transaction_amount",
                    hover_name="State",hover_data=["Transaction_amount"],title="Geovisual of the Transaction Amount",
                    mapbox_style="carto-positron",zoom=2,center={"lat": 20.5937, "lon": 78.9629},
                    opacity=0.5,color_continuous_scale="Turbo")
        fig6.update_geos(
        lonaxis_range=[vis3['Longitude'].min(), vis3['Longitude'].max()],
        lataxis_range=[vis3['Latitude'].min(), vis3['Latitude'].max()],
        projection_type="orthographic")
        st.plotly_chart(fig6)

else:
    col1,col2  = st.columns(2)
    #Visual4
    query4 = 'SELECT * FROM phonepe.aggregated_user WHERE State = %s AND Year = %s'
    cursor.execute(query4, (State_selected, Year_selected))
    vis4 = cursor.fetchall()
    vis4 = pd.DataFrame(vis4)
    vis4.columns = ['State','Year','Quarter','Brands','Count','Percentage']
    with col1:
        fig7= px.sunburst(vis4, path=['Year', 'Quarter', 'Brands'], values='Count',title= 'Distribution of the Brands used')
        st.plotly_chart(fig7)
    with col2:
        fig8= px.sunburst(vis4, path=['Year', 'Quarter', 'Brands'], values='Percentage',title= 'Percentage_Distribution of the Brands used')
        st.plotly_chart(fig8)
    
    #Visual5
    query5 = 'SELECT State, Year, Quarter, District, sum(Registered_user), sum(App_opens) FROM phonepe.map_user WHERE State = %s AND Year = %s Group by Quarter,District;'
    cursor.execute(query5, (State_selected, Year_selected))
    vis5 = cursor.fetchall()
    vis5 = pd.DataFrame(vis5)
    vis5.columns = ['State','Year','Quarter','District','Registered_users','App_opens']
    with col1:
         fig9 = px.scatter(vis5, x="Registered_users", y="App_opens", color="District",
                 hover_name="District",hover_data=['Quarter'] , size_max=60, title= 'District wise Registered users v/s App_opens')
         st.plotly_chart(fig9)

    with col2:
        fig10 = px.treemap(vis5, path=[px.Constant("Year"), 'Quarter','District'], values='Registered_users', title = 'Tree map distribution of Registered Users')
        fig10.update_traces(root_color="lightgrey")
        fig10.update_layout(margin = dict(t=50, l=25, r=25, b=25))
        st.plotly_chart(fig10)
    