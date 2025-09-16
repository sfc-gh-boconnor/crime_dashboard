############################################################################################

## Initialise the required packages for processing.

import os # To access masked passwords for security.
import geopandas as gpd # For geospatial manipulation.
import pandas as pd # For standard data wrangling.
import numpy as np # For building quantitive functions in the VAWG risk model.
import streamlit as st # For building the geospatial dashboard.
import requests # Import requests to support API calls.
import pyproj # For coordinate conversions
import json
from cryptography.hazmat.primitives import serialization

## Dashboard building packages

import folium as f # For geospatial mapping.
import streamlit_folium # For geospatial mapping.
import branca # For legend creation
from streamlit_folium import st_folium # For integrating maps into streamlit.
from folium.plugins import MarkerCluster
import streamlit as st # For dashboard building.
import altair as alt # For graphic visualisation.

## Import the Snowflake API connections.

from snowflake.snowpark import Session # Allows a direct connection back to Snowflake.
from shapely.geometry import Point # Some spatial functions
from shapely.geometry import shape, mapping # For geometry conversions
from datetime import datetime # For time slider functionality

############################################################################################
# Set some page config settings
st.set_page_config(
    page_title="CRISP Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded")
# Space for user parameters - requires masking

API_KEY = st.secrets["api_key"]

# Define the coordinate systems
crs_bng = pyproj.CRS("EPSG:27700")  # British National Grid
crs_wgs = pyproj.CRS("EPSG:4326")   # EPSG:4326 (lat/lon)
crs_utm = pyproj.CRS("EPSG:3857")   # EPSG:4326 (UTM)

############################################################################################

## Specify the user parameters to establish a Snowpark connection.

private_key_string = st.secrets["pem"]

private_key = serialization.load_pem_private_key(
    private_key_string.encode(),
    password=None,
)

@st.cache_resource
def init_snowpark_session():
    connection_parameters = {
    "account": st.secrets["account"],
    "user": st.secrets["user"],
    "private_key": private_key,
    "role": "ANALYST",
    "warehouse": "COMPUTE_WH",
    "database": st.secrets["database"],
    "schema": st.secrets["schema"]
    }
    return Session.builder.configs(connection_parameters).create()

# Establish a connection to the Snowflake data warehouses,
session = init_snowpark_session()

############################################################################################

# Create transformer
transformer = pyproj.Transformer.from_crs(crs_bng, crs_wgs, always_xy=True)

# Create transformer
transformerutm = pyproj.Transformer.from_crs(crs_bng, crs_utm, always_xy=True)

# Places function for calls
def search_os_places(query):
    params = {
        "key": API_KEY,
        "query": query,
        "fq": "COUNTRY_CODE:E COUNTRY_CODE:S COUNTRY_CODE:W",  # Exclude islands
        "dataset": "LPI",
        "maxresults": 1
    }
    response = requests.get("https://api.os.uk/search/places/v1/find", params=params)
    data = response.json()

    return data

# Data transformation function
@st.cache_data(ttl=600)
def get_geodata(source_id: str, query: str, crs: str = 'EPSG:4326'):
    df = session.sql(query).to_pandas()
    df['geometry'] = df['GEOGRAPHY'].apply(lambda x: shape(json.loads(x)))
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=crs)
    gdf = gdf.drop(columns=['GEOGRAPHY'])
    return gdf

# Query builder
def build_query(source, lon, lat, buf_distance):
    return f"""
        SELECT a.*
        FROM {source} a
        WHERE ST_DISTANCE(
            a.GEOGRAPHY,
            TO_GEOGRAPHY('POINT({lon} {lat})')
        ) <= {buf_distance}
    """

# Chart generation function

def generate_crime_trend_chart(df, title):

    # Create a max count variable to ensure the data does not leave the top of the chart.
    max_count = df['Count'].max()

    # Define custom blue shades for each group
    unique_groups = df['Group'].unique()
    blue_palette = ['#1C56F6', '#226E9C', '#3C93C2', '#9EC9E2', '#E4F1F7']
    color_scale = alt.Scale(
        domain=list(unique_groups),
        range=blue_palette[:len(unique_groups)]
    )

    chart = alt.Chart(df).mark_line(point=True).encode(
        x=alt.X(
            'CHARTMONTH:T',
            title='Month',
            axis=alt.Axis(
                labelAngle=0,
                format='%B',
                tickCount='month'
            )
        ),
        y=alt.Y(
            'Count:Q',
            title='Crime Count',
            scale=alt.Scale(domain=[0, max_count * 1.2])
        ),
        color=alt.Color('Group:N', title='Crime classifications', scale=color_scale),
        tooltip=['CHARTMONTH:T', 'Count:Q', 'Group:N']
    ).properties(
        title=title,
        width='container',
        height=300
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).configure_title(
        fontSize=16,
        color='white',
        anchor='start'
    )

    return chart

############################################################################################

## Connect to the database using snowpark functions

############################################################################################

# Space for building the dashboard



# Custom CSS for background and sidebar
st.markdown("""
    <style>
        .stApp {
            background-color: #f4f1ef;
        }
        [data-testid="stSidebar"] {
            background-color: #FFFFFF;
        }
        [data-testid="stSidebar"] * {
            color: #131312;
        }
        .stTextInput > div > div > input {
            color: #131312;
            background-color: #f4f1ef;
        }
        input[type="number"] {
            background-color: #f4f1ef;
            color: #131312;
            padding: 5px;
            border-radius: 5px;
            border: 1px solid #ccc;
        }
        .streamlit-expanderHeader {
        background-color: #f4f1ef;
        font-weight: normal;
        font-size: 18px;
        color: #131312;
        }
        div[data-baseweb="select"] > div {
            background-color: white !important;
            border-radius: 4px;
            padding: 2px;
        }
    </style>
""", unsafe_allow_html=True)

############################################################################################

#alt.themes.enable("light")

# Add a side bar which will be used as our primary method for filtering
with st.sidebar:

    # Create side bar columns
    bars = st.columns([1, 6]) 

    with bars[1]:

        st.markdown(
            "<h1 style='margin: 0; font-size: 32px;'>Geolocate your address</h1>",
            unsafe_allow_html=True
        )

        st.caption("This dashboard provides insight into the crime characteristics using real time analytics within your local community.")

        # Add some padding by the logo
        spacer = st.empty()
        spacer.markdown('<div style="height: 0.001px;"></div>', unsafe_allow_html=True)

        # Add the title for the dashboard
        st.subheader('Location filter')

        # Add the search functionality that taps into the OS Places API
        query = st.text_input("Enter an address or postcode:")

        # Add the search functionality that taps into the OS Places API
        buf_distance = st.number_input("Enter address search distance (m)", min_value=0, max_value=1000, step=100)

        # Add a drop down filter option to select different datasets
        layer_options = ["Buildings", "Street Lights", "Land Use", "Greenspace", "Crime"]
        selected_layers = {}
        selected_layers_drop = ["No value"]

        # Add the options within an expander function. We want to use this to allow the user to select different layers
        # Add the title for the dashboard
        if buf_distance > 0:
          st.subheader('Data Selection')

          with st.expander("Select Layers"):

              # Add smarter filtering capacity
              for layer in layer_options:

                  # Add the check box logic
                  is_checked = st.checkbox(layer)
                  selected_layers[layer] = is_checked

                  # Add the next value
                  if is_checked:
                      selected_layers_drop.append(layer)

        # Add a simple if query to return results from the query perhaps add functionality to specify the number of addresses you want?
        # for now lets just return the first entry?
        if query:
            results = search_os_places(query)
            addresses = results.get("results", [])
            
            if addresses:
                # Bake a message to say an address has been matched
                st.markdown("""
                    <div style="
                        background-color: #f4f1ef;
                        padding: 10px;
                        border-left: 5px solid #1C56F6;
                        border-radius: 4px;
                        font-weight: normal;
                        color: #333;
                        margin-top: 10px;
                        margin-bottom: 10px;
                    ">
                        Address match found
                    </div>
                """, unsafe_allow_html=True)

                # Add a sub header
                st.subheader("Selected address:")

                for item in addresses:
                    # Address metadata things
                    address = item["LPI"]["ADDRESS"]
                    admin = item["LPI"]["ADMINISTRATIVE_AREA"]
                    chr = item["LPI"]["BLPU_STATE_CODE_DESCRIPTION"]
                    clss = item["LPI"]["CLASSIFICATION_CODE_DESCRIPTION"]
                    mtchsc, mtch = item["LPI"]["MATCH"], item["LPI"]

                    # Address coordinate things
                    X, Y = item["LPI"]["X_COORDINATE"], item["LPI"]["Y_COORDINATE"]
                    lon, lat = transformer.transform(X, Y)
                    XU, YU = transformerutm.transform(X, Y)

                    # Address overview - keep this simple
                    st.markdown(f"""
                        <div style="background-color: rgba(243, 242, 242, 0.4); padding: 15px; border-radius: 8px;">
                            <table style="width: 100%; border-collapse: collapse;">
                                <tr>
                                    <td style="font-weight: normal; padding: 6px;">Administrative area:</td>
                                    <td style="padding: 6px;">{admin}</td>
                                </tr>
                                <tr>
                                    <td style="font-weight: normal; padding: 6px;">Address:</td>
                                    <td style="padding: 6px;">{address}</td>
                                </tr>
                                <tr>
                                    <td style="font-weight: normal; padding: 6px;">Classification:</td>
                                    <td style="padding: 6px;">{clss}</td>
                                </tr>
                                <tr>
                                    <td style="font-weight: normal; padding: 6px;">Address status:</td>
                                    <td style="padding: 6px;">{chr}</td>
                                </tr>
                                <tr>
                                    <td style="font-weight: normal; padding: 6px;">Matching confidence:</td>
                                    <td style="padding: 6px;">Score: {mtchsc}</td>
                                </tr>
                            </table>
                        </div>
                    """, unsafe_allow_html=True)

            else:
                st.markdown("""
                    <div style="
                        background-color: #f4f1ef;
                        padding: 10px;
                        border-left: 5px solid #1C56F6;
                        border-radius: 4px;
                        font-weight: normal;
                        color: #333;
                        margin-top: 10px;
                        margin-bottom: 10px;
                    ">
                        No address match found
                    </div>
                """, unsafe_allow_html=True)

        # Build in cases for building the buffer geometry which we will use to pull data through.
        if query:
            if addresses:
                
                layer_options = ["Buildings", "Street Lights", "Land Use", "Greenspace", "Crime"]

                # Use a buffer filter to optimise queries.
                if buf_distance > 0:
                    
                    # We want to create a buffer we can use to set the map frame with. Remember to convert meters to degrees.
                    buffer_geom = Point(lon, lat).buffer(buf_distance / 111320)

                    # Generate a geodataframe for the buffer in the relevant coordinate system (will probably have to change this later).
                    buffer_gdf = gpd.GeoDataFrame(geometry=[buffer_geom], crs="EPSG:4326")

                    # Retrieve the bounds of the buffer.
                    bounds = buffer_gdf.total_bounds

                    if selected_layers.get("Buildings"):

                        # Use the query builder to create a query for buildings.
                        building_query = build_query("DATA_OPS_TESTING_DB.COLLATERAL_SCH.ES_FINAL_NGDBUILD_INDEXED", lon, lat, buf_distance)

                        # Transform the dataframe into a geodataframe.
                        Test_gdf = get_geodata("buildings", building_query)

                    if selected_layers.get("Street Lights"):

                        # Use the query builder to create a query for street lights.
                        streetlight_query = build_query("DATA_OPS_TESTING_DB.COLLATERAL_SCH.ES_FINAL_NGDSTRTLGHT_INDEXED", lon, lat, buf_distance)

                        # Transform the dataframe into a geodataframe.
                        stlgt_gdf = get_geodata("streetlights", streetlight_query)

                    if selected_layers.get("Land Use"):

                        # Use the query builder to create a query for landuse.
                        landuse_query = build_query("DATA_OPS_TESTING_DB.COLLATERAL_SCH.ES_NGDLUSITE_AOI", lon, lat, buf_distance)

                        # Transform the dataframe into a geodataframe.
                        landuse_gdf = get_geodata("landuse", landuse_query)

                    if selected_layers.get("Greenspace"): 

                        # Use the query builder to create a query for greenspaces.
                        greenspace_query = build_query("DATA_OPS_TESTING_DB.COLLATERAL_SCH.ES_OPENGS_AOI", lon, lat, buf_distance)

                        # Transform the dataframe into a geodataframe.
                        greenspace_gdf = get_geodata("greenspace", greenspace_query)
                    
                    if selected_layers.get("Crime"): 

                        # Use the query builder to create a query for the crime query.
                        crime_query = build_query("DATA_OPS_TESTING_DB.COLLATERAL_SCH.ES_FINAL_CRIME_INDEXED", lon, lat, buf_distance)

                        # Transform the dataframe into a geodataframe.
                        crime_gdf = get_geodata("crime", crime_query)

                else: 
                    st.markdown("""
                    <div style="
                        background-color: #f4f1ef;
                        padding: 10px;
                        border-left: 5px solid #1C56F6;
                        border-radius: 4px;
                        font-weight: normal;
                        color: #333;
                        margin-top: 10px;
                        margin-bottom: 10px;
                    ">
                        Please increase buffer size
                    </div>
                """, unsafe_allow_html=True)


############################################################################################

# Add initial dropdown functions
mapcols = st.columns([1, 8])

# Add initial dropdown functions
cols = st.columns([2, 1])

# Create an empty dataframe
crime_filtered = pd.DataFrame()

with cols[1]:

    # Add a subheading for the filtering section
    st.markdown(
            """
            <h1 style='margin-top: 0px; margin-bottom: 0; font-size: 32px; font-family: Arial;'>
                Community insight
            </h1>
            """,
            unsafe_allow_html=True
        )
    
    # Add instructions to help users use the filters
    st.caption("Please customise your search requirements to retrieve insight about your chosen area")

    # Add the sub columns section
    subcol = st.columns([1,1])

    if selected_layers.get("Crime"):

        # Add temporal functionality converting the dae into the right format
        crime_gdf['RANDOM_DATE'] = pd.to_datetime(crime_gdf['RANDOM_DATE'])

        # Filter by crime options
        crime_options = crime_gdf['CRIME_TYPE'].dropna().unique()

        # Filter by crime options
        selected_crimes = []

        # Add the second filter for types of crime
        with st.expander("Select Crime"):
            for crime in crime_options:
                if st.checkbox(crime, value=False):
                    selected_crimes.append(crime)

            if selected_crimes:
                subcol = st.columns(2)
                with subcol[0]:
                    start_date = st.date_input("Start date", crime_gdf['RANDOM_DATE'].min().date())

                with subcol[1]:
                    end_date = st.date_input("End date", crime_gdf['RANDOM_DATE'].max().date())

                # Filter based on both crime type and date
                crime_filtered = crime_gdf[
                    (crime_gdf["CRIME_TYPE"].isin(selected_crimes)) &
                    (crime_gdf["RANDOM_DATE"].dt.date >= start_date) &
                    (crime_gdf["RANDOM_DATE"].dt.date <= end_date)
                ]
            
            else:
                st.markdown("""
                            <div style="
                                background-color: #f4f1ef;
                                padding: 10px;
                                border-left: 5px solid #1C56F6;
                                border-radius: 4px;
                                font-weight: normal;
                                color: #333;
                                margin-top: 10px;
                                margin-bottom: 10px;
                            ">
                                Please select crime type
                            </div>
                        """, unsafe_allow_html=True)
                
                # Fallback: filter only by crime type
                crime_filtered = crime_gdf[crime_gdf["CRIME_TYPE"].isin(selected_crimes)]
    else:
       st.markdown("""
                    <div style="
                        background-color: #f4f1ef;
                        padding: 10px;
                        border-left: 5px solid #1C56F6;
                        border-radius: 4px;
                        font-weight: normal;
                        color: #333;
                        margin-top: 10px;
                        margin-bottom: 10px;
                    ">
                        Please select crime data
                    </div>
                """, unsafe_allow_html=True)
       

    if not crime_filtered.empty:

        # Add a generate insight button
        st.caption("Community statistics")

        # Add the selected features attribute
        selected_feature = st.selectbox("Select data", selected_layers_drop)

        # Incorporate the insight button to generate statistics about your specific area
        if selected_feature and selected_feature != "No value":

            # Incorporate the insight button to generate statistics about your specific area
            if st.button("Generate Insight"):
                
                # Compute engine to be a function eventually

                # Use the query builder to create a query for street lights.
                data_grid = build_query("DATA_OPS_TESTING_DB.COLLATERAL_SCH.ES_FINAL_AGGREGATED_H3_11_GEOM_ABSOLUTEFINAL", lon, lat, buf_distance)

                # Transform the dataframe into a geodataframe.
                data_grid_gdf = get_geodata("data grid", data_grid)

                # Process the crime data 
                crime_agg = crime_filtered.groupby('H3_11').size().reset_index(name='Count')

                # Live grid creation
                live_grid = pd.merge(data_grid_gdf, crime_agg, how='left', left_on='H3_CELL_11', right_on='H3_11')

                # Create a version where only crimes are present in the grid
                live_grid_filt = live_grid[live_grid['Count'].notna()]

        # CRIME DATA PREPARATION

                 # Add appropriate dates to the crimefiltered data frame
                crime_filtered['CHARTMONTH'] = crime_filtered['RANDOM_DATE'].dt.to_period('M').astype(str)

                # Reformat the chartmonth attribute to a datetime format
                crime_filtered['CHARTMONTH'] = pd.to_datetime(crime_filtered['CHARTMONTH'], format='%Y-%m')

                # Group the crime counts by the months to get aggregate stats
                crime_dates = crime_filtered.groupby('CHARTMONTH').size().reset_index(name='Count')

                # Add a tag to differentiate the table
                crime_dates['Group'] = 'Total crime'

        # INSIGHT 1 STREET LIGHTS

                # Incorporate the insight button to generate statistics about your specific area
                if selected_feature == "Street Lights":

                # Generate statistics for the no light category

                    # Filter the no light grid data frame to only take cells where there are 0 street lights present.
                    nolight_filt = live_grid_filt[live_grid_filt['LIGHT COUNT'] == 0]

                    # Generate the total count of crimes in the no light sections for the overview stats.
                    nolight = nolight_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the no light category.
                    nolight_crimes = crime_filtered[crime_filtered['H3_11'].isin(nolight_filt['H3_CELL_11'])]

                    # Group the no light crimes to get aggregate stats
                    nolight_dates = nolight_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for just no light.
                    nolight_dates['Group'] = 'Dark Areas'

                # Generate statistics for the mid light category

                    # Filter the mid light grid data frame to only take cells where there are 1 - 2 street lights present.
                    lightmid_filt = live_grid_filt[(live_grid_filt['LIGHT COUNT'] >= 1) & (live_grid_filt['LIGHT COUNT'] <= 2)]

                    # Generate the total count of crimes in the mid light sections for the overview stats.
                    lightmid = lightmid_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the no light category.
                    midlight_crimes = crime_filtered[crime_filtered['H3_11'].isin(lightmid_filt['H3_CELL_11'])]

                    # Group the no light crimes to get aggregate stats
                    midlight_dates = midlight_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for just no light.
                    midlight_dates['Group'] = 'Slightly lit'

                # Generate statistics for the light category

                    # Filter the mid light grid data frame to only take cells where there are more than 2 street lights present.
                    light_filt = live_grid_filt[live_grid_filt['LIGHT COUNT'] > 2]

                    # Generate the total count of crimes in the light sections for the overview stats.
                    light = light_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the no light category.
                    light_crimes = crime_filtered[crime_filtered['H3_11'].isin(light_filt['H3_CELL_11'])]

                    # Group the no light crimes to get aggregate stats
                    light_dates = light_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for just no light.
                    light_dates['Group'] = 'Well Lit'

                # Combine tables

                    light_crime_stats = pd.concat([crime_dates, nolight_dates, midlight_dates, light_dates], ignore_index=True)

                    # Add a generate insight button
                    st.caption("Overall crime statistics")

                    # Generate the chart
                    chart = generate_crime_trend_chart(light_crime_stats, 'Monthly Crime Statistics')

                    # Plot the chart
                    st.altair_chart(chart, use_container_width=True)

                    # Create a new caption for aggregate insights
                    st.caption('Spatial insight at a glance')

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px; font-family:Arial, sans-serif;">
                            <h4 style="color:black; margin:0;">Crimes in dark areas</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{nolight}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px; font-family:Arial, sans-serif;">
                            <h4 style="color:black; margin:0;">Crimes in slightly lit areas</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{lightmid}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px; font-family:Arial, sans-serif;">
                            <h4 style="color:black; margin:0;">Crimes in well lit areas</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{light}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

        # INSIGHT 2 GREENSPACE

                # Incorporate the insight button to generate statistics about your specific area
                if selected_feature == "Greenspace":

                # Generate statistics for the no greenspace category

                    # Filter the no light grid data frame to only take cells where there are 0 street lights present.
                    nogreenspace_filt = live_grid_filt[live_grid_filt['GREENSPACE COUNT'] == 0]

                    # Generate the total count of crimes in the no greenspace sections for the overview stats.
                    nogreenspace = nogreenspace_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the no light category.
                    nogreenspace_crimes = crime_filtered[crime_filtered['H3_11'].isin(nogreenspace_filt['H3_CELL_11'])]

                    # Group the no light crimes to get aggregate stats
                    nogreenspace_dates = nogreenspace_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for just no light.
                    nogreenspace_dates['Group'] = 'Not near greenspace'

                # Generate statistics for the greenspace category

                    # Generate the total count of crimes in the greenspace sections for the overview stats.
                    greenspace_filt = live_grid_filt[live_grid_filt['GREENSPACE COUNT'] >= 1]

                    # Generate the total count of crimes in the no greenspace sections for the overview stats.
                    greenspace = greenspace_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the no light category.
                    greenspace_crimes = crime_filtered[crime_filtered['H3_11'].isin(greenspace_filt['H3_CELL_11'])]

                    # Group the no light crimes to get aggregate stats
                    greenspace_dates = greenspace_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for just no light.
                    greenspace_dates['Group'] = 'Near greenspace'

                # Combine tables

                    greenspace_crime_stats = pd.concat([crime_dates, nogreenspace_dates, greenspace_dates], ignore_index=True)
                
                # Add a generate insight button
                    st.caption("Overall crime statistics")

                    # Generate the chart
                    chart = generate_crime_trend_chart(greenspace_crime_stats, 'Monthly Crime Statistics')

                    # Plot the chart
                    st.altair_chart(chart, use_container_width=True)

                    # Add a caption for the new data section
                    st.caption('Spatial insight at a glance')

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px;">
                            <h4 style="color:black; margin:0;">Crimes near greenspaces</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{greenspace}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px;">
                            <h4 style="color:black; margin:0;">Crimes not near greenspaces</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{nogreenspace}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )

            # INSIGHT 3 BUILDINGS

                # Incorporate the insight button to generate statistics about your specific area
                if selected_feature == "Buildings":

                # Generate statistics for the residential category

                    # Filter the residential grid data frame to only take cells where there are  or more residential buildings.
                    residential_filt = live_grid_filt[live_grid_filt['RESIDENITAL BUILDING COUNT'] >= 1]

                    # Generate the total count of crimes in the residential sections for the overview stats.
                    residential = residential_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the residential category.
                    residential_crimes = crime_filtered[crime_filtered['H3_11'].isin(residential_filt['H3_CELL_11'])]

                    # Group the residential crimes to get aggregate stats
                    residential_dates = residential_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for residential buildings.
                    residential_dates['Group'] = 'Near residential buildings'

                # Generate statistics for the retail category

                    # Filter the retail grid data frame to only take cells where there are  or more residential buildings.
                    retail_filt = live_grid_filt[live_grid_filt['RETAIL BUILDING COUNT'] >= 1]

                    # Generate the total count of crimes in the retail sections for the overview stats.
                    retail = retail_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the retail category.
                    retail_crimes = crime_filtered[crime_filtered['H3_11'].isin(retail_filt['H3_CELL_11'])]

                    # Group the retail crimes to get aggregate stats
                    retail_dates = retail_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for retail buildings.
                    retail_dates['Group'] = 'Near retail buildings'

                # Generate statistics for the mixed use category

                    # Filter the mixed use grid data frame to only take cells where there are  or more residential buildings.
                    mixeduse_filt = live_grid_filt[live_grid_filt['MIXED_USE_COUNT'] >= 1]

                    # Generate the total count of crimes in the mixed use sections for the overview stats.
                    mixeduse = mixeduse_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the mixed use category.
                    mixeduse_crimes = crime_filtered[crime_filtered['H3_11'].isin(mixeduse_filt['H3_CELL_11'])]

                    # Group the mixed use crimes to get aggregate stats
                    mixeduse_dates = mixeduse_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for mixed use buildings.
                    mixeduse_dates['Group'] = 'Near mixed use buildings'

                # Combine tables

                    buildings_crime_stats = pd.concat([crime_dates, residential_dates, retail_dates, mixeduse_dates], ignore_index=True)
                
                # Generate line charts

                    st.caption("Overall crime statistics")

                    # Generate the chart
                    chart = generate_crime_trend_chart(buildings_crime_stats, 'Monthly Crime Statistics')

                    # Plot the chart
                    st.altair_chart(chart, use_container_width=True)

                    st.caption('Spatial insight at a glance')

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px;">
                            <h4 style="color:black; margin:0;">Crimes near residential buildings</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{residential}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px;">
                            <h4 style="color:black; margin:0;">Crimes near retail buildings</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{retail}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )
                    
                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px;">
                            <h4 style="color:black; margin:0;">Crimes near mixed use buildings</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{mixeduse}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )

            # INSIGHT 4 LANDUSE

                # Filter for the landuse selection
                if selected_feature == "Land Use":

                # Generate statistics for the residential sites category

                    # Filter the site grid data frame to only take cells where there are  or more residential sites.
                    residential_site_filt = live_grid_filt[live_grid_filt['RESIDENTIAL SITE COUNT'] >= 1]

                    # Generate the total count of crimes in the residential site sections for the overview stats.
                    residential_site = residential_site_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the mixed use category.
                    residential_site_crimes = crime_filtered[crime_filtered['H3_11'].isin(residential_site_filt['H3_CELL_11'])]

                    # Group the residential site crimes to get aggregate stats
                    residential_site_dates = residential_site_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for mixed use buildings.
                    residential_site_dates['Group'] = 'Near residential sites'
                
                # Generate statistics for the retail sites category

                    # Filter the site grid data frame to only take cells where there are one or more retail sites.
                    retail_site_filt = live_grid_filt[live_grid_filt['RETAIL SITE COUNT'] >= 1]

                    # Generate the total count of crimes in the retail site sections for the overview stats.
                    retail_site = retail_site_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the retail category.
                    retail_site_crimes = crime_filtered[crime_filtered['H3_11'].isin(retail_site_filt['H3_CELL_11'])]

                    # Group the retail site crimes to get aggregate stats
                    retail_site_dates = retail_site_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for retail sites.
                    retail_site_dates['Group'] = 'Near retail sites'
                
                # Generate statistics for the industrial sites category

                    # Filter the site grid data frame to only take cells where there are one or more industrial sites.
                    industrial_site_filt = live_grid_filt[live_grid_filt['INUSTRIAL SITE COUNT'] >= 1]

                    # Generate the total count of crimes in the industrial site sections for the overview stats.
                    industrial_site = industrial_site_filt['Count'].sum()

                    # Filter the crimes from crime filtered to only get the crimes in the industrial category.
                    industrial_site_crimes = crime_filtered[crime_filtered['H3_11'].isin(industrial_site_filt['H3_CELL_11'])]

                    # Group the industrial site crimes to get aggregate stats
                    industrial_site_dates = industrial_site_crimes.groupby('CHARTMONTH').size().reset_index(name='Count')

                    # Ascribe a value for industrial sites.
                    industrial_site_dates['Group'] = 'Near industrial sites'

                # Combine tables

                    sites_crime_stats = pd.concat([crime_dates, residential_site_dates, retail_site_dates, industrial_site_dates], ignore_index=True)
                
                # Generate line charts

                    st.caption("Overall crime statistics")

                    # Generate the chart
                    chart = generate_crime_trend_chart(sites_crime_stats, 'Monthly Crime Statistics')

                    # Plot the chart
                    st.altair_chart(chart, use_container_width=True)

                    st.caption('Spatial insight at a glance')

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px;">
                            <h4 style="color:black; margin:0;">Crimes near residential sites</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{residential_site}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )

                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px;">
                            <h4 style="color:black; margin:0;">Crimes near retail sites</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{retail_site}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )
                    
                    # Markdown visualisation
                    st.markdown(
                        f"""
                        <div style="background-color:#FFFFFF; padding:10px; border-radius:10px; text-align:center; margin-bottom:20px;">
                            <h4 style="color:black; margin:0;">Crimes near industrial sites</h4>
                            <p style="color:black; font-size:24px; font-weight:bold; margin:0;">{industrial_site}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                        )

            # INSIGHT 4 CRIME

                    # Incorporate the insight button to generate statistics about your specific area
                if selected_feature == "Crime":

                    # Add a generate insight button
                    st.caption("Overall crime statistics")

                    # Generate the chart
                    chart = generate_crime_trend_chart(crime_dates, 'Monthly Crime Statistics')

                    # Plot the chart
                    st.altair_chart(chart, use_container_width=True)


############################################################################################

# Basemap Roads
basemaps_roads = f.FeatureGroup(name='OS Road Map')
# Basemap Light
basemaps_light = f.FeatureGroup(name='OS Light Map')

# Crime cluster functionality
crime_cluster = MarkerCluster(
    icon_create_function="""
    function(cluster) {
        return L.divIcon({
            html: '<div style="background-color:#1C56F6; color:white; border-radius:50%; width:40px; height:40px; display:flex; align-items:center; justify-content:center;"><span>' + cluster.getChildCount() + '</span></div>',
            className: 'custom-cluster',
            iconSize: [40, 40]
        });
    }
    """
)
# Street lights
streetlights_layer = f.FeatureGroup(name='Streetlights', overlay=True, control=True)
# Buildings
buildings_layer = f.FeatureGroup(name='Buildings', overlay=True, control=True)
# Landuse
greenspace_layer = f.FeatureGroup(name='Greenspace', overlay=True, control=True)
# Landuse
landuse_layer = f.FeatureGroup(name='Landuse', overlay=True, control=True)
# Buffer
buffer_layer = f.FeatureGroup(name='Buffer', overlay=True, control=True)
# Crime
crime_layer = f.FeatureGroup(name='Crime')

# Define the tile layer URL with the API key and SRS
tile_light = "https://api.os.uk/maps/raster/v1/zxy/Light_3857/{z}/{x}/{y}.png?key=R9GFKkaqfgSi8NY63QOvgAoMuPTrSjkp"

# Define the tile layer URL with the API key and SRS
tile_roads = "https://api.os.uk/maps/raster/v1/zxy/Road_3857/{z}/{x}/{y}.png?key=R9GFKkaqfgSi8NY63QOvgAoMuPTrSjkp"

# Add a custom tile layer
f.TileLayer(
    tiles=tile_roads,
    attr='&copy; <a href="http://www.ordnancesurvey.co.uk/">Ordnance Survey</a>',
    name='OS Maps Road',
    overlay=True,
    control=True
).add_to(basemaps_roads)

# Add a custom tile layer
f.TileLayer(
    tiles=tile_light,
    attr='&copy; <a href="http://www.ordnancesurvey.co.uk/">Ordnance Survey</a>',
    name='OS Maps Light',
    overlay=True,
    control=True
).add_to(basemaps_light)

# Create the map centered on the same coordinates
if query:
    if addresses:
        # Set the map to the location of the address 
        m = f.Map(location=[lat, lon], zoom_start=16, max_zoom=19, tiles=None)

        # Add features to the map
        basemaps_roads.add_to(m)
        basemaps_light.add_to(m)
        
        # Add a marker
        f.Marker([lat, lon], popup=address, icon=f.Icon(color='darkblue')).add_to(m)

        # Ensure that information is only pulled through if the buffer us greater the zero. Also ensure the layer order is preserved

        if buf_distance > 0:

            m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

            if selected_layers.get("Land Use"):

                # Add, style and process landuse layers to the web map.
                for _, row in landuse_gdf.iterrows():
                    f.GeoJson(
                        mapping(row.geometry), 
                        style_function=lambda x: {
                            'color': '#f3f2f2',
                            'fillColor': '#FADADD',
                            'weight': 1,
                            'fillOpacity': 1.0
                        },
                        tooltip=row.get('DESCRIPTION', 'Description')
                    ).add_to(landuse_layer)

                landuse_layer.add_to(m)
            
            if selected_layers.get("Greenspace"):

                # Add, style and process greenspace layers to the web map.
                for _, row in greenspace_gdf.iterrows():
                    f.GeoJson(
                        mapping(row.geometry), 
                        style_function=lambda x: {
                            'color': '#f3f2f2',
                            'fillColor': '#cee967',
                            'weight': 1,
                            'fillOpacity': 1.0
                        },
                        tooltip=row.get('FUNCTION', 'Description') 
                    ).add_to(greenspace_layer)

                greenspace_layer.add_to(m)
            
            if selected_layers.get("Buildings"):

                # Add, style and process building layers to the web map.
                for _, row in Test_gdf.iterrows():
                    f.GeoJson(
                        mapping(row.geometry), 
                        style_function=lambda x: {
                            'color': '#f3f2f2',
                            'fillColor': '#a39f9c',
                            'weight': 1,
                            'fillOpacity': 1
                        },
                        tooltip= (row.get('DESCRIPTION', 'Description'))
                    ).add_to(buildings_layer)

                buildings_layer.add_to(m)

            if selected_layers.get("Street Lights"):

                for _, row in stlgt_gdf.iterrows():
                    f.CircleMarker(
                        location=[row.geometry.y, row.geometry.x],
                        radius=4, 
                        color='#000000',  
                        fill=True,
                        fill_color='#ffbe0a',
                        fill_opacity=0.6,
                        weight=1,
                        tooltip=row.get('DESCRIPTION', 'Street Light')
                    ).add_to(streetlights_layer)

                streetlights_layer.add_to(m)

            if selected_layers.get("Crime"):

                # Choose which crime data to use
                crime_final = crime_filtered if len(crime_filtered) < len(crime_gdf) else crime_gdf

                for _, row in crime_final.iterrows():
                    f.CircleMarker(
                        location=[row.geometry.y, row.geometry.x],
                        radius=6,
                        color='#1C56F6',
                        fill=True,
                        fill_color="#1C56F6",
                        fill_opacity=1.0,
                        weight=0.5,
                        tooltip=f"{row.get('CRIME_TYPE', 'Crime type')} — {row.get('RANDOM_DATE', 'Date')}"
                    ).add_to(crime_cluster)

                # Add the cluster to the named layer
                crime_cluster.add_to(crime_layer)

                # Add crime cluster to the map
                crime_layer.add_to(m)

            # Add the single feature from bff_gdf
            f.Circle(
                location=[lat, lon],
                radius=buf_distance,
                color='#1C56F6',
                fill=False,
                fill_opacity=0.2,
                popup=f"Buffer: {buf_distance}m"
            ).add_to(buffer_layer)

            buffer_layer.add_to(m)

    else:
        # Set the map to the default location
        m = f.Map(location=[52.4814, -1.8998], zoom_start=14, max_zoom=19, tiles=None)

        # Add features to the map
        basemaps_roads.add_to(m)
        basemaps_light.add_to(m)
else:
    # Set the map to the default location
    m = f.Map(location=[52.4814, -1.8998], zoom_start=14, max_zoom=19, tiles=None)

    # Add features to the map
    basemaps_roads.add_to(m)
    basemaps_light.add_to(m)

# Define legend entries with colors
legend_items = {
    "Land Use": "#FADADD",
    "Greenspace": "#cee967",
    "Buildings": "#a39f9c",
    "Street Lights": "#ffbe0a",
    "Crime": "#1C56F6",
    "Buffer": "#1C56F6"
}

# Build legend HTML dynamically
legend_html = """
{% macro html(this, kwargs) %}
<div style="
    position: fixed;
    bottom: 50px;
    left: 50px;
    width: 220px;
    height: auto;
    z-index:9999;
    font-size:14px;
    background-color: white;
    border:2px solid grey;
    padding: 10px;
    opacity: 0.85;
">
<b>Features</b><br>
"""

# Add only selected items
for layer_name, color in legend_items.items():
    if selected_layers.get(layer_name) or (layer_name == "Buffer" and buf_distance > 0):
        if layer_name == "Buffer":
            # Buffer will get a line to distinguish it from the crime data
            legend_html += f'<span style="display:inline-block; width:30px; height:2px; background-color:{color}; vertical-align:middle;"></span> {layer_name}<br>\n'
        else:
            # Use square for polygons, circle for points
            symbol = "&#9632;" if layer_name in ["Land Use", "Greenspace", "Buildings"] else "&#9679;"
            legend_html += f'<span style="color:{color};">{symbol}</span> {layer_name}<br>\n'

legend_html += "</div>\n{% endmacro %}"

# Inject into map
legend = branca.element.MacroElement()
legend._template = branca.element.Template(legend_html)
m.get_root().add_child(legend)


# Add layer control
f.LayerControl().add_to(m)

############################################################################################

with cols[0]:
    with mapcols[0]:
        st.image("images/OS_logo.svg", width=150)

    with mapcols[1]:
        st.markdown(
            """
            <h1 style='margin-top: -20px; margin-bottom: 0; font-size: 32px; font-family: Arial;'>
                Crime Reporting & Intelligence Spatial Platform
            </h1>
            """,
            unsafe_allow_html=True
        )

with cols[0]:
    # Display the map in the first call
    st_folium(m, width=1500, height=1150, returned_objects=["all"])
