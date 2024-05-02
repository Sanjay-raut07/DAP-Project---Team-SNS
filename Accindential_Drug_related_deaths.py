#!/usr/bin/env python
# coding: utf-8

# ### Importing all the necessary libraries

# In[1]:


import requests
import pandas as pd
import numpy as np
from io import StringIO
import os
import pymongo
from pymongo import MongoClient
from pandas import json_normalize
import json
import seaborn as sns
#to ignore warnings
import warnings
warnings.filterwarnings('ignore')


# In[2]:


# Specify the folder path where you want to save the Data
file_name = "Accidental_Drug_Related_Deaths_2012-2022.csv"


# ### Making the Directory Global

# In[3]:


#To ensure your code is portable and the specified folder is accessible on any system,we are using
#environment variables and the Python os module to dynamically create paths based on the 
#user's system environment and creating the folder if not exist
# Getting the desktop directory
desktop_path = os.path.join(os.path.expanduser("~"), 'Desktop')
new_folder_path = os.path.join(desktop_path, 'DAP Project_teamSNS')
if not os.path.exists(new_folder_path):
    os.makedirs(new_folder_path)
file_path = os.path.join(new_folder_path, file_name)
print(f"Directory created at: {new_folder_path}")
print(f"File path: {file_path}")


# ### Extracting dataset through API

# In[4]:


#Firstly we are Going to Extract our dataset through API
# URL to fetch CSV data
url = 'https://data.ct.gov/api/views/rybz-nyjw/rows.csv?accessType=DOWNLOAD'
# Make the GET request
response = requests.get(url)
print(response)
if response.status_code == 200:
    csv_data = StringIO(response.text)
    # Load data into a pandas DataFrame from the file-like object
    df = pd.read_csv(csv_data)
    # Save the DataFrame to CSV in the specified folder
    df.to_csv(file_path, index=False)
    print(f"Data saved to CSV file at: {file_path}")
else:
    print("Failed to retrieve data. Status code:", response.status_code)
print("Accidental_Drug_Related_Deaths_2012-2022 CSV DONE")


# ### Converting CSV into JSON

# In[5]:


# Dataset names for CSV and JSON files
datasets = ["Accidental_Drug_Related_Deaths_2012-2022"]
def convert_and_cleanup(data_name):
    csv_file_path = f"{new_folder_path}\\{data_name}.csv"
    json_file_path = f"{new_folder_path}\\{data_name}.json"
    try:
        # Load CSV into DataFrame
        df = pd.read_csv(csv_file_path)
        # Convert DataFrame to JSON
        df.to_json(json_file_path, orient='records', lines=True)
        # Remove the original CSV file
        os.remove(csv_file_path)
        print(f"Converted {csv_file_path} to JSON and deleted the CSV file.")
    except Exception as e:
        print(f"An error occurred processing {data_name}: {e}")
for dataset in datasets:
    convert_and_cleanup(dataset)


# ### Dumping the JSON File in Mongodb database

# In[6]:


# MongoDB connection
myclient = pymongo.MongoClient("mongodb://localhost:27017")
db = myclient['DAP']  # DAP is the name of the database
# File paths
Accidental_Data = f"{new_folder_path}\\Accidental_Drug_Related_Deaths_2012-2022.json"
# Inserting JSON data into MongoDB
try:
    collection2 = db.Accidental_Data
# Delete existing data in the collection
    collection2.delete_many({})  # This deletes all documents in the collection
    print("Existing data deleted from the collection2.")
    with open(Accidental_Data, 'r', encoding='utf-8') as file:
        for line in file:
            obj = json.loads(line)
            collection2.insert_one(obj)
    print("Accidental_Data data successfully inserted into MongoDB.")
except Exception as e:
    print("An error occurred while inserting JSON data:", e)


# ### Extracting data from mongodb into Dataframe

# In[7]:


client = MongoClient()
#point the client at mongo URI
client = MongoClient('mongodb://localhost:27017')
#select database
db = client['DAP']
#select collection
test = db.Accidental_Data
#convert entire collection to pandas Dataframe
test =list(test.find())
df = json_normalize(test)
df.head() # displaying the first 5 data


# In[8]:


#displaying the last 5 data
df.tail()


# In[9]:


#Showing data infomation
df.info()


# In[10]:


#Checking for Dupliactes
df.nunique()


# In[11]:


#Missing Values Calculation
df.isnull().sum()


# In[12]:


#idengifying null value by visualization
import seaborn as sns
import matplotlib.pyplot as plt
sns.displot(data=df.isnull().melt(value_name='missing'),y='variable',hue='missing',multiple='fill',height=8,aspect=1.4)
plt.axvline(0.5, color='r')


# In[13]:


#Calculating the percentage of missing values
(df.isnull().sum()/(len(df)))*100


# ### As we can see there are many null values are present in the dataset
# 1) Columns with No Missing Values (0% Null):-
# id, Date, Date Type, Cause of Death, DeathCityGeo
# 2) Low Percentage of Missing Values (1% - 10% Null):-
# Age, Sex, Residence City, Residence County, Injury City, Injury Place, Description of Injury, Location, Manner of Death, ResidenceCityGeo, InjuryCityGeo
# 3) Moderate Percentage of Missing Values (10% - 30% Null):- 
# Residence State, Injury County, Injury State, Death City, Death County, Fentanyl, Any Opioid
# 4) High Percentage of Missing Values (30% - 50% Null):- 
# Death State
# 5) Very High Percentage of Missing Values (Over 50% Null):-
# Ethnicity, Location if Other, Other Significant Conditions, Heroin, Heroin death certificate (DC), Cocaine, Fentanyl Analogue, Oxycodone, Oxymorphone, Ethanol, Hydrocodone, Benzodiazepine, Methadone, Meth/Amphetamine, Amphet, Tramad, Hydromorphone, Morphine (Not Heroin), Xylazine, Gabapentin, Opiate NOS, Heroin/Morph/Codeine, Other Opioid, Other

# In[14]:


#Age
#filling null age values with mean age and coverting it to integer type
age_mean = df['Age'].mean()
df['Age'] = df['Age'].fillna(age_mean)
df['Age'] = df['Age'].astype(int)
df['Age'].isnull().sum()


# In[15]:


df['Race'].unique()


# In[17]:


df['Age'].unique()


# In[18]:


df['Ethnicity'].unique()


# In[19]:


df['Injury State'].unique()
df['Death State'].unique()
df['Residence State'].unique()


# In[20]:


df['Race'].unique()


# In[21]:


#Aggregating similar attributes
df['Race'] = df['Race'].replace('Black or African American','Black')
df['Race'] = df['Race'].replace('Asian Indian','Asian/Indian')


# In[22]:


#Replace Connecticut with CT
# Replace 'CONNECTICUT' with 'CT' in the 'state' column
df['Injury State'] = df['Injury State'].replace('CONNECTICUT', 'CT')


# In[23]:


#print(df['Date Type'].unique())
#Things to remove
"""Residence city
   Residence county
   Injury city
   Injury county
   Injury Place
   Death City
   Death county
   Heroin death certificate (DC)
   Location if Other
   Other Significant Conditions
   ResidenceCityGeo 
   InjuryCityGeo
   DeathCityGeo
"""


# In[24]:


df.fillna('None', inplace=True)


# In[25]:


df.info()


# In[26]:


new_df=df.drop(['Residence City','Residence County',
         'Injury City','Injury County','Injury Place',
         'Death City','Death County','Heroin death certificate (DC)',
         'Location if Other',
         'ResidenceCityGeo','InjuryCityGeo','DeathCityGeo'],axis=1)


# In[27]:


#Encoding all 'Y' to 1 and all none values to 0
# List of drug-related columns
drug_columns = [
    'Heroin', 'Cocaine', 'Fentanyl', 'Fentanyl Analogue', 'Oxycodone',
    'Oxymorphone', 'Ethanol', 'Hydrocodone', 'Benzodiazepine', 'Methadone',
    'Meth/Amphetamine', 'Amphet', 'Tramad', 'Hydromorphone',
    'Morphine (Not Heroin)', 'Xylazine', 'Gabapentin', 'Opiate NOS',
    'Heroin/Morph/Codeine', 'Other Opioid','Any Opioid']


# In[28]:


# Apply the encoding
for column in drug_columns:
    new_df[column] = new_df[column].map({'Y': 1}).fillna(0).astype(int)

# Show some of the modified data to confirm changes
new_df[drug_columns].head()


# In[30]:


import plotly.express as px
#need to group by Date and sum/average detections
time_data = new_df.groupby('Date')[drug_columns].sum().reset_index()
# Create a line graph
fig = px.line(time_data, x='Date', y=drug_columns, title='Trend of Drug Detections Over Time')
fig.show()


# In[31]:


new_df.head()


# In[32]:


new_df.fillna('None', inplace=True)


# In[33]:


new_df.info()


# In[34]:


#might have to do binning for age 
new_df.head()


# In[35]:


new_df['Sex'].hist()


# In[36]:


import seaborn as sns
import matplotlib.pyplot as plt
plt.figure(figsize=(6, 4))
sns.boxplot(x=df['Age'])
plt.title('Age Box Plot')
plt.xlabel('Age')
plt.show()


# In[37]:


plt.figure(figsize=(8, 4))
sns.kdeplot(df['Age'], shade=True, color="r")
plt.title('Age Density Plot')
plt.xlabel('Age')
plt.show()


# In[38]:


plt.figure(figsize=(8, 6))
plt.scatter(x=new_df['Age'], y=new_df['Race'], alpha=0.7, color='green')
plt.title('Age vs Race Scatter Plot')
plt.xlabel('Age')
plt.ylabel('Race')
plt.show()


# In[39]:


state_counts = new_df['Death State'].value_counts()
fig, ax = plt.subplots()
ax.pie(state_counts, labels=state_counts.index, autopct='%1.1f%%', startangle=90)
ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.title("Distribution of Records by State")
plt.show()


# In[40]:


df2=new_df
df2.head()


# In[42]:


from sklearn.cluster import KMeans
# Handle missing values if any (assuming none for this example)
# df2['Age'].dropna(inplace=True)  # Uncomment this line if there are missing values
# Elbow Method to determine the optimal number of clusters
inertia = []
k_range = range(1, 10)
for k in k_range:
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(df2[['Age']])
    inertia.append(kmeans.inertia_)


# In[43]:


# Plotting the Elbow graph
plt.figure(figsize=(8, 4))
plt.plot(k_range, inertia, marker='o')
plt.xlabel('Number of clusters')
plt.ylabel('Inertia')
plt.title('Elbow Method For Optimal k')
plt.show()


# In[44]:


# Applying K-Means with an assumed optimal number of clusters
# Let's assume 3 clusters from the elbow method observation
kmeans = KMeans(n_clusters=3, random_state=42)
df2['Cluster'] = kmeans.fit_predict(df2[['Age']])


# In[45]:


# Analyze the clusters
print(df2.groupby('Cluster')['Age'].agg(['min', 'max', 'mean', 'count']))
# Visualizing the clusters
plt.figure(figsize=(8, 6))
for cluster in df2['Cluster'].unique():
    cluster_group = df2[df2['Cluster'] == cluster]
    plt.scatter(cluster_group['Age'], np.zeros_like(cluster_group['Age']), label=f'Cluster {cluster}')
plt.xlabel('Age')
plt.yticks([])
plt.title('Distribution of Ages by Cluster')
plt.legend()
plt.show()


# In[46]:


from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
# Define categorical and numeric columns
categorical_cols = ['Sex']
numeric_cols = ['Age']
# Preprocessing pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numeric_cols),
        ('cat', OneHotEncoder(), categorical_cols)
    ])


# In[47]:


# Fit and transform the data
pipeline = Pipeline(steps=[('preprocessor', preprocessor)])
X_processed = pipeline.fit_transform(df2)


# In[48]:


# Clustering
kmeans = KMeans(n_clusters=2, random_state=42)
df2['Cluster'] = kmeans.fit_predict(X_processed)


# In[49]:


# Plotting the results
# Since we only have one numeric attribute ('Age'), let's visualize it against the index
plt.figure(figsize=(10, 6))
sns.scatterplot(x=df2.index, y='Age', hue='Cluster', data=df2, palette='viridis', s=100)
plt.title('Cluster Distribution by Age and Index')
plt.xlabel('Index')
plt.ylabel('Age')
plt.legend(title='Cluster')
plt.grid(True)
plt.show()
# If more numeric attributes were available, consider pair plots or 3D plots


# ### Data to be Dumped in postgresql

# In[50]:


new_df.dtypes


# In[51]:


#First we have to convert all the dtype into str as postgresql does not accept the float dtype
new_df['_id'] = new_df['_id'].astype(str)
new_df['Date'] = new_df['Date'].astype(str)
new_df['Date Type'] = new_df['Date Type'].astype(str)
new_df['Sex'] = new_df['Sex'].astype(str)
new_df['Race'] = new_df['Race'].astype(str)
new_df['Residence State'] = new_df['Residence State'].astype(str)
new_df['Ethnicity'] = new_df['Ethnicity'].astype(str)
new_df['Injury State'] = new_df['Injury State'].astype(str)
new_df['Description of Injury'] = new_df['Description of Injury'].astype(str)
new_df['Death State'] = new_df['Death State'].astype(str)
new_df['Location'] = new_df['Location'].astype(str)
new_df['Cause of Death'] = new_df['Cause of Death'].astype(str)
new_df['Manner of Death'] = new_df['Manner of Death'].astype(str)
new_df['Other'] = new_df['Other'].astype(str)


# In[52]:


import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine


# In[53]:


# Connection parameters for PostgreSQL
database = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "Mypassword07"
}


# In[1]:


# Establish a connection to the PostgreSQL server
try:
    conn = psycopg2.connect(**database)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    # Creating a cursor to perform database operations
    cur = conn.cursor()
    # Fisrt it will check if the 'DAP Project' database already exists,
    #if not it will create the database
    cur.execute("SELECT 1 FROM pg_database WHERE datname='DAP Project';")
    exists = cur.fetchone()
    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier('DAP Project')))
        print("Database 'DAP Project' was successfully created.")
    else:
        print("Database 'DAP Project' already exists. No action was taken.")
except psycopg2.Error as e:
    print("An error occurred: ", e)
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
print("Finished checking and potentially creating the 'DAP Poject database.")


# In[55]:


# Create the database engine
# PostgreSQL connection parameters
username = 'postgres'
password = 'Mypassword07'
host = 'localhost'
port = '5432'
database = 'DAP Project'
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')


# In[56]:


#Finally our all connection setup is done and now by using the 'to_sql' will dump all the
#data in database
new_df.to_sql('Accidential Data', con=engine, index=False, if_exists='replace')


# In[ ]:




