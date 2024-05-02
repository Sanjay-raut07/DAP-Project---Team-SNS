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
from dagster import asset, job
import warnings


@asset
def Extract_Dataset_API():
    # Specify the folder path where you want to save the Data
    file_name1 = "Accidental_Drug_Related_Deaths_2012-2022.csv"
    file_name2 = "NCHS_-_Drug_Poisoning_Mortality_by_State__United_States.csv"
    file_name3 = "Opiod_EMS_calls.csv"

    # Making the Directory Global
    #To ensure your code is portable and the specified folder is accessible on any system,we are using
    #environment variables and the Python os module to dynamically create paths based on the 
    #user's system environment and creating the folder if not exist
    # Getting the desktop directory
    desktop_path = os.path.join(os.path.expanduser("~"), 'Desktop')
    new_folder_path = os.path.join(desktop_path, 'DAP Project_teamSNS')
    if not os.path.exists(new_folder_path):
        os.makedirs(new_folder_path)
    file_path1 = os.path.join(new_folder_path, file_name1)
    file_path2 = os.path.join(new_folder_path, file_name2)
    file_path3 = os.path.join(new_folder_path, file_name3)
    print(f"Directory created at: {new_folder_path}")
    print(f"File path: {file_path1}{file_path2}{file_path3}")

    #Accidentail
    # Extracting dataset through API
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
        df.to_csv(file_path1, index=False)
        print(f"Data saved to CSV file at: {file_path1}")
    else:
        print("Failed to retrieve data. Status code:", response.status_code)
    print("Accidental_Drug_Related_Deaths_2012-2022 CSV DONE")


    #NCHS
    #Firstly we are Going to Extract our dataset through API
    # URL to fetch CSV data
    url = 'https://data.cdc.gov/api/views/44rk-q6r2/rows.csv?accessType=DOWNLOAD'
    # Make the GET request
    response = requests.get(url)
    print(response)
    if response.status_code == 200:
        csv_data = StringIO(response.text)
        # Load data into a pandas DataFrame from the file-like object
        df = pd.read_csv(csv_data)
        # Save the DataFrame to CSV in the specified folder
        df.to_csv(file_path2, index=False)
        print(f"Data saved to CSV file at: {file_path2}")
    else:
        print("Failed to retrieve data. Status code:", response.status_code)
    print("NCHS_-_Drug_Poisoning_Mortality_by_State__United_States CSV DONE")

    #Opioid EMS Calls
    #Firstly we are Going to Extract our dataset through API
    # URL to fetch CSV data
    url = 'https://data.tempe.gov/api/download/v1/items/2daeeafd2741494c8294ca415e5a793e/csv?layers=0'
    # Make the GET request
    response = requests.get(url)
    print(response)
    if response.status_code == 200:
        csv_data = StringIO(response.text)
        # Load data into a pandas DataFrame from the file-like object
        df = pd.read_csv(csv_data)
        # Save the DataFrame to CSV in the specified folder
        df.to_csv(file_path3, index=False)
        print(f"Data saved to CSV file at: {file_path3}")

    else:
        print("Failed to retrieve data. Status code:", response.status_code)
    print("Opoid EMS calls CSV DONE")


    # Converting CSV into JSON
    # Dataset names for CSV and JSON files
    datasets = [
    "Accidental_Drug_Related_Deaths_2012-2022",
    "NCHS_-_Drug_Poisoning_Mortality_by_State__United_States"
    ]
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


    # Dumping the JSON File in Mongodb database
    # MongoDB connection
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    db = myclient['DAP']  # DAP is the name of the database
    # File paths
    Drug_Poisoning_Data = f"{new_folder_path}\\NCHS_-_Drug_Poisoning_Mortality_by_State__United_States.json"
    Accidental_Data = f"{new_folder_path}\\Accidental_Drug_Related_Deaths_2012-2022.json"
    csv_file_path = f"{new_folder_path}\\Opiod_EMS_calls.csv"
    # Inserting JSON data into MongoDB
    try:
        collection1 = db.Drug_Poisoning_Data
        
        # Delete existing data in the collection
        collection1.delete_many({})  # This deletes all documents in the collection
        print("Existing data deleted from the collection1.")

        with open(Drug_Poisoning_Data, 'r', encoding='utf-8') as file:
            for line in file:
                obj = json.loads(line)  # Parse the JSON data from a line
                collection1.insert_one(obj)
        print("Drug_Poisoning_Data data successfully inserted into MongoDB.")
        collection2 = db.Accidental_Data
        
        # Delete existing data in the collection
        collection2.delete_many({})  # This deletes all documents in the collection
        print("Existing data deleted from the collection2.")
        
        with open(Accidental_Data, 'r', encoding='utf-8') as file:
            for line in file:
                obj = json.loads(line)  # Parse the JSON data from a line
                collection2.insert_one(obj)
        print("Accidental_Data data successfully inserted into MongoDB.")
    except Exception as e:
        print("An error occurred while inserting JSON data:", e)
    # Inserting CSV data into MongoDB
    try:
        collection3 = db.Opiod_calls_data
        
    # Delete existing data in the collection
        collection3.delete_many({})  # This deletes all documents in the collection
        print("Existing data deleted from the collection3.")
    
        csv_data = pd.read_csv(csv_file_path)
        csv_data_dict = csv_data.to_dict('records')
        collection3.insert_many(csv_data_dict)
        print("Opiod_calls_data successfully inserted into MongoDB.")
    except Exception as e:
        print("An error occurred while inserting CSV data:", e)

@asset
def EDA_Accidental_Data():
    #Extracting data from mongodb into Dataframe of Accidental_Data
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
    print(df.head()) # displaying the first 5 data

    #Showing data infomation
    print(df.info())

    #Age
    #filling null age values with mean age and coverting it to integer type
    age_mean = df['Age'].mean()
    df['Age'] = df['Age'].fillna(age_mean)
    df['Age'] = df['Age'].astype(int)
    df['Age'].isnull().sum()

    #Aggregating similar attributes
    df['Race'] = df['Race'].replace('Black or African American','Black')
    df['Race'] = df['Race'].replace('Asian Indian','Asian/Indian')

    #Replace Connecticut with CT
    # Replace 'CONNECTICUT' with 'CT' in the 'state' column
    df['Injury State'] = df['Injury State'].replace('CONNECTICUT', 'CT')
    
    df.fillna('None', inplace=True)

    new_df=df.drop(['Residence City','Residence County',
         'Injury City','Injury County','Injury Place',
         'Death City','Death County','Heroin death certificate (DC)',
         'Location if Other',
         'ResidenceCityGeo','InjuryCityGeo','DeathCityGeo'],axis=1)
    
    #Encoding all 'Y' to 1 and all none values to 0
    # List of drug-related columns
    drug_columns = [
        'Heroin', 'Cocaine', 'Fentanyl', 'Fentanyl Analogue', 'Oxycodone',
        'Oxymorphone', 'Ethanol', 'Hydrocodone', 'Benzodiazepine', 'Methadone',
        'Meth/Amphetamine', 'Amphet', 'Tramad', 'Hydromorphone',
        'Morphine (Not Heroin)', 'Xylazine', 'Gabapentin', 'Opiate NOS',
        'Heroin/Morph/Codeine', 'Other Opioid','Any Opioid']
    
    # Apply the encoding
    for column in drug_columns:
        new_df[column] = new_df[column].map({'Y': 1}).fillna(0).astype(int)

    # Show some of the modified data to confirm changes
    new_df[drug_columns].head()

    print(new_df.head())

    return new_df

@asset
def NCHS_Dataset_EDA():
    client = MongoClient()
    #point the client at mongo URI
    client = MongoClient('mongodb://localhost:27017')
    #select database
    db = client['DAP']
    #select collection
    test = db.Drug_Poisoning_Data
    #convert entire collection to pandas Dataframe
    test =list(test.find())
    df = json_normalize(test)
    df.head() # displaying the first 5 data


    new_df=df.drop(['_id','Standard Error for Crude Rate','Low Confidence Limit for Crude Rate','Upper Confidence Limit for Crude Rate'
                ,'Standard Error Age-adjusted Rate','Standard Error Age-adjusted Rate'
                , 'Lower Confidence Limit for Age-adjusted Rate','Upper Confidence for Age-adjusted Rate'
                ,'State Crude Rate in Range','Unit'],axis=1)

    vdf= new_df
    #Showing graph of death over a year
    data = new_df
    # Filter the data for the United States only and for age-adjusted rates
    us_data = data[(data['State'] == 'United States') & (data['Age-adjusted Rate'].notnull())]
    pivot_data = us_data.pivot_table(
        index='Year', 
        columns='Sex', 
        values='Age-adjusted Rate',
        aggfunc='mean'
    ).loc[1999:2019]
    pivot_data.reset_index(inplace=True)
    pivot_data.head()

    return new_df

@asset
def Opiod_EMS_Dataset_EDA():
    client = MongoClient()
    #point the client at mongo URI
    client = MongoClient('mongodb://localhost:27017')
    #select database
    db = client['DAP']
    #select collection
    df = db.Opiod_calls_data
    #convert entire collection to pandas Dataframe
    df =pd.DataFrame(list(df.find()))
    df.head()

    df = df.drop(columns=['ï»¿X','Y','Latitude_Random','Longitude_Random'])
    df.head()

    #replacing null values with the most frequent value of that class 
    most_frequent = df['Month'].mode()[0]
    df['Month'].fillna(most_frequent, inplace=True)
    #most_frequent = df['Narcan_Given'].mode()[0]
    df['Narcan_Given'].fillna('Unknown', inplace=True)
    most_frequent = df['Patient_Gender'].mode()[0]
    df['Patient_Gender'].fillna(most_frequent, inplace=True)
    most_frequent = df['Patient_ASU'].mode()[0]
    df['Patient_ASU'].fillna(most_frequent, inplace=True)
    most_frequent = df['Patient_Veteran'].mode()[0]
    df['Patient_Veteran'].fillna(most_frequent, inplace=True)
    most_frequent = df['Patient_Homeless'].mode()[0]
    df['Patient_Homeless'].fillna(most_frequent, inplace=True)
    most_frequent = df['Notes'].mode()[0]
    df['Notes'].fillna(most_frequent, inplace=True)

    from sklearn.preprocessing import LabelEncoder
    label_encoder = LabelEncoder()
    # Apply LabelEncoder to each categorical column
    df['Opioid_Use'] = label_encoder.fit_transform(df['Opioid_Use'])
    df['Narcan_Given'] = label_encoder.fit_transform(df['Narcan_Given'])
    df['Patient_Gender'] = label_encoder.fit_transform(df['Patient_Gender'])
    df['Patient_ASU'] = label_encoder.fit_transform(df['Patient_ASU'])
    df['Patient_Veteran'] = label_encoder.fit_transform(df['Patient_Veteran'])
    df['Patient_Homeless'] = label_encoder.fit_transform(df['Patient_Homeless'])
    df['Time_of_Day'] = label_encoder.fit_transform(df['Time_of_Day'])
    df.head()

    return df
 
    
@asset
def Accidential_data_to_be_Dumped_in_postgress(EDA_Accidental_Data):
    new_df = EDA_Accidental_Data
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

    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from sqlalchemy import create_engine

    # Connection parameters for PostgreSQL
    database = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "Mypassword07"
    }

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

    # Create the database engine
    # PostgreSQL connection parameters
    username = 'postgres'
    password = 'Mypassword07'
    host = 'localhost'
    port = '5432'
    database = 'DAP Project'
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

    #Finally our all connection setup is done and now by using the 'to_sql' will dump all the
    #data in database
    new_df.to_sql('Accidential Data', con=engine, index=False, if_exists='replace')

@asset
def NCHS_data_to_be_Dumped_in_postgress(NCHS_Dataset_EDA):
    new_df = NCHS_Dataset_EDA
    #NCHS
    #First we have to convert all the dtype into str as postgresql does not accept the object dtype
    new_df['Year'] = new_df['Year'].astype(str)
    new_df['Deaths'] = new_df['Deaths'].astype(str)
    new_df['Population'] = new_df['Population'].astype(str)
    new_df['Crude Death Rate'] = new_df['Crude Death Rate'].astype(str)
    new_df['US Crude Rate'] = new_df['US Crude Rate'].astype(str)
    new_df['US Age-adjusted Rate'] = new_df['US Age-adjusted Rate'].astype(str)

    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from sqlalchemy import create_engine

    # Connection parameters for PostgreSQL
    database = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "Mypassword07"
    }

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

    # Create the database engine
    # PostgreSQL connection parameters
    username = 'postgres'
    password = 'Mypassword07'
    host = 'localhost'
    port = '5432'
    database = 'DAP Project'
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

    #Finally our all connection setup is done and now by using the 'to_sql' will dump all the
    #data in database
    new_df.to_sql('Accidential Data', con=engine, index=False, if_exists='replace')

    #Finally our all connection setup is done and now by using the 'to_sql' will dump all the
    #data in database
    new_df.to_sql('NCHS', con=engine, index=False, if_exists='replace')

@asset
def Opiod_EMS_data_to_be_Dumped_in_postgress(Opiod_EMS_Dataset_EDA):
    df = Opiod_EMS_Dataset_EDA
    df['_id'] = df['_id'].astype(str)
    df['Incident_Date'] = pd.to_datetime(df['Incident_Date'])

    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from sqlalchemy import create_engine

    # Connection parameters for PostgreSQL
    database = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "Mypassword07"
    }

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

    # Create the database engine
    # PostgreSQL connection parameters
    username = 'postgres'
    password = 'Mypassword07'
    host = 'localhost'
    port = '5432'
    database = 'DAP Project'
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

    #Finally our all connection setup is done and now by using the 'to_sql' will dump all the
#data in database
    df.to_sql('Opiod EMS Calls', con=engine, index=False, if_exists='replace')





@job
def my_data_pipeline():
    Accidential_data_to_be_Dumped_in_postgress(EDA_Accidental_Data())

@job
def my_data_pipeline():
    NCHS_data_to_be_Dumped_in_postgress(NCHS_Dataset_EDA())

@job
def my_data_pipeline():
    Opiod_EMS_data_to_be_Dumped_in_postgress(Opiod_EMS_Dataset_EDA())










