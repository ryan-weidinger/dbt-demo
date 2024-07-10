###########################################################################
### Description: This script initializes test data for the project      ###
### It will create a file or database and table, and load data into it. ###
### Formats supported currently:                                        ###
###   - CSV                                                             ###
###   - Snowflake                                                       ###
### This script is mapped to the 'generate_data' make command.          ###
###########################################################################

# Import requires packages
import pandas as pd
from faker import Factory
from random import randint
from enum import Enum
from datetime import timedelta
from typing import List, Dict
from abc import ABC, abstractmethod
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import dotenv_values


# Define the supported formats
class StorageFormats(Enum):
    CSV = 1
    SNOWFLAKE = 2

class TestData(ABC):
    """
    Abstract class to create, store, and write test data
    """

    @abstractmethod
    def generate_data(self, num_records: int) -> List[Dict]:
        pass

    def data_to_df(self) -> pd.DataFrame:
        try:
            return pd.DataFrame(self.data)
        except AttributeError as ae:
            raise AttributeError(f"{ae}. \nMust create test data before writing to CSV")
        except Exception as e:
            raise e
    
    def write_to_csv(self, path: str = ''):
        
        # Convert data to DataFrame
        data_df = pd.DataFrame(self.data)
        
        # Write data to CSV
        try:
            data_df.to_csv(path, index=False)
        except Exception as e:
            print("Exception while writing data to CSV")
            raise e
    
    def write_to_snowflake(self, conn, table_name: str, database: str, schema: str) -> None:
        
        # Convert data to DataFrame
        data_df = pd.DataFrame(self.data)
        
        # Write data to Snowflake
        try:
            write_pandas(
                conn=conn, 
                df=data_df, 
                table_name=table_name,
                database=database,
                schema=schema,
                auto_create_table=True,
                overwrite=True,
            )
            # data_df.to_sql(table_name, conn, if_exists='replace')
        except Exception as e:
            print("Exception while writing data to Snowflake")
            raise e
        

class CustomerMasterTestData(TestData):
    """
    Class to create, store, and write fake customer master data
    """

    def generate_data(self, num_records: int) -> List[Dict]:
        """
        Method to produce fake customer master data
        :param num_records: Number of records to generate
        :return: List of dictionaries containing the data
        """

        # Define the list of countries
        country_locale_map = {
            "US": "en_US", 
            "UK": "en_GB",
        }
        
        # Create a list to store the data
        data = []
        for i in range(num_records):

            # Randomly select a country
            country = list(country_locale_map.keys())[randint(0, len(country_locale_map) - 1)]

            # Create a faker object for the given country
            faker = Factory.create(country_locale_map[country])

            # Set certain fields based on the country
            if country == "US":
                state = faker.state_abbr()
                county = None
            else:
                state = None
                county = faker.county()

            # Randomly generate some fake data elements that other elements depend on
            is_active = faker.boolean(
                chance_of_getting_true=90
            )
            created_at = faker.date_time_this_century()
            updated_at = created_at + timedelta(randint(1, 7), randint(60, 1000))
            if not is_active:
                deleted_at = updated_at + timedelta(randint(1, 7), randint(60, 1000))
            else:
                deleted_at = None
            
            # Append the data to the list of data dictionaries
            data.append(
                {
                    "cust_id": randint(10000, 20000),
                    "first_name": faker.first_name(),
                    "last_name": faker.last_name(),
                    "email": faker.email(),
                    "phone": faker.phone_number(),
                    "address": faker.address(),
                    "city": faker.city(),
                    "county": county,
                    "state": state,
                    "postal_code": faker.postcode(),
                    "country": country,
                    "is_active": is_active,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": deleted_at,
                }
            )
        self.data = data


# Run this code when script is executed
if __name__ == "__main__":

    # Choose the number of records to generate
    num_records = input("Enter the number of records to generate [1000]: ")
    num_records = int(num_records) if num_records else 1000
    print(f"Number of records to generate: {num_records:,}")

    # Create dictionary of supported formats
    valid_format_dict = {str(format.value): format.name for format in StorageFormats}

    # Let the user choose the format to use
    format_input = input(
        f"Choose a data format to use to create test data: {valid_format_dict} "
    )
    print(f"Format option selected: '{format_input}'")

    try:
        format_select = valid_format_dict[format_input]
    except KeyError:
        print(
            f"Invalid value given for format. Accepted values are: {valid_format_dict.keys()} \nUsing CSV by default..."
        )
        format_select = "CSV"
    print(f"Test data format: '{format_select}'")

    # Create test customer master data
    customer_test_data = CustomerMasterTestData()
    customer_test_data.generate_data(num_records=100)
    
    # Write data to CSV
    if format_select == "CSV":
        customer_test_data.write_to_csv(path="data/source/customer_master_test_data.csv")
    elif format_select == "SNOWFLAKE":

        # Import configuration parameters
        env = dotenv_values('.env')
        print(env)
        USER = env.get("USER")
        PASSWORD = env.get("PASSWORD")
        ACCOUNT = env.get("ACCOUNT")
        WAREHOUSE = env.get("WAREHOUSE")
        DATABASE = env.get("DATABASE")
        SCHEMA = env.get("SCHEMA")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
        )
        print("Connected to Snowflake")

        # Write data to Snowflake
        customer_test_data.write_to_snowflake(
            conn=conn, 
            table_name="CUSTOMER_MASTER_TEST_DATA",
            database=DATABASE,
            schema=SCHEMA,
        )

        # Close the connection
        conn.close()
    
    else:
        print(f"Format '{format_select}' not supported yet. Please select a supported format.")

