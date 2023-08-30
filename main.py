# This is a sample Python script.

import json
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import pandasql
from flask import Flask, jsonify


def load_data_from_json(filename_json: str) -> list:
    """
    This function loads data from a json.

    Parameters:
    filename_json (string): filename of the json.

    Returns:
    list: list with the content of the json in case it has been possible to read it .

    Raises:
    FileNotFoundError: File not found.
    JSONDecodeError: Invalid JSON data.

    Example:
    load_data_from_json("storage.googleapis.com_xcc-de-assessment_events.json")
    """

    try:
        with open(filename_json, 'r') as json_file:
            return json.load(json_file)

    except FileNotFoundError:
        print("File not found.")
    except json.JSONDecodeError:
        print("Invalid JSON data.")


def flatten_data(data_in_list_format):
    """
    This function flatens the data from a list into a pandas dataframe.

    Parameters:
    arg1 (d): data to flatten.

    Returns:
    dataframe: dataframe with the content of the list from the json in dataframe format .

    Example:
    flatten_data(dataframe)
    """

    # flatten the data
    # Flatten the nested data
    flattened_data = []
    for item in data_in_list_format:
        flattened_item = {
            'id': item['id'],
            'type': item['type'],
            'customer-id': item['event']['customer-id'],
            'timestamp': item['event']['timestamp']
        }
        flattened_data.append(flattened_item)

    # Convert the flattened data to a DataFrame
    return pd.DataFrame(flattened_data)


def calculate_sessions(dataframe):
    """
    This function calculate sessions series considering the columns customer-id and time_diff.

    Parameters:
    dataframe (Dataframe): dataframe that contains time_dif and customer-id.

    Returns:
    series: sessions .

    Example:
    calculate_sessions(dataframe)
    """
    # Create a session column based on 'customer-id' and 'time_diff'
    sessions = []
    current_session = 0
    session_time_diff = 0
    previous_customer = 0
    cumulative_sum_dif = 0

    for index, row in dataframe.iterrows():
        # new customer, new session
        if row["customer-id"] != previous_customer:
            current_session += 1
            cumulative_sum_dif = 0

        if row["customer-id"] == previous_customer:

            if row['time_diff'] < 3600:
                cumulative_sum_dif += row['time_diff']
               #if cumulative_sum_dif since the beginning of the session is bigger than an hour it is a new session
                if cumulative_sum_dif > 3600:
                    current_session += 1
                    cumulative_sum_dif = 0
                # if cumulative_sum_dif < 3600 then it is the same session and cumulative_sum_dif is the same
            else:  # time_diff is bigger than 3600 with previous row
                current_session += 1
                cumulative_sum_dif = 0

        sessions.append(current_session)

        previous_customer = row["customer-id"]

    dataframe['session'] = sessions

    return dataframe


def save_postgres_docker(dataframe, dataframe_name):
    """
    This function calculate save a dataframe with a determined name in a postgres database.

    Parameters:
    dataframe (Dataframe): dataframe to save.
    dataframe_name (string): dataframe name we are going to use.

    Returns:

    Example:
    save_postgres_docker(dataframe, "dataframe_name_1")
    """
    # Connect to the PostgreSQL database
    db_params = {
        'database': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': '5432'
    }

    connection = psycopg2.connect(**db_params)

    # Create a SQLAlchemy engine
    engine = create_engine('postgresql://myuser:mypassword@localhost:5432/mydatabase')

    # Save the DataFrame to the database
    dataframe.to_sql(dataframe_name, con=engine, if_exists='replace', index=False)

    # Close the database connection
    connection.close()


def read_postgres_docker(dataframe_name):
    """
    This function reads dataframe with dataframe_name from postgres database.

    Parameters:
    dataframe_name (string): dataframe name of the dataframe we are going to read.

    Returns:
        dataframe: dataframe once it has been read

    Example:
    read_postgres_docker( "dataframe_name_1")
    """
    db_params = {
        'database': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': '5432'
    }

    connection = psycopg2.connect(**db_params)

    # Load the DataFrame from the database
    query_sql = f"SELECT * FROM {dataframe_name};"

    engine = create_engine('postgresql://myuser:mypassword@localhost:5432/mydatabase')

    df_from_db = pd.read_sql(query_sql, con=engine)

    connection.close()

    return df_from_db


def customer_bought_at_least_once(dataframe):
    """
    This function receives a dataframe and returns the dataframe only with rows whose customers have bought at least once.

    Parameters:
    dataframe (dataframe): dataframe to filter.

    Returns:
        dataframe: dataframe filtered only with customers who bought at least once.

    Example:
    customer_bought_at_least_once(dataframe)
    """
    # we only calculate if there is a buy in one session
    dataframe['there_is_buy'] = dataframe['type'].apply(lambda x: x == 'placed_order')

    customers_who_bought_at_least_once = dataframe.groupby(['customer_id', 'session'])['there_is_buy'].any().reset_index()

    filtered_df = customers_who_bought_at_least_once[customers_who_bought_at_least_once['there_is_buy']]

    # Filter to keep only customers with at least one there_is_buy=True
    list_of_customers_who_bought = filtered_df['customer_id'].unique()

    customers_that_bought_at_least_once = customers_who_bought_at_least_once[
        customers_who_bought_at_least_once['customer_id'].isin(list_of_customers_who_bought)]

    dataframe_filtered_customers_bought_once = dataframe[
        dataframe['customer_id'].isin(customers_that_bought_at_least_once["customer_id"])]

    return dataframe_filtered_customers_bought_once

def calculate_time_diff(dataframe):
    """
    This function receives a dataframe and returns the dataframe with time_diff added

    Parameters:
    dataframe (dataframe): dataframe to add time_diff.

    Returns:
        dataframe: dataframe with column time_diff added

    Example:
    calculate_time_diff(dataframe)
    """
    # sort timestamp to reckon time_diff later between visits
    df_order_timestamp = dataframe.sort_values(by=['customer-id', 'timestamp'], ascending=True)

    df_order_timestamp['timestamp'] = pd.to_datetime(df_order_timestamp['timestamp'])

    df_order_timestamp["time_diff"]=df_order_timestamp.groupby('customer-id')['timestamp'].diff().dt.total_seconds().fillna(0)

    return df_order_timestamp


def create_api(dataframe):
    """
    This function creates a flask Api with the dataframe content.

    Parameters:
    dataframe (dataframe): dataframe for creating the API.

    Example:
    create_api(dataframe)
    """
    app = Flask(__name__)

    @app.route('/metrics/orders', methods=['GET'])
    def get_order_metrics():
        median_visits = dataframe["average_of_averages"].to_dict()
        median_duration = dataframe["average_time_diff_per_counter"].to_dict()

        response = {
            "median_visits_before_order": median_visits,
            "median_session_duration_minutes_before_order": median_duration
        }
        return jsonify(response)

    app.run(debug=True)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    data = load_data_from_json('storage.googleapis.com_xcc-de-assessment_events.json')
    #flatten data into a dataframe
    data_flattened = flatten_data(data)

    # remove not null customers
    df_with_not_null_customer = data_flattened[data_flattened['customer-id'].notna()]

    # calculate time_diff between visits
    df_with_time_diff = calculate_time_diff(df_with_not_null_customer)

    #added
    df_with_sessions = calculate_sessions(df_with_time_diff)

    ##save it into a csv
    csv_filename = 'data.csv'
    df_with_sessions.to_csv(csv_filename, index=False)

    #save it into postgres
    save_postgres_docker(df_with_sessions, "dataframe_step_1")

    #read it from postgres to check it is working
    dataframe_read_from_postgres = read_postgres_docker("dataframe_step_1")


    """
    STEP 2
    """

    df_step1_renamed = dataframe_read_from_postgres.rename(columns={'customer-id': 'customer_id'})
    #stay with customers who bought at least once
    dataframe_step1_filtered_customers_bought_once = customer_bought_at_least_once(df_step1_renamed)

    query_sql = """
            SELECT customer_id, 
                AVG(sesions_until_buy) AS average_of_averages,
                AVG(sum_time_diff_per_counter_until_buy) AS average_time_diff_per_counter 
            FROM (
                SELECT
                    subquery.customer_id,
                    subquery.counter,
                    COUNT(subquery.counter) AS sesions_until_buy,
                    SUM(subquery.time_diff) AS sum_time_diff_per_counter_until_buy 
                FROM (
                    SELECT
                        dataframe_step1_filtered_customers_bought_once.*,  -- Insert the table name here
                        1 + SUM(there_is_buy) OVER (PARTITION BY customer_id ORDER BY session) - there_is_buy AS counter
                    FROM dataframe_step1_filtered_customers_bought_once) subquery
                GROUP BY subquery.customer_id, subquery.counter
            ) subquery
            GROUP BY customer_id
        """

    final_dataframe = pandasql.sqldf(query_sql)
    ##create API
    create_api(final_dataframe)
