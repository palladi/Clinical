import requests
import pandas as pd
import datetime
import uuid

# Base URL of the API
base_url = "https://clinicaltrials.gov/api/v2/studies"

# Parameters for pagination
params = {'pageToken': 'KV1-7ZuCkPYg'}  # Adjust the limit according to API's pagination size

def is_valid_date(date_str, format='%Y-%m-%d'):
    try:
        ref_date = datetime.datetime.strptime(date_str, format)
        return True
    except ValueError:
        return False

def fetcher(endpage=None):
    """Fetches the entire data from the API, 10 studies at a time
    
    Args:
        endpage - pageToken of the end page in str
    
    Returns:
        dataframe containing all the study results
    """
    data_frames = []
    i = 1
    while True and i < 15:
        # Make a GET request to the API
        response = requests.get(base_url, params=params)

        # Check if the request was successful
        if response.status_code != 200:
            print(f"Failed to fetch data. Status code: {response.status_code}")

        # Parse the JSON response
        data = response.json()

        # If no data is returned, break the loop
        if not data:
            break
        i += 1
        # Convert the JSON data to a DataFrame and add date
        df = pd.DataFrame(data)
        df['pipeline_start_timestamp'] = datetime.datetime.now()
        df['pipeline_run_id'] = str(uuid.uuid4())


        # Append the DataFrame to the list
        data_frames.append(df)

        # Increment the page number for the next request
        if df.iloc[-1, 1] is None or df.iloc[-1, 1] == '':
            break
        else:
            print('next page token', df.iloc[-1, 1])
            params['pageToken'] = df.iloc[-1, 1]

    # Concatenate all DataFrames
    full_data = pd.concat(data_frames, ignore_index=True)
    return full_data


def df_filter(df, date):
    """ filters the rows of the df by lastUpdatePostDate
    
    Args:
        df - dataframe
        date - reference date
        
    Returns:
        filtered dataframe
    """
    try:
        datetime_obj = datetime.datetime.strptime(df['lastUpdateSubmitDate'], "%Y-%m-%d")
        return datetime_obj > date
    except KeyError:
        return False


def organiser(df, rows):
    """ Breaks down the dataframe in to parquet files
    
    Args:
        df - data frame
        rows - max no.of rows per file
        
    Returns:
        batches of data in multiple parquet files
    """
    files = (len(df) + rows - 1) // rows

    # Split the DataFrame into smaller slices and save each as a parquet file
    for i in range(files):
        # Get the start and end indices for the current chunk
        start_idx = i * files
        end_idx = min(start_idx + rows, len(df))

        # Slice the DataFrame to get the current chunk
        sliced_df = df.iloc[start_idx:end_idx]
        now = datetime.datetime.now()
        base_name = now.strftime('%Y%m%d%H%M%S')

        # Create a filename for the current slice
        filename = f'{base_name}_{i+1}.parquet'

        # Save the current chunk as a Parquet file
        sliced_df.to_parquet(filename)
        print(f'Saved {filename}')

#ref_date = datetime.datetime.strptime('2024-01-01', "%Y-%m-%d")


if __name__ == "__main__":
    a = str(input("Would you like to filter the studies update after a specific date? (y/n): "))
    if a == 'y' :
        b = input("Please enter the reference date in yyyy-mm-dd format: ")
        if is_valid_date(b):
            ref_date = datetime.datetime.strptime(b, "%Y-%m-%d")

            # Fetch the data
            full_data = fetcher()

            # Filter the fetched data by last update post date
            filtered_df = full_data[
                full_data['studies'].apply(lambda x: df_filter(x['protocolSection']['statusModule'], ref_date))]

            # Split the data into parquet files
            organiser(filtered_df, 50)

        else:
            print(f"{b} is not a valid date.")

    elif a == 'n' :

        # Fetch the data
        full_data = fetcher()

        # Split the data into parquet files
        organiser(full_data, 50)

    else:
        raise ("Invalid input")