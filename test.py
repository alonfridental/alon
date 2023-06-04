import glob
import os
import gc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import requests
import gzip
from io import BytesIO
from datetime import datetime
import sqlite3
from tqdm import tqdm
from selenium.webdriver.chrome.service import Service

# Read the text file
# with open("header.txt", "r") as f:
#     lines = f.readlines()
# column_names = ['timestamp', 'user_id', 'pixel_color', 'x', 'y']
# data = []
# for line in lines[1:]:
#     cols = line.strip().split(",")
#     values = {
#         'timestamp': cols[0],
#         'user_id': cols[1],
#         'pixel_color': cols[2],
#         'x': cols[3][1:],
#         'y': cols[4][:-1]
#     }
#     data.append(values)
# pixel_df = pd.DataFrame(data, columns=column_names)
# pixel_df['timestamp'] = pixel_df['timestamp'].str[:-4]
#
# pixel_df['timestamp'] = pd.to_datetime(pixel_df['timestamp'], format="ISO8601")

# pixel_df.to_csv("test.csv")


url = "https://placedata.reddit.com/data/canvas-history/index.html"


def getUrl(web_url):
    # Launch a new instance of the Chrome browser
    # options = Options()
    # options.add_experimental_option('detach', True)
    chrome_options = webdriver.ChromeOptions()
    service = Service("C://webdrivers//chromedriver.exe")  # Replace '/path/to/chromedriver' with the actual path
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # Navigate to the URL of the web page
    driver.get(web_url)
    # Find all the <a> tags in the web page and extract their "href" attributes
    urls = []
    for link in driver.find_elements(By.TAG_NAME, "a"):
        url = link.get_attribute("href")
        if url is not None:
            urls.append(url)
    driver.quit()
    return urls


urls = getUrl(url)


#
#
# def createDF(url):
#     response = requests.get(url)
#     compressed_file = BytesIO(response.content)
#     decompressed_file = gzip.GzipFile(fileobj=compressed_file)
#     df = pd.read_csv(decompressed_file, header=0, names=["timestamp", "user_id", "pixel_color", "coordinate"])
#     return df
def createDF(url):
    response = requests.get(url)
    compressed_file = BytesIO(response.content)
    try:
        decompressed_file = gzip.GzipFile(fileobj=compressed_file)
        df = pd.read_csv(decompressed_file, header=0, names=["timestamp", "user_id", "pixel_color", "coordinate"])
        return df
    except Exception as e:
        print(f"Error reading file: {url}")
        print(e)
        return None


#

def getLastTimestamp(urls):
    smallest_timestamp = datetime(1, 1, 1, 0, 0, 0).isoformat() + 'Z'
    iso8601_format = "%Y-%m-%dT%H:%M:%SZ"
    final_max_timestamp = datetime.strptime(smallest_timestamp, iso8601_format)
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS max_timestamps (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        max_timestamp TEXT)''')
    conn.commit()

    batch_size = 10
    url_batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    for batch in tqdm(url_batches):
        for url in batch:
            # Download the gzipped csv file from the URL
            df = createDF(url)
            df['timestamp'] = df['timestamp'].str[:-4]
            df['timestamp'] = pd.to_datetime(df['timestamp'], format="ISO8601")
            # find the time everyone changed to white
            temp_max = df.loc[df['pixel_color'] != '#FFFFFF', 'timestamp'].max()
            if pd.isna(temp_max):
                continue
            df = df.dropna(subset=['timestamp'])
            temp_max = pd.Timestamp(temp_max)
            if temp_max > final_max_timestamp:
                final_max_timestamp = temp_max
            del df
            gc.collect()
            cursor.execute("INSERT INTO max_timestamps (max_timestamp) VALUES (?)", (str(final_max_timestamp),))
            conn.commit()
    conn.close()

    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute("SELECT max(max_timestamp) FROM max_timestamps")
    results = cursor.fetchall()
    conn.close()
    for row in results:
        return row[0]


#

def lastColor(pixel_df):
    # get the time when all the pixels went white.
    # max_timestamp = getLastTimestamp(urls)
    temp_max_timestamp = "2022-04-04 22:47:40"
    iso8601_format = "%Y-%m-%d %H:%M:%S"
    max_timestamp = datetime.strptime(temp_max_timestamp, iso8601_format)
    filtered_df = pixel_df[pixel_df['timestamp'] <= max_timestamp]
    # pixel_df['coordinate'] = pixel_df['coordinate'].apply(tuple)
    max_idx = filtered_df.groupby('coordinate')['timestamp'].idxmax()
    result = filtered_df.loc[max_idx, ['coordinate', 'pixel_color', 'timestamp']]
    return result


#
#
# # #
# temp = lastColor(pixel_df)
# print(temp.head(4))


#
def placementUpdate(pixel_df):
    # max_timestamp = getLastTimestamp(urls)
    temp_max_timestamp = "2022-04-04 22:47:40"
    iso8601_format = "%Y-%m-%d %H:%M:%S"
    max_timestamp = datetime.strptime(temp_max_timestamp, iso8601_format)
    filtered_df = pixel_df[pixel_df['timestamp'] <= max_timestamp]
    # pixel_df['coordinate'] = pixel_df['coordinate'].apply(tuple)
    coord_count = filtered_df.groupby('coordinate').size().reset_index(name="pixel_allocations")
    return coord_count


# color_data = lastColor(pixel_df)
# pixel_allocations_df = placementUpdate(pixel_df)
# merged_df = pixel_allocations_df.merge(color_data, on='coordinate', how='outer')
#
# print(merged_df.head(3))

# # pixel_df.to_excel("test.csv")
#
#

# def myFunc(urls, batch_size=5):
#     conn = sqlite3.connect('database.db')
#     cursor = conn.cursor()
#     cursor.execute('''CREATE TABLE IF NOT EXISTS pixel_data (
#                           id INTEGER PRIMARY KEY AUTOINCREMENT,
#                           coordinate TEXT,
#                           pixel_allocations INTEGER,
#                           final_color TEXT)''')
#     conn.commit()
#     pixel_allocations_df = pd.DataFrame(columns=['coordinate', 'pixel_allocations'])
#
#     url_batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
#     for url_batch in tqdm(url_batches):
#         temp_pixel_allocations = pd.DataFrame(columns=['coordinate', 'pixel_allocations'])
#         temp_color_data = pd.DataFrame(columns=['coordinate', 'final_color'])
#
#         for url in url_batch:
#             df = createDF(url)
#             df['timestamp'] = df['timestamp'].str[:-4]
#             df['timestamp'] = pd.to_datetime(df['timestamp'], format="ISO8601")
#             temp_placement_counter = placementUpdate(df)
#             temp_placement_counter['pixel_allocations'] = temp_placement_counter.groupby('coordinate')['count'].cumsum()
#             temp_pixel_allocations = pd.concat([temp_pixel_allocations, temp_placement_counter])
#
#             color_data = lastColor(df)
#             temp_color_data = pd.concat([temp_color_data, color_data])
#
#             del df, temp_placement_counter, color_data
#             gc.collect()
#
#         temp_pixel_allocations = temp_pixel_allocations.groupby('coordinate').max().reset_index()
#         merged_df = temp_pixel_allocations.merge(temp_color_data, on='coordinate', how='outer')
#
#         cursor.executemany("INSERT INTO pixel_data (coordinate, pixel_allocations, final_color) VALUES (?, ?, ?)",
#                            merged_df[['coordinate', 'pixel_allocations', 'pixel_color']].values.tolist())
#         conn.commit()
#
#         del temp_pixel_allocations, temp_color_data, merged_df
#         gc.collect()
#
#     cursor.execute("SELECT coordinate, pixel_allocations, final_color FROM pixel_data")
#     results = cursor.fetchall()
#     conn.close()
#     result_df = pd.DataFrame(results, columns=['coordinate', 'pixel_allocations', 'final_color'])
#     return result_df

def myFunc(urls, batch_size=5):
    # conn = sqlite3.connect('database.db')
    # cursor = conn.cursor()
    # cursor.execute('''CREATE TABLE IF NOT EXISTS pixel_data (
    #                       id INTEGER PRIMARY KEY AUTOINCREMENT,
    #                       coordinate TEXT,
    #                       pixel_allocations INTEGER,
    #                       final_color TEXT)''')
    # conn.commit()

    url_batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    result_files = []

    for batch_idx, url_batch in enumerate(tqdm(url_batches[:1])):
        temp_pixel_allocations = pd.DataFrame(columns=['coordinate', 'pixel_allocations', 'timestamp'])
        temp_color_data = pd.DataFrame(columns=['coordinate', 'final_color', 'timestamp'])

        for url_idx, url in enumerate(url_batch[:1]):
            try:
                df = createDF(url)
                if df is None:
                    continue
                df['timestamp'] = df['timestamp'].str[:-4]
                df['timestamp'] = pd.to_datetime(df['timestamp'], format="ISO8601")
                temp_placement_counter = placementUpdate(df)
                temp_pixel_allocations = pd.concat([temp_pixel_allocations, temp_placement_counter])

                color_data = lastColor(df)
                color_data.rename(columns={'pixel_color': 'final_color'}, inplace=True)
                temp_color_data = pd.concat([temp_color_data, color_data])

                del df, temp_placement_counter, color_data
                gc.collect()
            except Exception as e:
                print(f"Error processing URL: {url}")
                print(e)

        temp_pixel_allocations = temp_pixel_allocations.groupby('coordinate').max().reset_index()
        temp_pixel_allocations = temp_pixel_allocations.drop('timestamp', axis=1)
        merged_df = temp_pixel_allocations.merge(temp_color_data, on='coordinate', how='outer', suffixes=('', ''))

        result_filename = f"result_batch_{batch_idx}.csv"
        result_files.append(result_filename)
        merged_df.to_csv(result_filename, index=False)
        #
        # cursor.executemany("INSERT INTO pixel_data (coordinate, pixel_allocations, final_color) VALUES (?, ?, ?)",
        #                    merged_df[['coordinate', 'pixel_allocations', 'final_color']].values.tolist())
        # conn.commit()

        del temp_pixel_allocations, temp_color_data, merged_df
        gc.collect()

    # conn.close()

    return result_files


# Call the function and get the result files and DataFrame
result_files = myFunc(urls)

# # Iterate over the result files and print their contents
# for result_file in result_files:
#     df = pd.read_csv(result_file)
#     # Group by "coordinate" and find the row with maximum "timestamp" for each coordinate
#     max_timestamp_df = df.groupby('coordinate')['timestamp'].max().reset_index()
#     # Merge with the original DataFrame to get the corresponding values for other columns
#     merged_df = pd.merge(df, max_timestamp_df, on=['coordinate', 'timestamp'])
#     # Write the modified DataFrame back to the result file, overwriting the previous content
#     merged_df.to_csv(result_file, index=False)
#     print(f"Contents of {result_file}:")
#     print(df.head(2))

merged_df = None

# Iterate over the result files and merge their contents
for result_file in result_files:
    df = pd.read_csv(result_file)
    if merged_df is None:
        merged_df = df
    else:
        # Merge the current DataFrame with the previously merged DataFrame
        merged_df = pd.concat([merged_df, df])

# Group by "coordinate" and find the row with the maximum "timestamp" for each coordinate
max_timestamp_df = merged_df.groupby('coordinate')['timestamp'].max().reset_index()

# Merge with the merged DataFrame to get the corresponding values for other columns
merged_df = pd.merge(merged_df, max_timestamp_df, on=['coordinate', 'timestamp'])

# Iterate over the result files again and update their contents with the max timestamps
# for result_file in result_files:
#     df = pd.read_csv(result_file)
#     # Merge the original DataFrame with the max_timestamp_df based on "coordinate" and "timestamp" columns
#     merged_df_temp = pd.merge(df, max_timestamp_df, on=['coordinate', 'timestamp'])
#     # Write the modified DataFrame back to the result file, overwriting the previous content
#     merged_df_temp.to_csv(result_file, index=False)
#     print(f"Contents of {result_file}:")
#     print(merged_df_temp.head(2))

import glob
import pandas as pd

result_files = glob.glob('result*.csv')

max_timestamps = {}

for result_file in result_files:
    for chunk in pd.read_csv(result_file, chunksize=100000):  # Adjust chunk size as per your system's memory capacity
        for index, row in chunk.iterrows():
            coordinate = row['coordinate']
            timestamp = row['timestamp']
            if coordinate in max_timestamps:
                max_timestamps[coordinate] = max(max_timestamps[coordinate], timestamp)
            else:
                max_timestamps[coordinate] = timestamp

for coordinate, max_timestamp in max_timestamps.items():
    print(f"Coordinate: {coordinate}, Max Timestamp: {max_timestamp}")



