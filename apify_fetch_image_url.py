import sys
import os
import time
import json
import requests
import pandas as pd
import numpy as np
from apify_client import ApifyClient
from sqlalchemy import create_engine, text
import psycopg2
from io import StringIO
import boto3
from io import BytesIO
import xlsxwriter

from datetime import datetime, timedelta

current_date = datetime.now()
cutoff_date = current_date - timedelta(days=30)

 
S3_BUCKET = ''
DATABASE_URL = ""
table_name = "e"


# Apify tokens and actor IDs
APIFY_TOKEN = ""
APIFY_FACEBOOK_POST_SCRAPER = ""
APIFY_X_SCRAPER = ""
APIFY_YOUTUBE_SCRAPER = "r"

# (Optional) Another Apify client token for IG, if you have a separate token
APIFY_TOKEN_INSTAGRAM = "apify_a"

# For Instagram
# We'll assume you have an actor "nH2AHrwxeTRJoN5hX", as in your code.
APIFY_IG_ACTOR_ID = "nH2AhX"


def fetch_data_from_postgresql(table_name):
    try:
        # Create a connection object
        engine = create_engine(DATABASE_URL)
        connection = engine.connect()

        # Use pandas to read data from the specified PostgreSQL table
        df_sql = pd.read_sql(table_name, connection)
        
        # Close the connection
        connection.close()
        
        return df_sql
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame() 
  
  
  


# -----------------------------------------------------------------------------
# 2. Helper functions
# -----------------------------------------------------------------------------

def apify_run(actor_id, run_input, token=APIFY_TOKEN):
    """
    Helper function that triggers an Apify actor run, waits for completion,
    and returns the results as a pandas DataFrame.
    """
    apify_client = ApifyClient(token)
    
    # Trigger the actor
    run = apify_client.actor(actor_id).call(run_input=run_input)
    
    # Get dataset ID from the run
    try:
        dataid = run["defaultDatasetId"]
    except KeyError:
        print("Error: Could not get dataset ID from the run response.")
        return pd.DataFrame()

    # Fetch items from the dataset
    dataset_url = f"https://api.apify.com/v2/datasets/{dataid}/items?token={token}"
    response = requests.get(dataset_url)
    
    if response.status_code != 200:
        print(f"Error retrieving data from dataset: {response.text}")
        return pd.DataFrame()
    
    data_json = response.json()
    if not data_json:
        print("No data returned from the Apify actor.")
        return pd.DataFrame()

    df = pd.DataFrame(data_json)
    return df


def facebook_input(urls):
    """
    Prepares the input dictionary for the Facebook posts scraper on Apify.
    """
    start_urls = []
    for url in urls:
        # Apify requires an object with { "url": ..., "method": "GET" }
        start_urls.append({"url": url, "method": "GET"})
    
    data_input = {
        "resultsLimit": 400, 
        "startUrls": start_urls
    }
    return data_input


def parse_facebook_posts(df):
    """
    Parses the Facebook posts DataFrame returned by the Apify Facebook Post Scraper.
    Extracts the 'facebookUrl', image link, and any OCR text from the media.
    """
    results = []
    for index, row in df.iterrows():
        facebook_url = row.get("facebookUrl", "")
        image_link = None
        ocr_text = None

        # Check 'preferred_thumbnail' first
        preferred_thumbnail = row.get("preferred_thumbnail")
        if preferred_thumbnail and isinstance(preferred_thumbnail, dict):
            image_link = preferred_thumbnail.get('image', {}).get('uri')
        elif preferred_thumbnail and isinstance(preferred_thumbnail, str):
            try:
                preferred_thumbnail_data = json.loads(preferred_thumbnail)
                image_link = preferred_thumbnail_data.get('image', {}).get('uri')
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"Error parsing preferred_thumbnail at row {index}: {e}")

        # If 'image_link' is None, check 'media'
        if not image_link:
            media = row.get("media")
            if media and isinstance(media, list) and len(media) > 0:
                media_item = media[0]
                image_link = (
                    media_item.get('image', {}).get('uri') or
                    media_item.get('url') or
                    media_item.get('thumbnail')
                )
                ocr_text = media_item.get('ocrText', 'No OCR text found')

        results.append({
            "facebookUrl": facebook_url,
            "image_link": image_link if image_link else "No image found",
            "ocr_text": ocr_text if ocr_text else "No OCR text found"
        })

    return pd.DataFrame(results)





def run_apify_actor_ig(urls, batch_size=100):
    """
    Runs the Instagram Apify actor with a batch approach.
    The actor ID is assumed to be 'nH2AHrwxeTRJoN5hX' from your example.
    """
    # Initialize the ApifyClient (with your IG token or main token if same)
    client = ApifyClient("")

    # Split the URL list into batches
    batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    all_results = []
    
    for batch in batches:
        # Prepare the Actor input with the batch of URLs
        run_input = {
            "username": batch,  # or if your actor expects "startUrls": ...
            "resultsLimit": 30,
        }
        
        # Run the Actor and wait for it to finish
        run = client.actor(APIFY_IG_ACTOR_ID).call(run_input=run_input)
        
        # Iterate over the items in the default dataset
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            # Here we assume 'displayUrl', 'inputUrl', etc. are returned
            display_url = item.get('displayUrl', None)
            input_url = item.get('inputUrl', None)
            in_url = item.get('url', None)
            all_results.append({
                'input_url': input_url,
                'displayUrl': display_url,
                'url': in_url
            })

    # Convert the results to a DataFrame
    df_results = pd.DataFrame(all_results)
    return df_results




def x_input(urls):
    """
    Prepares the input dictionary for the X (Twitter) scraper on Apify.
    """
    start_urls = [{"url": url} for url in urls]

    data_input = {
        "customMapFunction": "(object) => { return {...object} }",
        "end": "2024-11-30",
        "includeSearchTerms": False,
        "maxItems": 500,
        "onlyImage": False,
        "onlyQuote": False,
        "onlyTwitterBlue": False,
        "onlyVerifiedUsers": False,
        "onlyVideo": False,
        "sort": "Latest",
        "start": "2023-01-01",
        "startUrls": start_urls,
        "tweetLanguage": "en"
    }
    return data_input


def parse_apify_output_x(df):
    """
    A more robust parser for the output of the Apify X (Twitter) scraper.
    Handles missing or unexpected columns.
    """
    if df.empty:
        print("Warning: Received an empty DataFrame from Apify for Twitter.")
        return pd.DataFrame(columns=["url", "image_link"])

    # Check if essential columns exist
    required_columns = {"twitterUrl", "media"}
    if not required_columns.issubset(set(df.columns)):
        print(f"Warning: The DataFrame does not have the required columns: {required_columns}")
        print(f"Actual columns are: {list(df.columns)}")
        return pd.DataFrame(columns=["url", "image_link"])

    # Everything looks good, proceed
    df_subset = df[["twitterUrl", "media"]].copy()

    # Convert the 'media' column to a more consistent format
    # Sometimes 'media' can be None, an empty list, or unexpected
    def extract_image_link(media_field):
        if isinstance(media_field, list) and len(media_field) > 0:
            # If it's a list, assume the first item is the main image link
            return media_field[0]
        return ""

    df_subset["image_link"] = df_subset["media"].apply(extract_image_link)

    # Rename columns for consistency
    df_subset.rename(columns={"twitterUrl": "url"}, inplace=True)
    # Keep only the columns we actually need
    df_subset = df_subset[["url", "image_link"]]

    return df_subset


def process_urls_in_batches(df, batch_size, sleep_time=10):
    """
    Processes X (Twitter) URLs in batches with a specified sleep time between batches.
    Returns a combined DataFrame containing results for all batches.
    """
    urls = df['link'].tolist()
    all_results = []

    # Create an iterator for the URLs
    from itertools import islice
    url_iter = iter(urls)
    batch_number = 1

    while True:
        batch = list(islice(url_iter, batch_size))
        if not batch:
            break  # No more URLs

        print(f"Processing Twitter batch {batch_number} with {len(batch)} URLs...")

        try:
            # Prepare input for the X scraper
            data_input = x_input(batch)
            # Run Apify
            X_apify_df = apify_run(APIFY_X_SCRAPER, data_input)
            if isinstance(X_apify_df, pd.DataFrame):
                print(f"Batch {batch_number} returned {X_apify_df.shape[0]} rows.")
                all_results.append(X_apify_df)
            else:
                print(f"Warning: Batch {batch_number} did not return a DataFrame.")

        except Exception as e:
            print(f"Error in batch {batch_number}: {str(e)}")

        batch_number += 1
        print(f"Sleeping for {sleep_time} seconds...\n")
        time.sleep(sleep_time)

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()



def youtube_input(urls):
    usernames = []
    for url in urls:
        usernames.append(url)
    
    data_input = {
        "customMapFunction": "(object) => { return {...object} }",
        "duration": "all",
        "features": "all",
        "getTrending": False,
        "includeShorts": False,
        "maxItems": 1000,
        "sort": "r",
        "startUrls": usernames,
        "uploadDate": "all"
    }
    
    return data_input


#def parse_apify_output_youtube(df):
#    df = df[['url', 'thumbnails']]
#    df['image_link'] = df['thumbnails'].apply(lambda x: x[0]['url'] if isinstance(x, list) and len(x) > 0 else '')
#    
#    return df
    
def parse_apify_output_youtube(df):
    """
    Parses the YouTube DataFrame returned by Apify.
    Handles cases where expected columns might be missing.
    """
    # Check if DataFrame is empty
    if df.empty:
        print("Warning: Received an empty DataFrame from Apify for YouTube.")
        return pd.DataFrame(columns=["url", "image_link"])
    
    # Print available columns for debugging
    print(f"YouTube DataFrame columns: {list(df.columns)}")
    
    # Check for required columns
    if 'url' not in df.columns:
        print("Warning: 'url' column not found in YouTube data.")
        return pd.DataFrame(columns=["url", "image_link"])
    
    if 'thumbnails' not in df.columns:
        print("Warning: 'thumbnails' column not found in YouTube data.")
        # Return DataFrame with URLs but no image links
        result_df = df[['url']].copy()
        result_df['image_link'] = ''
        return result_df
    
    # If both columns exist, proceed with parsing
    df_subset = df[['url', 'thumbnails']].copy()
    df_subset['image_link'] = df_subset['thumbnails'].apply(
        lambda x: x[0]['url'] if isinstance(x, list) and len(x) > 0 else ''
    )
    
    return df_subset[['url', 'image_link']]
    


def safe_concat(dfs_list):
    cleaned_list = []
    for d in dfs_list:
        if not d.empty:
            d = d.reset_index(drop=True)
            d = d.loc[:, ~d.columns.duplicated()]  # remove col duplicates
        cleaned_list.append(d)
    return pd.concat(cleaned_list, ignore_index=True)



def write_dataframe_to_s3(df, file_path):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(S3_BUCKET, file_path).put(Body=csv_buffer.getvalue())
        print(f"Data written to s3://{S3_BUCKET}/{file_path}")
    except Exception as e:
        print(f"Error writing to S3: {e}")

def write_dataframe_to_excel_s3(df, file_path):
    try:
        # Create a buffer
        excel_buffer = BytesIO()
        # Write DataFrame to the buffer using ExcelWriter
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)
        # Seek to the beginning of the stream
        excel_buffer.seek(0)

        # Upload the buffer content to S3
        s3_resource = boto3.resource('s3')
        s3_resource.Object(S3_BUCKET, file_path).put(Body=excel_buffer.getvalue())
        print(f"Data written to Excel file in S3: s3://{S3_BUCKET}/{file_path}")
    except Exception as e:
        print(f"Failed to write DataFrame to Excel and upload to S3: {e}")
    
  
def main():

    print("Reading data file,,,,,,")

    
    df_new_table = fetch_data_from_postgresql(table_name)
    #write_dataframe_to_s3(df_new_table, "sprout_social/staging_master_table/snapshot_master_table.csv")
        # 3.2. Drop duplicates
    df = df_new_table.drop_duplicates(subset=["link"])
    df["dateoflastedit"] = pd.to_datetime(df["dateoflastedit"])
    
    # 3.3. Filter rows that need tagging
    needs_tagging = df[
            ((df["tag"] == "Overall") |
             (df["tag"].isnull()) |
             (df["tag"].str.strip() == "")) &
            ((df["dateoflastedit"] > cutoff_date) | 
             (df["dateoflastedit"].isnull()))
    ]
    
    df['need_tagging'] = 0  # Initialize with 0
    df.loc[needs_tagging.index, 'need_tagging'] = 1 
    
    print("Number of posts needing tagging:", len(needs_tagging))

    # 3.4. Split by network
    df_fb = needs_tagging[needs_tagging["network"] == "facebook"]
    df_x  = needs_tagging[needs_tagging["network"] == "twitter"]
    df_ig = needs_tagging[needs_tagging["network"] == "Instagram"]
    df_yt = needs_tagging[needs_tagging["network"] == "youtube"]
    # -------------------------------------------------------------------------
    # FACEBOOK
    # -------------------------------------------------------------------------
    fb_urls = df_fb['link'].dropna().tolist()

    
    if fb_urls:
        print(f"Scraping Facebook URLs (count={len(fb_urls)})...")
        fb_data_input = facebook_input(fb_urls)
        fb_apify = apify_run(APIFY_FACEBOOK_POST_SCRAPER, fb_data_input)
        fb_image_links = parse_facebook_posts(fb_apify)
        fb_image_links.drop_duplicates(subset="facebookUrl", keep="first", inplace=True)
        fb_image_links.rename(columns={"facebookUrl": "url"}, inplace=True)

    else:
        fb_image_links = pd.DataFrame(columns=["url", "image_link", "ocr_text"])
  
 
    # -------------------------------------------------------------------------
    # INSTAGRAM
    # -------------------------------------------------------------------------
    df_ig['link'] = df_ig['link'].fillna('')
    ig_urls = df_ig['link'].tolist()

    # Exclude 'stories'
    filtered_ig_urls = [url for url in ig_urls if 'stories' not in url]
    #filtered_ig_urls = filtered_ig_urls[0:2]
    
    if filtered_ig_urls:
        print(f"Scraping Instagram URLs (count={len(filtered_ig_urls)})...")
        results_ig = run_apify_actor_ig(filtered_ig_urls, batch_size=100)
        # Clean up the resulting DF
        results_ig['input_url'] = results_ig['input_url'].fillna(results_ig['url'])
        results_ig.rename(columns={'input_url': 'url', 'displayUrl': 'image_link'}, inplace=True)
        ig_image_links = results_ig[['url','image_link']]

    else:
        ig_image_links = pd.DataFrame(columns=["url","image_link"])
        
    # -------------------------------------------------------------------------
    # TWITTER (X)
    # -------------------------------------------------------------------------
    if len(df_x) > 0:
        df_x = df_x.head(10)
        print(f"Scraping X (Twitter) URLs (count={len(df_x)})...")
        X_apify = process_urls_in_batches(df_x, batch_size=300, sleep_time=10)
        X_image_links = parse_apify_output_x(X_apify)
        X_image_links.rename(columns={"twitterUrl": "url"}, inplace=True)
        X_image_links = X_image_links[['url','image_link']]
        print("X_image_links=", X_image_links)
    else:
        X_image_links = pd.DataFrame(columns=["url","image_link"])


    # -------------------------------------------------------------------------
    # YOUTUBE

    # -------------------------------------------------------------------------
    yt_urls = df_yt['link'].dropna().tolist()

    if yt_urls:
        print(f"Scraping YouTube URLs (count={len(yt_urls)})...")
        yt_data_input = youtube_input(yt_urls)
        yt_apify = apify_run(APIFY_YOUTUBE_SCRAPER, yt_data_input)
        
        # Add debugging info
        print(f"YouTube result shape: {yt_apify.shape}")
        print(f"YouTube columns: {list(yt_apify.columns)}")
        if not yt_apify.empty:
            print(f"First row sample: {yt_apify.iloc[0].to_dict()}")
        
        yt_image_links = parse_apify_output_youtube(yt_apify)
        yt_image_links = yt_image_links[['url','image_link']]
        print(yt_image_links)
    else:
        yt_image_links = pd.DataFrame(columns=["url","image_link"])

    # -------------------------------------------------------------------------
    # Combine all
    # -------------------------------------------------------------------------
    combined_image_links = safe_concat(
        [fb_image_links, X_image_links, ig_image_links, yt_image_links]
    )
    print("Combined result shape:", combined_image_links.shape)
    
    right_df_cleaned = combined_image_links.drop_duplicates(subset=['url'])
    right_df_cleaned = right_df_cleaned.dropna(subset=['url'])
    right_df_cleaned.reset_index(drop=True, inplace=True)
    print("shape of =",df_new_table.shape)
    
    merged_df = df_new_table.merge(
    right_df_cleaned[['url', 'image_link']],
    left_on='link',
    right_on='url',
    how='left'
    ) 

    
    merged_df = merged_df.merge(
    df[['post_id', 'need_tagging']], 
    on='post_id',
    how='left'
    )
    merged_df['need_tagging'] = merged_df['need_tagging'].fillna(0)
    # Write to Excel in S3
    write_dataframe_to_excel_s3(merged_df, "sprout_social/tag_processing/with_image_url/combined_image_links.xlsx")

       
if __name__ == "__main__":
    main()
   
