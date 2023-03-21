import os
import time
import hashlib
from urllib.parse import urljoin

import requests
import pandas as pd
from tqdm import tqdm
import pickle as pk
from os.path import join as path_join

# Define the directory where the data will be saved
DATA_DIR = './data'

# Define whether to use pagination technique when querying the API
USE_PAGINATION = False

API_KEY = os.environ.get('CT_API_KEY')

'''
Please include the URLs you would like to analyze in the articles.csv file, preferably using the canonical form of the URLs.
The required columns for the analysis are "canonical_url", "bias", "reliability", and "source" (if conducting an outlet-specific analysis).
Otherwise, just the first three are enough. A sample file is provided for reference.
To access the proprietary rankings of the article used in the paper, kindly visit Ad Fontes Media's website for access and licensing information.
'''


def pickle_data(obj, file_path):
    """Save an object to a file using pickle."""
    with open(file_path+".pkl", "wb") as f:
        pk.dump(obj, f)



def get_url_posts(canonical_url):
    """Get CrowdTangle posts for a given URL."""
    def get_one_batch_of_url_posts(canonical_url, start_date="2018-01-01", 
                                   end_date="2022-09-01"):
        link_ep = "https://api.crowdtangle.com/links"
        query_params = {
            "token": API_KEY,
            "link": canonical_url,
            "startDate": start_date,
            "endDate": end_date,
            "includeHistory": "true",
            "searchField": "Include_query_strings",
            "count": "1000",
            "sortBy": "date",
            "platforms": "facebook",
        }
        response = requests.get(link_ep, params=query_params)
        response.raise_for_status()
        response_data = response.json()
        posts_data = response_data["result"]["posts"]
        pagination_status = response_data["result"]["pagination"]
        return posts_data, pagination_status

    posts_data = []
    end_date = "2022-09-01"
    try:
        if USE_PAGINATION:
                next_batch_posts_data, pagination_status = get_one_batch_of_url_posts(
                    canonical_url, end_date=end_date
                )
                time.sleep(30)
                posts_data += next_batch_posts_data
                while pagination_status != {}:
                    next_page = pagination_status["nextPage"]
                    response = requests.get(next_page)
                    time.sleep(30)
                    response.raise_for_status()
                    response_data = response.json()
                    next_batch_posts_data = response_data["result"]["posts"]
                    pagination_status = response_data["result"]["pagination"]
                    posts_data += next_batch_posts_data
                
        else:
            while True:
                next_batch_posts_data, pagination_status = get_one_batch_of_url_posts(
                    canonical_url, end_date=end_date
                )
                time.sleep(30)
                posts_data += next_batch_posts_data
                if len(next_batch_posts_data) < 990: # or pagination_status == {}:
                    break
                last_post_date, last_post_time = next_batch_posts_data[-1]["date"].split(" ")
                end_date = last_post_date + 'T' + last_post_time
        return posts_data
    except requests.exceptions.RequestException as e:
        print(f"Error with getting CT data for URL: {canonical_url}\n{e}")
        return None
        


if __name__ == "__main__":
    articles_df = pd.read_csv(path_join(DATA_DIR, 'articles.csv'))
    canonical_urls = list(articles_df['canonical_url'])
    posts_data_dir = path_join(DATA_DIR, "crowdtangle")
    for url in tqdm(canonical_urls):
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        if not os.path.exists(path_join(posts_data_dir, f"{url_hash}.pkl")):
            posts_data = get_url_posts(url)
            if posts_data is not None:
                pickle_data((url, posts_data), path_join(posts_data_dir, url_hash))
