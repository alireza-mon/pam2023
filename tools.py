import numpy as np 


import json
import time 

import random 
import os
import json
import urllib
import pickle as pk
import sys
import requests
from urllib.parse import quote
import pandas as pd
from os.path import join as path_join
import re
from tqdm import tqdm
import scipy.stats as stats
import statistics
from datetime import datetime
import urllib
import hashlib


def pickelize(obj,file):
    with open(file,"wb") as f:
        pk.dump(obj,f)

def un_pickelize(file,dir=None):
    if dir == None:
        with open(file,"rb") as f:
            return(pk.load(f))
    else:
        return un_pickelize(path_join(dir,f"{file}.pkl"))
    
    
def get_all_posts_of_article(url):
    DATA_DIR = './data'
    posts_data_dir = path_join(DATA_DIR, "crowdtangle")
    url_hash = hashlib.sha256(url.encode()).hexdigest()

    posts_data = un_pickelize(url_hash, dir=posts_data_dir)
    posts = posts_data[1]

    interaction_types = ['likeCount', 'shareCount', 'commentCount', 'loveCount', 'wowCount', 'hahaCount', 'sadCount', 'angryCount', 'thankfulCount', 'careCount']
    interaction_weights = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

    def process_post(post):
        post_date = datetime.strptime(post["date"], '%Y-%m-%d %H:%M:%S')

        history = {
            key: [] for key in interaction_types + ["date", "timestep", "all_interactions"]
        }
        max_diff = 0

        for history_point in post["history"]:
            history_point_date = datetime.strptime(history_point["date"], '%Y-%m-%d %H:%M:%S')
            if history_point_date < post_date:
                continue

            difference = history_point_date - post_date
            if difference.days > 21:
                continue

            max_diff = max(max_diff, difference.days)

            for interaction_type in interaction_types:
                history[interaction_type].append(history_point["actual"][interaction_type])

            history["all_interactions"].append(sum([history_point["actual"][interaction_type] * interaction_weights[i] for i, interaction_type in enumerate(interaction_types)]))
            history["date"].append(history_point["date"])
            history["timestep"].append(history_point["timestep"])

        if max_diff < 6 or len(history["date"]) == 0:
            return None

        for key in history:
            history[key] = np.flip(np.array(history[key]))

        return {
            "platformId": post["platformId"],
            "date": post["date"],
            "type": post["type"],
            "history": history,
        }

    return [processed_post for post in posts if (processed_post := process_post(post)) is not None]



def update_posts_time_series_to_seconds(posts):
    for post in posts:
        post_publication_date = datetime.strptime(post["date"], '%Y-%m-%d %H:%M:%S')
        post["history"]["date"] = np.array([
            int((datetime.strptime(date, "%Y-%m-%d %H:%M:%S") - post_publication_date).total_seconds())
            for date in post["history"]["date"]
        ])