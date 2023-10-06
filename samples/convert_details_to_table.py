# ██████╗ ███████╗████████╗ █████╗ ██╗██╗     ███████╗    ████████╗ █████╗ ██████╗ ██╗     ███████╗    
# ██╔══██╗██╔════╝╚══██╔══╝██╔══██╗██║██║     ██╔════╝    ╚══██╔══╝██╔══██╗██╔══██╗██║     ██╔════╝    
# ██║  ██║█████╗     ██║   ███████║██║██║     ███████╗       ██║   ███████║██████╔╝██║     █████╗      
# ██║  ██║██╔══╝     ██║   ██╔══██║██║██║     ╚════██║       ██║   ██╔══██║██╔══██╗██║     ██╔══╝      
# ██████╔╝███████╗   ██║   ██║  ██║██║███████╗███████║       ██║   ██║  ██║██████╔╝███████╗███████╗    
# ╚═════╝ ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝╚══════╝╚══════╝       ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚══════╝╚══════╝    
                                                                                                     
from typing import List, Any, Dict, Iterator, Tuple
import os
import sys
print(sys.version)
import time
import random
import json
import re
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import datetime
import pandas as pd

from dateparser.search import search_dates
from dateparser import parse as parse_dates

SEARCH_URl = "https://publicdatadigger.com/search"

INPUT_DATA_DIR = "/data/project/voter_registration_scraping/input"

SCRAPING_SEARCH_DIR = "/data/project/voter_registration_scraping/output/search"
SCRAPING_DETAILS_DIR = "/data/project/voter_registration_scraping/output/details"

SEARCH_RESULTS_CSV = f"/data/project/voter_registration_scraping/data/search_results.csv"

INPUT_DATA_CSV = "/data/project/voter_registration_scraping/input/final_race.csv"


def main():
    T_0 = datetime.datetime.now()

    detail_json_files = os.popen(f'find {SCRAPING_DETAILS_DIR}/json/FL -name "*.json"').readlines()
    print(f"Number of detail files: {len(detail_json_files):,}")

    df_list = []
    for i, fn in enumerate(detail_json_files):
        if i % 1000 == 0:
            print(".", end='')
        detail = json.load(open(fn.strip(), 'r'))
        voter_registrations = []
        for j, vr in enumerate(detail['voter_registrations']):
            rec = {}
            for k in vr.keys():
                if not isinstance(vr[k], dict):
                    rec[k] = vr[k]
                else:
                    for k1 in vr[k].keys():
                        rec[f"{k}_{k1}"] = vr[k][k1]

            voter_registrations.append(rec)

        voter_registrations_df = pd.DataFrame.from_dict(voter_registrations)
        
        profile_df = pd.DataFrame.from_dict([detail['profile']])
        for c in ['detail_url', 'json_file', 'scrape_date']:
            profile_df[c] = detail[c]
            
        profile_df['dummy'] = 1
        voter_registrations_df['dummy'] = 1
        df = pd.merge(profile_df, voter_registrations_df, on='dummy').drop('dummy', axis=1)
        df_list.append(df)

    print("\n")
    detail_df = pd.concat(df_list)
    print(f"Number of detail records: {detail_df.shape[0]:,}")
    print(f"Elapsed time: {datetime.datetime.now()-T_0}")
    detail_df.to_csv(f"/data/project/voter_registration_scraping/data/detail_results.csv", index=None)


if __name__ == '__name__':
    main()
    print("Done.")
