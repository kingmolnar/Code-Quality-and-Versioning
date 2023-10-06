import os
import sys
import re
import datetime
import time
import pandas as pd
import numpy as np
# if not '/usr/local/lib/python3.7/site-packages' in sys.path:
#     sys.path.insert(0, '/usr/local/lib/python3.7/site-packages')
    
from bs4 import BeautifulSoup

import xml

def metadata(soup):
    import json
    sec_hdr = {}
    prev_level = 0
    field_tree = {} 

    def _insert(field_tree, value, level):
        target = sec_hdr
        for j in range(level):
            target = sec_hdr.get(field_tree[j])
            if target is None:
                sec_hdr[field_tree[j]] = {}
                target = sec_hdr[field_tree[j]]

        target[field_tree[level]] = value


    for j, lin in enumerate(
                    soup \
                        .find('sec-header') \
                        .find('acceptance-datetime') \
                        .text.split('\n')
                    ):
        m = pat.match(lin)
        if m:
            field = m.group(2).replace(' ', '_').lower()
            value = m.group(4).strip()
            this_level = len(m.group(1))
            field_tree[this_level] = field
            if len(value)>0:
                # print(f"{field_tree}\t{value}\t{this_level}")
                _insert(field_tree, value, this_level)
            prev_level = this_level
            
    return json.dumps(sec_hdr)


def text_document(soup, max_blank_lines=1):
    body = soup.find('text').body
    for tag in body.find_all(lambda t: t.name in ['div', 'tr', 'p', 'br', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        tag.insert(0, '\n')

    new_lines = []
    blank_line_cntr = 0
    for line in body.text.split('\n'):
        if len(line.strip())==0:
            blank_line_cntr += 1
        else:
            blank_line_cntr = 0

        if blank_line_cntr <= max_blank_lines:
            new_lines.append(line)

    sec_txt = '\n'.join(new_lines)
    return sec_txt


def main():
    import boto3
    from bs4 import BeautifulSoup
    
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='molnarp-demo-knowledge-us-east-2',
                        Key='data/raw/sec/2017/QTR4/100148-1574085-8-K.html')
    txt = obj['Body'].read().decode('utf-8')
    print(f"Length of text {len(txt):,}")
    
    soup = BeautifulSoup(txt)
    
    sec_metadata = metadata(soup)
    sec_text = text_document(soup)
    
    
    