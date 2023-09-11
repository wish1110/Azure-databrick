import pandas as pd 
import json
from datetime import datetime
import s3fs
import requests
from bs4 import BeautifulSoup
import re

def run_webscrap_etl():
    url = 'https://www.supplychaindive.com'
    response = requests.get(url, verify = False)

    soup = BeautifulSoup(response.text, 'html.parser')


    text_list=[]
    link_list = []

    story1 = soup.find('a', {"class":"analytics t-dash-feed-item-1"})
    story2 = soup.find('a', {"class":"analytics t-dash-feed-item-2"})
    story3 = soup.find('a', {"class":"analytics t-dash-feed-item-3"})
    story4 = soup.find('a', {"class":"analytics t-dash-feed-item-4"})
    story5 = soup.find('a', {"class":"analytics t-dash-feed-item-5"})

    def get_text(story):
        return text_list.append(story.text.strip())

    def get_link(story):
        return link_list.append(story['href'])

    get_text(story1)
    get_text(story2)
    get_text(story3)
    get_text(story4)
    get_text(story5)

    get_link(story1)
    get_link(story2)
    get_link(story3)
    get_link(story4)
    get_link(story5)


    dict ={'article_name':text_list,'article_link':link_list}

    df = pd.DataFrame(data = dict)
    df.to_csv("webscrap.csv")