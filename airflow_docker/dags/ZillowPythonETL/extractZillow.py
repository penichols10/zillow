import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import json
from time import sleep

def scrape_page(req):
    
    '''
    Scrapes a single page of zillow search results, returning a dataframe containing the data
    '''
    
    soup = bs(req.content)
    data = soup.find('script', {'id':'__NEXT_DATA__'})
    data = json.loads(data.text)
    data = data['props']['pageProps']['searchPageState']['cat1']['searchResults']['listResults'] # This is one page of results
    
    df = pd.json_normalize(data)

    return df

def scrape_results():
    '''
    Scrapes multiple pages of Zillow search results, returning a dataframe containing the data
    '''
    dfs = []
    headers={
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-US;en;q=0.9",
        "accept-encoding": "gzip, deflate, br",
    }
    page_num = 1
    url = f'https://www.zillow.com/montgomery-county-md/{page_num}_p/'
    req = requests.get(url, headers=headers)
    
    
    # Messy code - clean it up
    while req.status_code == 200:
        df = scrape_page(req)
        dfs.append(df)
        sleep(10)
        page_num += 1
        
        url = f'https://www.zillow.com/montgomery-county-md/{page_num}_p/'
        req = requests.get(url, headers=headers, allow_redirects=False)
        

    
    dfs = pd.concat(dfs, axis=0)
    dfs = dfs.drop_duplicates(subset='zpid')
    dfs = dfs.reset_index(drop=True)
    
    # Save the dfs
    dfs.to_csv('C:\\Users\\Patrick\\Documents\\python\\zillow\\data\\zillow_raw.csv', index=False)
    
    return dfs

