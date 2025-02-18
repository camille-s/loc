#!/usr/bin/env python
import requests
import pandas as pd
from pathlib import Path
import json
from benedict import benedict
import multiprocessing
from tqdm.contrib.concurrent import process_map
import argparse
import re
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from urllib.parse import urldefrag
# search by text, filter for type = 'collection'.
# if no results, quit
 
num_cpu = multiprocessing.cpu_count() - 1
 
def to_snake_case(x: str) -> str:
    '''
    Convert text to snake case.  

    Args:
        x (str): String to convert

    Returns:
        str: String with spaces and non-word characters replaced with underscores, and all characters in lower case.
    '''
    return re.sub(r'(\s|[^\w])+', '_', x.lower())

def search(query: str) -> str:
    base_url = 'https://www.loc.gov/search'
    prms = {
        'fo': 'json',
        'q': query,
        'fa': 'type:collection',
        'at': 'results',
    }
    resp = requests.get(base_url, params = prms)
    resp.raise_for_status()
    
    resp_json = resp.json()['results'][0]
    json_bene = benedict(resp_json)
    print('Top result:', json_bene['title'])
    return Path(str(json_bene['items'])).stem


def query_collection(title: str, limit: int) -> benedict:
    base_url = f'https://www.loc.gov/collections/{title}'
    prms = {
        'fo': 'json',
        'fa': 'digitized:true',
        'c': limit,
        'at': 'results,pages'
    }
    resp = requests.get(base_url, params = prms)
    resp.raise_for_status()
    
    resp_json = resp.json()
    json_bene = benedict(resp_json)
    print('Number of results:', len(json_bene['results']))
    
    return json_bene

def read_essay(url: str) -> dict:
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    essay = soup.article
    # convert to md 
    essay = md(str(essay))
    title = Path(url).stem
    return { 'title': title, 'text': essay }

def write_essay(title: str, text: str, base_dir: str | Path) -> Path:
    fn = f'{title}.md'
    essay_dir = Path(base_dir) / 'essays'
    essay_dir.mkdir(parents=True, exist_ok=True)
    path = essay_dir / fn
    with open(path, 'w') as f:
        f.write(text)
    return path

def fetch_essays(res: dict | benedict, base_dir: Path | str) -> list[str]:
    pages = pd.DataFrame([r.subset(['title', 'children']) for r in res['pages']])
    pages = pages.explode('children')
    pages['url'] = pages['children'].apply(lambda x: x['link']).astype('str')
    urls = pages.loc[pages['title'] == 'Articles and Essays']['url'].to_list()
    # essays = [read_essay(u) for u in urls]
    for u in urls:
        essay = read_essay(u) # title, text
        write_essay(**essay, base_dir = base_dir)
    return urls
        


def prep_dirs(title: str) -> Path:
    '''
    Create directories for each collection

    Args:
        title (str): Title as returned from search()

    Returns:
        Path: Path to collection directory
    '''
    title = to_snake_case(title)
    direc_path = Path(title)
    direc_path.mkdir(parents=True, exist_ok=True)
    
    (direc_path / 'essays').mkdir(parents=True, exist_ok=True)
    (direc_path / 'items').mkdir(parents=True, exist_ok=True)
    
    return direc_path


def write_meta(meta: dict | benedict, base_dir: str | Path) -> Path:
    '''
    Write metadata to json as returned by search()

    Args:
        output_dir (str | Path): Path to collection directory
        meta (dict | benedict): Metadata

    Returns:
        Path: Path to json file
    '''
    meta_path = Path(base_dir) / 'search_results.json'
    with open(meta_path, 'w') as f:
        json.dump(meta, f)
    return meta_path

def extract_img_size(url: str, dim: str) -> int:
    if dim not in ['h', 'w']:
        raise ValueError('dim must be h or w')
    match = re.search(rf'(?<={dim}=)([0-9]+)', url)
    if match:
        return int(match.group(1))
    else:
        return 0
    
def extract_id(record: benedict) -> str | None:
    url = record['url']
    name = Path(str(url)).name
    id = re.match('(?:.*?)([0-9]+[a-z]?$)', name)
    if id:
        return name
    else:
        return None


def prep_img(record: benedict, base_dir: str | Path) -> benedict | None:
    # skip if a page, not an item with ID
    # hopefully this works for other collections
    id = extract_id(record)
    if id:
        imgs = pd.DataFrame({ 'url': record['image_url'] })
        imgs['h'] = imgs['url'].apply(lambda x: extract_img_size(x, 'h'))
        imgs['w'] = imgs['url'].apply(lambda x: extract_img_size(x, 'w'))
        imgs['bare_url'] = imgs['url'].apply(lambda x: urldefrag(x)[0])
        
        img = imgs.sort_values('h', ascending=False).iloc[0, :].to_dict()
        img = benedict(img)
        ext = Path(str(img['bare_url'])).suffix
        fn = f'{id}{ext}'
        
        img['path'] = Path(base_dir) / 'items' / fn
        
        return img.subset(['url', 'path'])
    else:
        return None
    
    
def prep_audio(record: benedict, base_dir: str | Path) -> benedict | None:
    id = extract_id(record)
    if id:
        url = record[['resources', 0, 'audio']]
        fn = Path(str(url)).name
        path = Path(base_dir) / 'items' / fn
        return benedict({'url': url, 'path': path})
    else:
        return None
        
    
def download_file(url: str, path: str | Path) -> Path | None:
    if not isinstance(path, Path):
        path = Path(path)
    if not path.exists():
        resp = requests.get(url)
        if resp.status_code == requests.codes.ok:
            with open(path, 'wb') as f:
                f.write(resp.content)
                return path
        else:
            return None
    

def prep_records(results: list[benedict], 
                 base_dir: str | Path, 
                 media_type: str) -> pd.DataFrame: 
    if media_type == 'image':
        records = [prep_img(r, base_dir) for r in results]
    elif media_type == 'audio':
        records = [prep_audio(r, base_dir) for r in results]
    else:
        raise ValueError('bad value for media_type')
    records = [r for r in records if r is not None]
    return pd.DataFrame(records)
    # imgs = [download_img(str(r['url']), str(r['path'])) for r in records]
    # return imgs
    
    

def get_args() -> tuple[str, int, str, bool]:
    '''
    Process command line arguments.  

    Returns:
        tuple[str, int, bool]: Returns a tuple of the collection search text, limit for records query, and boolean indicating whether this is a dry run with no downloads.
    '''
    prsr = argparse.ArgumentParser(
        prog='LOC scraper',
        description='Download digitized LOC records based on collection title')
    prsr.add_argument('-s',
                      '--search',
                      type=str,
                      required=True,
                      help='Title search text')
    prsr.add_argument('-l',
                      '--limit',
                      type=int,
                      help='Limit: total number of records to fetch (default: %(default)s)',
                      default=100)
    prsr.add_argument('-t',
                      '--media_type',
                      type = str,
                      help = 'Media type to filter for (default: %(default)s)',
                      choices = ['image', 'audio'],
                      default = 'image')
    prsr.add_argument(
        '-n',
        '--no_download',
        help='Don\'t download files, just make request & write json',
        action='store_true')

    args = prsr.parse_args()
    return (args.search, args.limit, args.media_type, args.no_download)

def main() -> None:
    '''
    Main function body
    '''
    # get search term, limit from args
    # search to get kebab case title
    search_txt, limit, media_type, no_download = get_args()
    title = search(search_txt)
    # make collection directory
    base_path = prep_dirs(title)
    
    # query by resolved title
    collection = query_collection(title, limit = limit)
    
    # write results metadata
    meta_path = write_meta(collection, base_dir = base_path)
    
    # stop here if dry-run
    if no_download:
        print(f'Skipping download; see {meta_path} for results\n')
        return
    
    # fetch & write essays
    essay_urls = fetch_essays(collection, base_dir = base_path)
    
    # get all records & download
    results = [benedict(r) for r in collection['results']]
    records_df = prep_records(results, base_dir = base_path, media_type = media_type)
    process_map(download_file, 
                records_df['url'],
                records_df['path'],
                max_workers=num_cpu,
                chunksize=10)
    return None

if __name__ == '__main__':
    main()