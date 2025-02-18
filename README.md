# LOC crawler

Use the Library of Congress's API to search for a collection and download all articles, images, and audio, plus metadata.

```bash
python crawler.py --help
usage: LOC scraper [-h] -s SEARCH [-l LIMIT] [-t {image,audio}] [-n]

Download digitized LOC records based on collection title

options:
  -h, --help            show this help message and exit
  -s, --search SEARCH   Title search text
  -l, --limit LIMIT     Limit: total number of records to fetch (default: 100)
  -t, --media_type {image,audio}
                        Media type to filter for (default: image)
  -n, --no_download     Don't download files, just make request & write json
```
