# Unissu Scraper
The project scrapes the details of PropTech companies listed in the Unissu(.com).

### About project

  - Thread and proxy management (100 proxies)
  - Used `requests` HTTP library
  - `json` response parsing

### Usage

The code uses 100 proxies. Create a Proxies.csv, copy and paste your proxies 
in the form:

`http://username:password@proxy.yourorg.com:80`

Put your proxies into Proxies.csv file, also make changes in the code.

```python
    # "http://username:password@proxy.yourorg.com:80"
    proxies = {
        "http": f"http://username:password@{appointed_proxy}",
        "https": f"http://username:password@{appointed_proxy}"
    }
```

Also, you need to change `max_num_worker_threads` to number of proxies
you put in the `Proxies.csv` file.


The code writes data to csv file, to get the xls file I imported
the csv file into the MS Office Excel and saved as xls file.

`Data -> From Text/CSV -> Choose comma -> Save as .xls`


### Installation


Create your environment and run these commands:

```sh
$ .\venv\Scripts\activate
$ pip install requirements
$ python main.py
```


### Development

If you want to contribute to the project don't forget to add docstrings 
and use `pip freeze > requirements.txt` 


### License
This repository is licensed under the MIT License. Please see the
 [LICENSE](https://github.com/nurettinabaci/UnissuScraper/blob/master/LICENSE) 
 file for more details.