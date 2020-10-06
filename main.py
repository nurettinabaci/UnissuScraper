#!/usr/bin/python
# -*- coding: utf-8 -*-
from math import floor
import csv
import json
import threading
import queue
from requests.exceptions import ProxyError
from requests.adapters import Response
from r_funcs import get_page, get_proxy

# # logging
# import logging
# logging.basicConfig(filename='log_out.txt', level=logging.DEBUG, filemode='w')
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True


csv_writer_lock = threading.Lock()
OFFSET_COUNT = 100
max_num_worker_threads = 99
company_slugs_list = []


def write_table_headers():
    '''Writes the column headers to file'''
    headers = ["ID", "COMPANY_NAME", "COMPANY_LOGO",
               "SLOGAN", "DESCRIPTION", "LOCATION",
               "HQ_COUNTRY", "FOUNDED", "OPERATING_STATUS",
               "COMPANY_TYPE", "IPO_STATUS", "RE_STATUS",
               "CATEGORIES", "BUSINESS_MODEL", "WEBSITE_URL",
               "LINKEDIN_URL", "FACEBOOK_URL", "TWITTER_URL",
               "STARTUP_IMAGE_1", "RELATED_COMPANIES", "CURRENT_TEAM",
               "SOURCE"]

    with open('output.csv', mode='a+', newline='', encoding='utf-32') as links_file:
        link_writer = csv.writer(links_file, quotechar='"')
        link_writer.writerow(headers)


write_table_headers()


def get_slug_of_company(companies: json):
    for company in companies:
        slug = company['slug']
        company_slugs_list.append(slug)


def do_work(endpoint, appointed_proxy):
    '''Sends request to endpoint and '''

    # "http://username:password@proxy.yourorg.com:80"
    proxies = {
        "http": f"http://username:password@{appointed_proxy}",
        "https": f"http://username:password@{appointed_proxy}"
    }

    r = get_page(page_link=endpoint, proxies=proxies, parameters={})
    try:
        json_resp = r.json()
    except json.decoder.JSONDecodeError as json_error:
        with open('links_error_log.csv', mode='a+', newline='', ) as links_file:
            link_writer = csv.writer(links_file, delimiter=',', quotechar='"')
            link_writer.writerow([endpoint])
        json_resp = {}
        print("Error:", json_error, '/', r.status_code, '/', str(proxy))

    companies = json_resp['results']
    get_slug_of_company(companies)


def worker():
    while True:
        item = q.get()
        if item is None:
            break
        ep = item[0]
        proxy = item[1]
        try:
            do_work(ep, proxy)
        except ProxyError:
            raise ProxyError("Proxy related problem")
        except Exception:
            raise Exception("Can't scrape for link: ", ep, proxy)
        q.task_done()


# Getting API constants
constants = get_page("https://api.unissu.com/constants/", parameters={})
constants = constants.json().get('constants')
PRODUCT_STAGE_CHOICES = constants.get('vendors').get('PRODUCT_STAGE_CHOICES')
BUSINESS_TYPE_CHOICES = constants.get('vendors').get('BUSINESS_TYPE_CHOICES')

# To get total number of companies make a requests with limit=1 value, without overheading the database
first_response: Response = get_page("https://api.unissu.com/vendors/list/?limit=1&offset=0", parameters={})
number_of_companies: int = first_response.json().get('count', 0)
data_range = floor(number_of_companies / 100)

# Create threads
q = queue.Queue(maxsize=max_num_worker_threads)
threads = []
for i in range(max_num_worker_threads):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)

# Iterate in the range of total number of companies and scrape
print("Scraping company slugs - first part")
for i in range(data_range + 1):
    offset = i * OFFSET_COUNT
    ep = f"https://api.unissu.com/vendors/list/?limit=100&offset={offset}"
    proxy = get_proxy(i % 100)
    q.put((ep, proxy))
q.join()

print("Stopping threads - first part is done")
for i in range(max_num_worker_threads):
    q.put(None)
for t in threads:
    t.join()


def get_by_path(var: json, args, default=None):
    value = var
    for arg in args:
        if isinstance(arg, str):
            value = value.get(arg, default)
            if value is default or not value:
                return default

        elif isinstance(arg, int):
            try:
                value = value[arg]
            except KeyError:
                return default
            except IndexError:
                return default
        else:
            return default
    return value


def get_company_details(company: json, link=None, proxy=None):
    '''Extracts the company data from JSON response'''

    id = company.get('id', None)
    company_name = company.get('name', None)
    company_logo = company.get('logo', None)
    slogan = company.get('headline', None)
    description = company.get('description', None)

    if locations := get_by_path(company, ('products', 0, 'available_countries'), default=None):
        locations = '; '.join([location['name'].strip() for location in locations if locations])

    hq_country = get_by_path(company, ('operating_markets', 0, 'name'), default=None)
    year_founded = company.get('year_founded', None)

    if operating_status := get_by_path(company, ('products', 0, 'product_stage'), default=None):
        operating_status = list(map(lambda d: d.get('label', None), PRODUCT_STAGE_CHOICES))[operating_status - 1]

    company_type = get_by_path(company, ('industry', 'label'), default=None)
    ipo_status = get_by_path(company, ('ownership', 'label'), default=None)
    re_sector = get_by_path(company, ('products', 0, 'sectors', 0, 'name'), default='-')

    if categories := get_by_path(company, ('products', 0, 'label'), default=None):
        categories = '; '.join([category['name'].strip() for category in categories])

    if business_model := get_by_path(company, ('products', 0, 'business_type'), default=None):
        business_model = '; '.join(list(map(lambda d: d.get('label', None),
                                            BUSINESS_TYPE_CHOICES))[business_model - 1].split(' and '))

    website_url = company.get('website', None)
    linkedin = company.get('linkedin', None)
    facebook = company.get('facebook', None)
    twitter = company.get('twitter', None)
    startup_image = get_by_path(company, ('images', 0, 'image'), default=None)

    # related_companies
    similar_vendors_ep = f"https://api.unissu.com/vendors/{company['slug']}/similar-vendors/"
    similar_vendors_r = get_page(page_link=similar_vendors_ep, parameters={}, proxies=proxy)
    try:
        similar_vendors = similar_vendors_r.json()
    except json.decoder.JSONDecodeError as json_error:
        similar_vendors = {}
        print("Error:", json_error, '/', similar_vendors_r.status_code, '/', str(proxy))

    if similar_vendors := similar_vendors.get('results', None):
        similar_vendors = '; '.join([vendor['name'].strip() for vendor in similar_vendors])

    # current_team
    current_team_ep = f"https://api.unissu.com/users/users/list/?vendor={id}"
    current_team_r = get_page(page_link=current_team_ep, parameters={})
    try:
        current_team = current_team_r.json()
    except json.decoder.JSONDecodeError as json_error:
        current_team = {}
        print("Error:", json_error, '/', current_team_r.status_code, '/', str(proxy))

    team, team_hist = [], []
    if current_team := current_team.get('results', None):
        for team_member in current_team:
            first_name = team_member['profile']['first_name'].strip()
            last_name = team_member['profile']['surname'].strip()
            check_dict = {first_name: last_name}
            if check_dict not in team_hist:
                team_hist.append({first_name: last_name})
            else:
                continue

            if team_member['profile']['position'] is None:
                team.append(' '.join([first_name, last_name]))
            else:
                position = team_member['profile']['position']['name']
                team.append(' '.join([first_name, last_name, f"({position})"]))

    team = '; '.join([member for member in team])

    source = link

    row = [id, company_name, company_logo, slogan, description, locations,
           hq_country, year_founded, operating_status, company_type, ipo_status,
           re_sector, categories, business_model, website_url, linkedin, facebook,
           twitter, startup_image, similar_vendors, team, source]

    for idx, elem in enumerate(row):
        if elem == '' or elem == "":
            row[idx] = None
    with csv_writer_lock:
        with open('output.csv', mode='a+', newline='', encoding='utf-32') as company_file:
            company_writer = csv.writer(company_file, quotechar='"')
            company_writer.writerow(row)


def do_work_company(endpoint, appointed_proxy):
    '''Sends requests with given proxy and extracts details from the JSON response'''

    # "http://username:password@proxy.yourorg.com:80"
    proxies = {
        "http": f"http://username:password@{appointed_proxy}",
        "https": f"http://username:password@{appointed_proxy}"
    }

    r = get_page(page_link=endpoint, proxies=proxies, parameters={})
    try:
        company = r.json()
    except json.decoder.JSONDecodeError as json_error:
        with open('links_error_log.csv', mode='a+', newline='', ) as links_file:
            link_writer = csv.writer(links_file, delimiter=',', quotechar='"')
            link_writer.writerow([endpoint])
        company = {}
        print("Error:", json_error, '/', r.status_code, '/', str(appointed_proxy))

    get_company_details(company, link=endpoint, proxy=proxies)


def worker_company():
    '''Worker function to scrape details of each company'''
    while True:
        item = q.get()
        if item is None:
            break
        ep = item[0]
        proxy = item[1]
        try:
            do_work_company(ep, proxy)
        except ProxyError:
            raise ProxyError("Proxy related problem")
        except Exception:
            raise Exception("Can't scrape for link: ", ep, proxy)
        q.task_done()


threads = []
q = queue.Queue(maxsize=max_num_worker_threads)
for i in range(max_num_worker_threads):
    t = threading.Thread(target=worker_company)
    t.start()
    threads.append(t)

print("Scraping company details - second part")

# Iterate over thousands of slugs batch by batch and scrape
batch_size = max_num_worker_threads
length = len(company_slugs_list)
for i in range(0, length, batch_size):
    companies_batch = company_slugs_list[i:i + batch_size]
    for idx, elem in enumerate(companies_batch):
        company_slug = elem
        proxy = get_proxy(idx)
        single_company_endpoint = f"https://api.unissu.com/vendors/{company_slug}/retrieve/"
        q.put((single_company_endpoint, proxy))
    q.join()

print("Stopping threads - second part is finished")
for i in range(max_num_worker_threads):
    q.put(None)
for t in threads:
    t.join()
