import csv
import datetime
import json
from random import randint
import sys
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re


def dict_to_csv(user_object: dict, csv_file_name: str):

    # append each value to array
    for k, v in user_object.items():
        user_object[k] = [v]

    coin_spec_df = pd.DataFrame.from_dict(user_object)

    try:
        result_df = pd.read_csv(csv_file_name, encoding='utf-8', low_memory=False)
        result = pd.concat([result_df, coin_spec_df], ignore_index=True, sort=False)
        result.to_csv(csv_file_name, index=False, mode='w', header=True, encoding='utf-8')
    except FileNotFoundError:
        coin_spec_df.to_csv(csv_file_name, index=False, mode='w', header=True, encoding='utf-8')

def list_to_csv(user_input: list, csv_file_name: str):
    csv_file = open(csv_file_name, "a")

    for i in user_input:
        i = i + "\n"
        csv_file.writelines(i)

    csv_file.close()

def read_from_csv(csv_file: str) -> list[list]:
    result = []

    file = open(csv_file, 'r', newline='')

    with file:
        red_file = csv.reader(file)
        [result.append(x) for x in red_file]

    return result


class UrlToSoup:
    def __init__(self):
        """get_soup method :)"""
        self._header: dict = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/83.0.4103.97 Safari/537.36",
        }
        self._user_agents: list = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/103.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/104.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/102.0.5005.63 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, "
            "like Gecko) CriOS/103.0.5060.63 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/100.0.4896.127 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/101.0.4951.67 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/72.0.3626.121 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/44.0.2403.157 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/101.0.4951.54 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/86.0.4240.198 Safari/E7FBAF"]
        self._numiscorner_countries = ['Algeria', 'Argentina', 'Australia', 'Brazil', 'British Virgin Islands', 'Bulgaria',
                           'Cameroon', 'Chile', 'China', 'Colombia', 'Costa Rica', 'Croatia', 'Cyprus',
                           'Czech Republic', 'Czechoslovakia', 'Denmark', 'East Caribbean States', 'Egypt', 'Ethiopia',
                           'Finland', 'French Polynesia', 'Gibraltar', 'Greece', 'Hong Kong', 'Iceland', 'India',
                           'Indonesia', 'Ireland', 'Israel', 'Ivory Coast', 'Jamaica', 'Japan', 'Jersey', 'Jordan',
                           'Kazakhstan', 'Kenya', 'Korea, South', 'Latvia', 'Lithuania', 'Luxembourg', 'Madagascar',
                           'Malaysia', 'Malta', 'Mauritius', 'Mexico', 'Moldova', 'Morocco', 'New Caledonia',
                           'New Zealand', 'Norway', 'Other - Africa', 'Pakistan', 'Panama', 'Peru', 'Philippines',
                           'Portugal', 'Romania', 'San Marino', 'Serbia', 'Seychelles', 'Singapore', 'Slovakia',
                           'Slovenia', 'Somalia', 'South Africa', 'Sri Lanka', 'Sweden', 'Thailand',
                           'Trinidad & Tobago', 'Tunisia', 'Turkey', 'Ukraine', 'Venezuela', 'Yugoslavia', 'Zimbabwe']

    @property
    def countries(self) -> list:
        return self._numiscorner_countries

    def get_soup(self, url: list[str]) -> BeautifulSoup | int:
        """on fail returns request status code"""
        url = ''.join(url)
        headers = self._header
        user_agents = self._user_agents
        pick_agent = randint(0, 9)
        user_agent = user_agents[pick_agent]
        headers['User-Agent'] = user_agent

        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            print('STOP request status code: ', r.status_code)
            return r.status_code

        return BeautifulSoup(r.text, 'html.parser')


class ScrapeDocument(UrlToSoup):
    def __init__(self):
        UrlToSoup.__init__(self)
        self._url = 'https://www.numiscorner.com//products/mexico-5-pesos-1948-mexico-city-km-465-ms60-62-silver-40-30-00'
        self._saved_urls = {}
        self._dom_element_selectors = [
            {'el_id': 'title', 'dom_el': 'h1', 'el_class': 'title'},
            {'el_id': 'subtitle', 'dom_el': 'h2', 'el_class': 'subtitle'},
            {'el_id': 'price', 'dom_el': 'div', 'el_class': 'product-price'},
            {'el_id': 'grade', 'dom_el': 's', 'el_class': ''},
            {'el_id': 'sold', 'dom_el': 'span', 'el_class': 'soldout-mention'},
            {'el_id': 'obverse_image', 'dom_el': 'a', 'el_css_id': 'zoom1'},
            {'el_id': 'reverse_image', 'dom_el': 'a', 'el_css_id': 'zoom2'},
            {'el_id': 'description_wrapper', 'dom_el': 'div', 'el_class': 'classic-description'},
            {'el_id': 'specification_wrapper', 'dom_el': 'ul', 'el_class': 'required-tag'},
            {'el_id': 'second_specification_wrapper', 'dom_el': 'ul', 'el_class': 'default-tag'},
            {'el_id': 'id_wrapper', 'dom_el': 'div', 'el_class': 'product-tab-content-bloc'},
        ]

        self.init_saved_urls()

    def get_html(self) -> dict[str, list[BeautifulSoup]]:
        result = {}
        dom_element_selectors = self._dom_element_selectors
        soup = self.get_soup(self._url)

        if isinstance(soup, int):
            print('status code: ', soup, ' link: ', self._url)
            return
        # use class or id
        for selector in dom_element_selectors:
            if 'el_class' in selector:
                result.update({selector['el_id']: soup.find_all(selector['dom_el'], selector['el_class'])})
            else:
                result.update({selector['el_id']: soup.find(id=selector['el_css_id'])})

        return result

    def rule_engine(self, html_object: dict[str, list[str]]) -> dict[str, list[BeautifulSoup]]:
        html_object = self.get_html()

        # split description_wrapper
        html_object.update({'obverse_description': html_object['description_wrapper'][0].find_all(id='head')})
        html_object.update({'reverse_description': html_object['description_wrapper'][0].find_all(id='reverse')})
        html_object.update({'km_number': html_object['description_wrapper'][0].find_all('span')})
        html_object.update({'weight': html_object['description_wrapper'][0].find_all(id='weight')})

        html_object.update({'numiscorner_id': html_object['id_wrapper'][1].find_all('div', 'classic-description')})
        # handle all [key_text : value_text] => {key_text : value_text} instances
        html_object.update({'other': html_object['description_wrapper'][0].find_all('li')})
        html_object.pop('description_wrapper')
        html_object['other'] = html_object['other'] + html_object['specification_wrapper'][0].find_all('li')
        html_object.pop('specification_wrapper')
        html_object['other'] = html_object['other'] + html_object['second_specification_wrapper'][0].find_all('li')
        html_object.pop('second_specification_wrapper')
        html_object['other'] = html_object['other'] + html_object['id_wrapper'][0].find_all('li')
        html_object.pop('id_wrapper')
        # handle image urls
        html_object['obverse_image'] = ['https:' + html_object['obverse_image'].find('img')['data-src']]
        html_object['reverse_image'] = ['https:' + html_object['reverse_image'].find('img')['data-src']]
        
        return html_object

    def extract_html_content(self, html_object: dict[str, list[str]]) -> dict[str, list[str]]:
        result: dict = {}
        # skip images
        for key, el_group in html_object.items():
            if key == 'obverse_image' or key == 'reverse_image':
                result.update({key: el_group})
                continue

            html_content: list[str] = []

            for el in el_group:
                clean_text: str = ' '.join(re.sub(r'[ ]{2,}',' ',el.get_text().strip()).split())

                html_content.append(clean_text)

            result.update({key: html_content})

        return result

    def clean_km_value(self, km_value: list[str]) -> str:
        result = ''

        if len(km_value) == 1:
            try:
                result = km_value[0].replace('KM : ','')
            except:
                print('text from km value removal failed, value got: ', km_value)
                result = km_value[0]  
        elif len(km_value) == 2:
            if km_value[0].find('KM') != -1:
                result = km_value[0].replace('KM : ','')
            elif km_value[1].find('KM') != -1:
                result = km_value[1].replace('KM : ','')


        return result    

    def get_html_content(self, html_object: dict[str, list[str]]) -> dict[str, list[str]]:
        # add item url to the item specification
        html_object.update({'product_url': [self._url]})
        other_values = {}

        for k, v in html_object.items():
            # unpack arrays with one element

            if k == 'numiscorner_id' and len(v) != 0:
                html_object.update({k:v[0].split(':')[1]})
            elif k == 'price' and len(v) != 0:
                html_object.update({k:v[0].replace('€','')})
            elif k == 'km_number':
                clean_km = self.clean_km_value(v)
                html_object.update({k:clean_km})
            elif k == 'other':
                other_values = self.clean_other_values(v)
            elif isinstance(v, list):
                clean_value = self.clean_generic_values(v)
                html_object.update({k:clean_value})

        # removing ald values, and merging clean ones with with the result (line 197-198)
        html_object.pop('other')
        html_object.update(other_values)

        return html_object

    def clean_generic_values(self, value: list[str]) -> str:
        result = ''

        if len(value) == 1:
            result = value[0]
        elif len(value) == 0:
            result = ''
        else:
            result = ' '.join(value)

        return result

    def clean_other_values(self, value: list[str]) -> dict :
        result = {}
        
        for i in value:
            arr = i.split(":")
            if len(arr) == 2:
                result.update({arr[0].strip(): arr[1].strip()})

        return result      

    def scrape_single_page(self, output_csv_name: str):
        result = self.get_html()
        result = self.rule_engine(result)
        result = self.extract_html_content(result)
        result = self.get_html_content(result)

        dict_to_csv(result, output_csv_name)

    def remember_url(self, url: str):
        url_input = {url: str(datetime.date.today())}
        self._saved_urls.update(url_input)

        with open("saved_urls.json", 'w') as outfile:
            json.dump(self._saved_urls, outfile, indent=4)

    def init_saved_urls(self):
        with open("saved_urls.json", 'r') as outfileOne:
            saved_urls: dict = json.load(outfileOne)
            self._saved_urls = saved_urls

    def scrape_urls(self, input_csv_links: str, output_csv_name: str):
        csv_gen = (row for row in open(input_csv_links))
        count = 0

        while True:
            count += 1
            try:
                url = next(csv_gen)
                self._url = url.strip()
                if self._url in self._saved_urls:
                    print(count, 'URL SKIPPED, found in saved urls: ', self._url)
                    continue
                try:
                    self.scrape_single_page(output_csv_name)
                    print(count, ': ', self._url)
                    self.remember_url(self._url)
                except Exception as e:
                    print(e,'\nurl: ', self._url) 
            except (StopIteration, KeyboardInterrupt):
                sys.exit('JOB DONE!')


test = ScrapeDocument()
test.scrape_urls('numiscorner_urls_collections.csv', 'result3.csv')









class ScrapeEtsy(UrlToSoup):
    def __init__(self):
        UrlToSoup.__init__(self)
        self._product_key = '717913062'
        self._dummy_url = 'https://www.etsy.com/listing/'
        self._product_url = ''
        self._csv_key_sku: list[list] = []
        self._comments_wrapper: BeautifulSoup = None
        self._scraped_data: dict = None

    @property
    def csv_key_sku(self):
        return self._csv_key_sku

    @property
    def comments_wrapper(self):
        return self._comments_wrapper

    @property
    def scraped_data(self) -> list[list]:
        return self._scraped_data

    @property
    def dummy_url(self):
        return self._dummy_url

    @property
    def product_key(self):
        return self._product_key

    @property
    def product_url(self):
        return self._product_url

    @scraped_data.setter
    def scraped_data(self, update: dict):
        self._scraped_data.update(update)

    @product_key.setter
    def product_key(self, new_key):
        self._product_key = new_key

    @comments_wrapper.setter
    def comments_wrapper(self, new_wrapper: BeautifulSoup):
        self._comments_wrapper = new_wrapper.find(id='same-listing-reviews-panel')

    def set_csv_keys_sku(self):
        red_data = read_from_csv('etsy_linkai.csv')
        self._csv_key_sku = red_data

    def set_product_url(self):
        self._product_url = self.dummy_url + self.product_key

    def product_keys_generator(self):
        data: list = self.csv_key_sku
        print(data)
        data_head: list = data.pop(0)

        i: str = ['']
        while i[0] != data[len(data) - 1][0]:
            print(data[len(data) - 1][0])
            for i in data:
                yield i[0]

    def get_product_comments(self):
        self.comments_wrapper = self.soup
        result = None

        while self.comments_wrapper is not None:
            result = {}

        return result

    def scrape_comments(self):
        self.set_csv_keys_sku()
        next_key = self.product_keys_generator()

        while True:
            try:
                self.product_key = next(next_key)
                self.set_product_url()
                self.set_soup(self.product_url)
                self.get_product_comments()
            except StopIteration:
                print('Generator is empty, no more products')
                break


# app = ScrapeNumiscornerImg()
# app.scrape_urls()
