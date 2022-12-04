import os
import re
import shutil
import time
from concurrent import futures

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


class Scraper:
    def __init__(self, start_url="https://auto.ria.com/uk/"):
        self.dataframe = None
        self.scraped_data = []

        # This code makes opening browser in background
        options = Options()
        options.binary_location = "/usr/bin/firefox"
        options.add_argument("-headless")

        # Initiating browser
        self.driver = webdriver.Firefox(options=options)

        # Calling browser
        self.driver.get(start_url)

        # Set correct user agent
        self.s = requests.Session()
        selenium_user_agent = self.driver.execute_script("return navigator.userAgent;")
        self.s.headers.update({"user-agent": selenium_user_agent})
        for cookie in self.driver.get_cookies():
            self.s.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

    def scrape(self, start_url, brand):
        # This code get reviews from site and save them into .csv
        tic = time.perf_counter()

        i = 500
        r = self.s.get(start_url)
        while r.status_code == 200:
            urls = self.get_urls(r)
            if urls:
                # start_time = time.time()
                # print("start", start_time)
                with futures.ThreadPoolExecutor(6) as executor:
                    outputs = executor.map(self.get_all_data, urls)

                outputs = [x for x in outputs if x]
                if outputs:
                    self.scraped_data.extend([x for x in outputs if x])
                # else:
                #     break
                # print("Multiprocess--- %s seconds ---" % (time.time() - start_time))

                # Next page
                time.sleep(0.5)
                print(i)
                r = s.get(start_url + f'&page={i}')
                i += 1
            else:
                print('No urls', start_url + f'&page={i - 1}')
                break
            if i % 100 == 0:
                self.dataframe = pd.DataFrame(self.scraped_data)
                self.dataframe.to_csv(f'data/scraped/{brand}_{i - 1}.csv')

        self.dataframe = pd.DataFrame(self.scraped_data)
        self.dataframe.to_csv(f'data/scraped/{brand}.csv')
        print(r.status_code, start_url + f'&page={i - 1}')
        toc = time.perf_counter()
        print(f"Time to get all reviews {(toc - tic) / 60} minutes")

    def get_all_data(self, url):
        s = requests.Session()
        selenium_user_agent = self.driver.execute_script("return navigator.userAgent;")
        s.headers.update({"user-agent": selenium_user_agent})
        for cookie in self.driver.get_cookies():
            s.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
        try:
            car = {}
            r = s.get(url)
            car['url'] = url
            car['car_type'] = self.get_car_type(r)
            car['price'] = self.get_price(r)
            car['model_year'] = self.get_model_year(r)
            car['country_dtp'] = self.get_country_and_dtp(r)
            cats, info = self.get_checked_info(r)
            car['main_cats'] = cats
            car['main_info'] = info
            cats, info = self.get_paid_checked_info(r)
            car['paid_cats'] = cats
            car['paid_info'] = info
            car['full_description'] = self.get_full_description(r)
            cats, info = self.get_additional_info(r)
            car['additional_cats'] = cats
            car['additional_info'] = info
            return car
        except:
            print(f'skipped car')
            return None

    @staticmethod
    def get_urls(response):
        urls = []
        soup = BeautifulSoup(response.text, 'html.parser')
        _li = soup.find_all('a', class_="address")
        for lead in _li:
            urls.append(lead['href'])
        return urls

    @staticmethod
    def get_car_type(response):
        soup = BeautifulSoup(response.text, 'html.parser')
        _li = soup.find_all('img', class_="outline m-auto")
        if _li:
            return _li[0]['alt']
        else:
            return None

    @staticmethod
    def get_price(response):
        soup = BeautifulSoup(response.text, 'html.parser')
        _li = soup.find_all('span', class_="price_value")
        return ''.join(re.findall(r'[\d ]+', str(_li[0]))).strip()

    @staticmethod
    def get_model_year(response):
        soup = BeautifulSoup(response.text, 'html.parser')
        sub_li = soup.find_all('h1', class_="head")
        if sub_li:
            return sub_li[0]['title']
        else:
            return None

    @staticmethod
    def get_country_and_dtp(response):
        result = ''
        soup = BeautifulSoup(response.text, 'html.parser')
        _li = soup.find_all('ul', class_="unstyle label-param")
        if _li:
            new_soup = BeautifulSoup(str(_li[0]), 'html.parser')
            new_li = new_soup.find_all('li', class_="item")
            for tag in new_li:
                result += tag.text + ','
        return result

    @staticmethod
    def get_checked_info(response):
        # CHECKED BY AUTORIA
        result_cats = ''
        result_info = ''
        soup = BeautifulSoup(response.text, 'html.parser')
        li = soup.find_all('div', class_="technical-info ticket-checked")
        if li:
            new_soup = BeautifulSoup(str(li[0]), 'html.parser')
            new_li = new_soup.find_all('span', class_="label")
            for tag in new_li:
                result_cats += tag.text + ';'
            new_li = new_soup.find_all('span', class_="argument")
            for tag in new_li:
                result_info += tag.text + ';'
        return result_cats, result_info

    @staticmethod
    def get_paid_checked_info(response):
        # PAID CHECKED BY AUTORIA
        result_cats = ''
        result_info = ''
        soup = BeautifulSoup(response.text, 'html.parser')
        li = soup.find_all('div', class_="paid technical-info ticket-checked")
        if li:
            new_soup = BeautifulSoup(str(li[0]), 'html.parser')
            new_li = new_soup.find_all('span', class_="label")
            for tag in new_li:
                result_cats += tag.text + ';'
            new_li = new_soup.find_all('span', class_="argument")
            for tag in new_li:
                result_info += tag.text + ';'
        return result_cats, result_info

    @staticmethod
    def get_full_description(response):
        # FULL DESCRIPTION
        soup = BeautifulSoup(response.text, 'html.parser')
        li = soup.find_all('div', class_="full-description")
        for tag in li:
            return tag.text

    @staticmethod
    def get_additional_info(response):
        # DESCRIPTION INFO
        result_cats = ''
        result_info = ''
        soup = BeautifulSoup(response.text, 'html.parser')
        li = soup.find_all('div', class_="box-panel description-car")
        if li:
            new_soup = BeautifulSoup(str(li[0]), 'html.parser')
            new_li = new_soup.find_all('span', class_="label")
            for tag in new_li:
                result_cats += tag.text + ';'
            new_li = new_soup.find_all('span', class_="argument")
            for tag in new_li:
                result_info += tag.text + ';'
        return result_cats, result_info

    @staticmethod
    def scrape_images(urls, path_to_save):
        for url in urls:
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                with open(os.path.join(path_to_save, f"{url.split('/')[-1]}"), 'wb') as f:
                    r.raw.decode_content = True
                    shutil.copyfileobj(r.raw, f)

# import json
#
# with open('json/brands.json') as f:
#     with open('json/brands.json') as f:
#         data = json.load(f)
#
# for brand, id in data.items():
#     print(brand)
#     url = f"https://auto.ria.com/uk/search/?categories.main.id=1&brand.id[0]={id}&year[0].gte=2000&indexName=auto,order_auto,newauto_search&body.id[0]=307&body.id[1]=449&body.id[2]=8&body.id[3]=2&body.id[4]=5&body.id[5]=3&body.id[6]=4&credit=0&confiscated=0&size=20"
#     s = Scraper()
#     s.scrape(url, brand)
