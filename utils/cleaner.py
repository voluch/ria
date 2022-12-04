import pandas as pd
import re


class DataCleaner:
    def __init__(self):
        self.df = None
        self.dict = None

    def clean(self, path_input, path_output):
        self.df = pd.read_csv(path_input, index_col=[0])
        self.remove_new_cars()
        self.get_car_type()
        self.process_country_dtp_col()
        self.dict = self.df.to_dict('records')
        self.transform_pairs('main_cats', 'main_info')
        self.transform_pairs('paid_cats', 'paid_info')
        self.transform_pairs('additional_cats', 'additional_info')
        self.df = pd.DataFrame(self.dict)
        self.df.to_csv(path_output)

    def get_car_type(self):
        self.df['Тип кузова'] = [re.search("^[а-яА-Яiі/ ]+", text).group(0) if re.search("^[а-яА-Яi/ ]+", text) else None
                                 for text in self.df.loc[:, 'car_type'].tolist()]

    def process_country_dtp_col(self):
        self.df['Пригнаний з'] = [
            re.search("Пригнаний з[а-яА-ЯІЮЄЇiієюї ]+", str(text)).group(0) if re.search("Пригнаний з[а-яА-ЯІЮЄЇiієюї ]+", str(text)) else None
            for text in self.df.loc[:, 'country_dtp'].tolist()]
        self.df['ДТП індикатор'] = [True if re.search("ДТП", str(text)) else False
            for text in self.df.loc[:, 'country_dtp'].tolist()]
        self.df['Торг'] = [True if re.search("Торг", str(text)) else False
            for text in self.df.loc[:, 'country_dtp'].tolist()]
        self.df['Обмін'] = [True if re.search("Обмін", str(text)) else False
            for text in self.df.loc[:, 'country_dtp'].tolist()]

    def transform_pairs(self, col1, col2):
        for car in self.dict:
            cats = str(car[col1]).split(';')
            cats_info = str(car[col2]).split(';')
            if len(cats) != len(cats_info):
                print(cats, cats_info, "\n\n")
            else:
                for _cat, _info in zip(cats, cats_info):
                    car[_cat.strip()] = _info.strip()

    def remove_new_cars(self):
        new_cars = [True if 'newauto' in car else False for car in self.df.loc[:, 'url'].tolist()]
        self.df.drop(self.df.loc[new_cars].index.tolist(), inplace=True)


cleaner = DataCleaner()
cleaner.clean('/home/vdubyna/PycharmProjects/ria/data/scraped/ford.csv',
              '/home/vdubyna/PycharmProjects/ria/data/cleaned/ford.csv')
