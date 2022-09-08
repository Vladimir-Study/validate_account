import os
from dotenv import load_dotenv
import requests
import psycopg2
import re
import datetime
from tqdm import tqdm
import schedule

load_dotenv()


# Класс принимает API tokenи проверяет валидность аккаунта,
# каждый метод возвращает True если аккаунт валиден
# mp_id: 1- Ozon, 2-
class ValidateAccount():

    # Ready
    def validate_ozon(self, client_id: str, api_key: str):
        url = 'https://api-seller.ozon.ru/v1/warehouse/list'
        headers = {
            'Client-Id': client_id,
            'Api-Key': api_key,
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Content-Type': 'application/json',
        }
        try:
            response = requests.post(url, headers=headers)
            if response.status_code == 200:
                return "Active"
            return "Disactive"
        except Exception as E:
            return E

    def access_token(self, client_secret: str, client_id: str):
        url = 'https://performance.ozon.ru/api/client/token'
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }
        response = requests.post(url, data=data)
        res = response.content.decode()
        pattern = r'["]{1}'
        access_token = re.split(pattern, res)
        return access_token[3]

    # Ready
    def validate_ozon_performance(self, client_secret: str, client_id: str):
        access_token = self.access_token(client_secret, client_id)
        access_token = f'Bearer {access_token}'
        url = "https://performance.ozon.ru:443/api/client/campaign"
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Authorization': access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return "Active"
            return "Disactive"
        except Exception as E:
            return E

    # Ready
    def validate_wildberries(self, token: str):
        url = 'https://suppliers-api.wildberries.ru/api/v2/warehouses'
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Authorization': token,
            'accept': 'application/json'
        }

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return "Active"
            return "Disactive"
        except Exception as E:
            return E

    # Ready
    def validate_wbstatistic(self, token: str):
        url = f'https://suppliers-stats.wildberries.ru/api/v1/supplier/incomes'
        date = datetime.date.today()
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        }
        params = {
            'dateFrom': date,
            'key': token,
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return "Active"
            return "Disactive"
        except Exception as E:
            return E

    # Ready
    def validate_yandex(self, token: str, client_id: str):
        url = 'https://api.partner.market.yandex.ru/v2/campaigns.json'
        headers = {
            'Content-Type': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Authorization': f'OAuth oauth_token={token}, oauth_client_id={client_id}',
        }
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return "Active"
            return "Disactive"
        except Exception as E:
            return E


def request_params(params_index: dict, line_table: tuple) -> dict:
    return_params = {}
    for key, val in params_index.items():
        if line_table[1] == 1 and key in ['client_secret_performance',
                                          'client_id_performance', 'client_id_api', 'api_key']:
            if line_table[val] is not None and line_table[val] != '':
                param = {key: line_table[val]}
                return_params = {**return_params, **param}
        elif line_table[1] == 2 and key in ['client_id_api', 'api_key']:
            if line_table[val] is not None and line_table[val] != '':
                param = {key: line_table[val]}
                return_params = {**return_params, **param}
        elif line_table[1] == 3 and key in ['client_id_api', 'api_key']:
            if line_table[val] is not None and line_table[val] != '':
                param = {key: line_table[val]}
                return_params = {**return_params, **param}
    if return_params != {}:
        return return_params


def set_account_status():
    conn = psycopg2.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        dbname='market_db',
        user=os.environ['DB_LOGIN'],
        password=os.environ['DB_PASSWORD'],
        target_session_attrs='read-write',
        sslmode='verify-full'
    )
    select = conn.cursor()
    select.execute("SELECT * FROM account_list;")
    lines_table = select.fetchall()
    param_index = {
        'client_secret_performance': 2,
        'client_id_performance': 4,
        'client_id_api': 5,
        'api_key': 6,
    }
    mp_validate = ValidateAccount()
    for line in tqdm(lines_table):
        params_request = request_params(param_index, line)
        if line[1] == 1 and params_request is not None:  # Ozon
            if 'client_secret_performance' in params_request.keys() \
                    and 'api_key' in params_request.keys():
                mp_oz_per = mp_validate.validate_ozon_performance(params_request['client_secret_performance'],
                                                                  params_request['client_id_performance'])
                mp_oz = mp_validate.validate_ozon(params_request['client_id_api'], params_request['api_key'])
                line_write = f"UPDATE account_list SET status_1 = '{mp_oz}', status_2 = '{mp_oz_per}' WHERE id = {line[0]};"
                select.execute(line_write)
                conn.commit()
            elif 'api_key' not in params_request.keys():
                mp_oz_per = mp_validate.validate_ozon_performance(params_request['client_secret_performance'],
                                                                  params_request['client_id_performance'])
                line_write = f"UPDATE account_list SET status_2 = '{mp_oz_per}' WHERE id = {line[0]};"
                select.execute(line_write)
                conn.commit()
            elif 'client_secret_performance' not in params_request.keys():
                mp_oz = mp_validate.validate_ozon(params_request['client_id_api'], params_request['api_key'])
                line_write = f"UPDATE account_list SET status_1 = '{mp_oz}' WHERE id = {line[0]};"
                select.execute(line_write)
                conn.commit()
        elif line[1] == 2 and params_request is not None:  # Yandex
            mp_ya = mp_validate.validate_yandex(params_request['client_id_api'],
                                                params_request['api_key'])
            line_write = f"UPDATE account_list SET status_1 = '{mp_ya}' WHERE id = {line[0]};"
            select.execute(line_write)
            conn.commit()
        elif line[1] == 3 and params_request is not None:  # Wildberries
            if 'client_id_api' in params_request.keys() and 'api_key' in params_request.keys():
                mp_wb_stat = mp_validate.validate_wbstatistic(params_request['api_key'])
                mp_wb = mp_validate.validate_wildberries(params_request['client_id_api'])
                line_write = f"UPDATE account_list SET status_1 = '{mp_wb}', status_2 = '{mp_wb_stat}' WHERE id = {line[0]};"
                select.execute(line_write)
                conn.commit()
            elif 'api_key' in params_request.keys():
                mp_wb_stat = mp_validate.validate_wbstatistic(params_request['api_key'])
                line_write = f"UPDATE account_list SET status_2 = '{mp_wb_stat}' WHERE id = {line[0]};"
                select.execute(line_write)
                conn.commit()
            elif 'client_id_api' in params_request.keys():
                mp_wb = mp_validate.validate_wildberries(params_request['client_id_api'])
                line_write = f"UPDATE account_list SET status_1 = '{mp_wb}' WHERE id = {line[0]};"
                select.execute(line_write)
                conn.commit()
        elif params_request is None:
            line_write = f"UPDATE account_list SET status_1 = 'Disactive', status_2 = 'Disactive' WHERE id = {line[0]};"
            select.execute(line_write)
            conn.commit()
    conn.close()


if __name__ == '__main__':
    set_account_status()  # Запуск не по расписанию
    # schedule.every().day.at('7:00').do(set_account_status)  # Запуск каждый день в 7:00 утра

