import os
from dotenv import load_dotenv
import requests
import psycopg2
import re
import datetime
from pprint import pprint
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


def connection():
    conn = psycopg2.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        dbname='market_db',
        user=os.environ['DB_LOGIN'],
        password=os.environ['DB_PASSWORD'],
        target_session_attrs='read-write',
        sslmode='verify-full'
    )
    return conn


def get_account_id(conn):
    with conn:
        with conn.cursor() as select:
            select.execute(
                "SELECT * FROM account_list WHERE mp_id = 1 "
                "OR mp_id = 2 OR mp_id = 3 OR mp_id = 15 OR "
                "mp_id = 14;"
            )
            lines_table = select.fetchall()
            accounts_list = {1: [], 2: [], 3: [], 14: [], 15: []}
            for line in lines_table:
                if line[1] == 1:
                    accounts_list[1].append(line[0])
                if line[1] == 2:
                    accounts_list[2].append(line[0])
                if line[1] == 3:
                    accounts_list[3].append(line[0])
                if line[1] == 14:
                    accounts_list[14].append(line[0])
                if line[1] == 15:
                    accounts_list[15].append(line[0])
            return accounts_list


def data_collection(accounts_list: list, conn, accounts_data: dict, mp_id: int):
    with conn:
        with conn.cursor() as select:
            for key, val in accounts_list.items():
                if key == mp_id:
                    for account_id in val:
                        select.execute(
                            f"SELECT sa.attribute_name, asd.attribute_value " 
                            f"FROM account_list al join account_service_data asd " 
                            f"on al.id = asd.account_id join  service_attr sa on "
                            f"asd.attribute_id = sa.id where al.id = {account_id};"
                        )
                        list_data = select.fetchall()
                        if len(list_data) != 0:
                            account_data = {}
                            for tuple_data in list_data:
                                temporary_dict_data = {tuple_data[0]: tuple_data[1]}
                                account_data = {**account_data, **temporary_dict_data}
                            accounts_data[mp_id] = {**accounts_data[mp_id], **{account_id: account_data}}
            return accounts_data


def status_update(conn, status, id):
    try:
        with conn:
            with conn.cursor() as select:
                select.execute(
                    f"UPDATE account_list SET status_1 = '{status}' WHERE id = {id};"
                )
                conn.commit()
    except Exception as E:
        print(f"Exception in sent to DB: {E}")


def main():
    accounts_data = {1: {}, 2: {}, 3: {}, 14: {}, 15: {}}
    conn = connection()
    account_list = get_account_id(conn)  # Запуск не по расписанию
    for key in accounts_data.keys():
        res = data_collection(account_list, conn, accounts_data, key)
        accounts_data = {**accounts_data, **res}
    class_instance = ValidateAccount()
    for key, val in accounts_data[1].items():
        if ('client_id_api' and 'api_key') in val.keys():
            account_status = class_instance.validate_ozon(val['client_id_api'], val['api_key'])
            status_update(conn, account_status, key)
        else:
            status_update(conn, 'Disactive', key)
    for key, val in accounts_data[14].items():
        if ("client_id_performance" and "client_secret_performance") in val.keys():
            account_status = class_instance.validate_ozon_performance(
                val['client_secret_performance'], val['client_id_performance'])
            status_update(conn, account_status, key)
        else:
            status_update(conn, 'Disactive', key)
    for key, val in accounts_data[2].items():
        if ("api_key" and "client_id_api") in val.keys():
            account_status = class_instance.validate_yandex(
                val['client_id_api'], val['api_key'])
            status_update(conn, account_status, key)
        else:
            status_update(conn, 'Disactive', key)
    for key, val in accounts_data[3].items():
        if "client_id_api" in val.keys():
            account_status = class_instance.validate_wildberries(
                val['client_id_api'])
            status_update(conn, account_status, key)
        else:
            status_update(conn, 'Disactive', key)
    for key, val in accounts_data[15].items():
        if "api_key" in val.keys():
            account_status = class_instance.validate_wbstatistic(
                val['api_key'])
            print(key, account_status)
            status_update(conn, account_status, key)
        else:
            status_update(conn, 'Disactive', key)


if __name__ == '__main__':
    main()
