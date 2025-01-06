import requests
import json
import psycopg2
from psycopg2.extras import RealDictCursor
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime
import time
import undetected_chromedriver as uc


class DNSUpdater:
    def __init__(self):
        self.db_params = {
            'dbname': 'dns',
            'user': 'postgres',
            'password': '8080',
            'host': '192.168.1.67',
            'port': '5432'
        }
        self.headers = {
            'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
        }
        self.headersPrice = {
            'accept': '*/*',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'max-age=0',
            'content-type': 'application/x-www-form-urlencoded',
            # 'cookie': 'rrpvid=888584152736280; rcuid=657f5f5c1707cd69170be526; tmr_lvid=ad99facfcf4b1c77d5627ddd365d7122; tmr_lvidTS=1703006273318; _ym_uid=1703006273650960440; adrcid=AFqzKJ4sUcZ-5FzNfodZSJQ; current_path=4e4ab000281a2a3ebac0c971cf1d46c5ef532f47fb32a46931e91e4e4127fd47a%3A2%3A%7Bi%3A0%3Bs%3A12%3A%22current_path%22%3Bi%3A1%3Bs%3A153%3A%22%7B%22city%22%3A%229437a276-5970-11de-8bf7-00151716f9f5%22%2C%22cityName%22%3A%22%5Cu0420%5Cu043e%5Cu0441%5Cu0442%5Cu043e%5Cu0432-%5Cu043d%5Cu0430-%5Cu0414%5Cu043e%5Cu043d%5Cu0443%22%2C%22method%22%3A%22manual%22%7D%22%3B%7D; _ga=GA1.1.1489783013.1703006273; cf_avails=now-today-tomorrow-later-out_of_stock; cookieImagesUploadId=60bcfa2e562d9a515e25968b960d09813e6d38116d33f8a59e07c7511d0e9b6aa%3A2%3A%7Bi%3A0%3Bs%3A20%3A%22cookieImagesUploadId%22%3Bi%3A1%3Bs%3A36%3A%22a94228fa-2939-4cf8-be61-e8aa8d31e8d7%22%3B%7D; auth_public_uid=754050c310542d81b8c7bd76bf9c538a; cartUserCookieIdent_v3=8aba7aa7a14bf8590dce61af2fc008ed75581952d308185a7500931cbd5e2472a%3A2%3A%7Bi%3A0%3Bs%3A22%3A%22cartUserCookieIdent_v3%22%3Bi%3A1%3Bs%3A36%3A%22e7eebb33-4176-377e-8719-d0ee9df448fd%22%3B%7D; _ymab_param=EX1EJ6c9Dj6kuqbj9E5wIrzHipschBD7ImMeCtj1ssBuXSSd_LBKAzOtfTVFvUEHiAP4dtPU_rcFH33HFvV_AiwVflw; ab_spa=%7B%22home-delivery-test%22%3A%22main_post_delivery_2%22%7D; _gcl_au=1.1.526570916.1733588514; rrlevt=1733831928715; date-user-last-order-v2=8ec4ef83584550987322ba5a9c112f71415c1ba007b6f35794dcd038bc45a7b5a%3A2%3A%7Bi%3A0%3Bs%3A23%3A%22date-user-last-order-v2%22%3Bi%3A1%3Bi%3A1734222065%3B%7D; _ga_ND7GY87YET=GS1.1.1734633187.13.0.1734633187.60.0.23673227; phonesIdentV2=338eaf86-c97d-4203-94b3-953cd5985c74; _ab_=%7B%22card-top%22%3A%22reviews_2%22%2C%22avails-text%22%3A%22group_e%22%7D; rai=b8f00d3a0c7d1ac59b2efd7e262aec0a; rsu-configuration-id=9b4f1d79b31289b6e9a2bff2700928503107203fd0ff4344a3aafc1dfc56463fa%3A2%3A%7Bi%3A0%3Bs%3A20%3A%22rsu-configuration-id%22%3Bi%3A1%3Bs%3A36%3A%222d042019-7ae4-489d-bab4-54f3f1259bbb%22%3B%7D; _ym_d=1735813453; qrator_jsr=1736040582.203.0nq732VhHmoa64wA-gsb1flrt1fgino63t3s0p5r215qs1g3b-00; qrator_ssid=1736040585.036.ddwAiHDBN3oD5C75-c16clos7am31abon6tsqic0d6b318k14; qrator_jsid=1736040582.203.0nq732VhHmoa64wA-pc32c0m5nsah7e1nrdp53iqa1f4vgedu; lang=ru; _ym_isad=2; _ym_visorc=b; domain_sid=tuPZJLCzuh3av9wCKChDi%3A1736040589069; PHPSESSID=823b08bcd9413344d366df3ddc9e68b9; _csrf=9777b7d096fdb170f3d44e5d9097d36a5e56fd6b666813f4e12d90c58e5992cba%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%2297qPlOMYskduFOD4y_w587sJWNSta0Pu%22%3B%7D; city_path=rostov-na-donu; tmr_detect=0%7C1736040603234; last-cart-update=1; _ga_FLS4JETDHW=GS1.1.1736040588.143.1.1736040828.60.0.1183741848',
            'origin': 'https://www.dns-shop.ru',
            'priority': 'u=1, i',
            'referer': 'https://www.dns-shop.ru/catalog/17a8a05316404e77/planshety/no-referrer',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-csrf-token': 'qDlZa924bS8MdAW_kVwcoE_jQ3pK20Eqnw6qwpEoAxuRDig7sfcgdn8fYcrXE1iUNrw0T3LsMmDIQPm28BhTbg==',
            'x-requested-with': 'XMLHttpRequest',
        }
        self.cookies = {}

        # Определяем атрибут self.running с начальным значением False
        self.running = False

        self.counter = 0

    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def make_repeated_request(self, url, method="GET", headers=None, data=None, params=None, max_retries=10,
                              retry_delay=2):
        """
        Выполняет запрос к серверу с повторением до успешного ответа.

        :param url: URL для запроса.
        :param method: HTTP-метод ("GET", "POST", "PUT", "DELETE" и т. д.).
        :param headers: Заголовки запроса.
        :param data: Данные для метода "POST".
        :param params: Параметры для метода "GET".
        :param max_retries: Максимальное количество попыток.
        :param retry_delay: Задержка между попытками (в секундах).
        :return: Успешный объект `requests.Response`.
        :raises Exception: Если не удалось выполнить успешный запрос после max_retries.
        """
        for attempt in range(1, max_retries + 1):
            try:
                # Выполнение запроса
                if method.upper() == "GET":
                    response = requests.get(url, headers=headers, params=params)
                elif method.upper() == "POST":
                    response = requests.post(url, headers=headers, data=data)
                elif method.upper() == "PUT":
                    response = requests.put(url, headers=headers, data=data)
                elif method.upper() == "DELETE":
                    response = requests.delete(url, headers=headers, data=data)
                else:
                    raise ValueError(f"Метод {method} не поддерживается.")

                # Проверка успешности
                if response.status_code == 200:
                    return response
                else:
                    print(f"Попытка {attempt}: ошибка {response.status_code}. Повтор запроса...")
            except requests.RequestException as e:
                print(f"Попытка {attempt}: ошибка {e}. Повтор запроса...")

            # Задержка перед повторной попыткой
            time.sleep(retry_delay)

        # Если все попытки не удались
        raise Exception(f"Не удалось выполнить успешный запрос к {url} после {max_retries} попыток.")

    def load_products(self):
        urls = [
            'https://www.dns-shop.ru/products1.xml',
            'https://www.dns-shop.ru/products2.xml',
            'https://www.dns-shop.ru/products3.xml',
            'https://www.dns-shop.ru/products4.xml',
            'https://www.dns-shop.ru/products5.xml',
            'https://www.dns-shop.ru/products6.xml'

        ]
        conn = self.get_db_connection()
        cursor = conn.cursor()

        for url in urls:
            print(url)
            response = requests.get(url)
            root = ET.fromstring(response.content)
            for url_elem in root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                loc = url_elem.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
                id_part, id_name = loc.split('/')[-3], loc.split('/')[-2]

                cursor.execute("""
                    INSERT INTO dns_shop (id, url, id_name) VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (id_part, loc, id_name))
                if cursor.rowcount == 1:
                    print(f'Добавлен товар {id_part}, {loc}, {id_name}')

            conn.commit()

        cursor.close()
        conn.close()

    def load_name(self):
        conn = self.get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT id FROM dns_shop WHERE id2 is Null')
        product_ids = cursor.fetchall()

        for product in product_ids:
            id_p = product['id']
            url = f'https://www.dns-shop.ru/product/{id_p}/'
            response = requests.get(url, headers=self.headers, cookies=self.cookies)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                product_card = soup.find('div', {'class': 'container product-card'})
                product_id = product_card.get('data-product-card') if product_card else None

                if product_id:
                    print(product_id, id_p)
                    cursor.execute("UPDATE dns_shop SET id2 = %s WHERE id = %s", (product_id, id_p))
                    conn.commit()

                item = soup.find('meta', {'itemprop': 'position', 'content': '2'})
                if item:
                    item_name = item.find_parent('li').find('span', itemprop='name').get_text()
                    if item_name:
                        print(item_name, id_p)
                        cursor.execute("UPDATE dns_shop SET category = %s WHERE id = %s", (item_name, id_p))
                        conn.commit()

            else:
                self.get_cookies()  # Update cookies if needed

        cursor.close()
        conn.close()

    def update_product_info(self):
        conn = self.get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT id2 FROM dns_shop WHERE id2 is not Null and type is null")
        product_ids = cursor.fetchall()

        self.process_ids_and_update_table(cursor, product_ids)
        cursor.close()
        conn.close()

    def process_ids_and_update_table(self, cursor, ids):
        for product in ids:
            product_id = product["id2"]
            url = f'https://www.dns-shop.ru/pwa/pwa/get-product/?id={product_id}'
            response = requests.get(url, headers=self.headers, cookies=self.cookies)

            if response.status_code == 200:
                data = response.json().get('data', {})
                name = data.get('name', 'N/A').replace('"', "").replace("'", "")
                code = data.get('code', 'N/A')
                category = self.extract_category(data)

                cursor.execute("""
                    UPDATE dns_shop SET code = %s, name = %s, type = %s WHERE id2 = %s
                """, (code, name, category, product_id))
                conn.commit()
            else:
                self.get_cookies()

    def extract_category(self, data):
        possible_keys = [
            'Общие параметры', 'Основные параметры', 'Основные характеристики',
            'Общие характеристики', 'Общая информация', 'Классификация',
            'Общие параметры и питание', 'Данные о товаре', 'Классификация и внешний вид', 'Характеристики'
        ]
        for key in possible_keys:
            characteristics = data.get('characteristics', {}).get(key, [])
            for item in characteristics:
                if item.get('title') == 'Тип':
                    return item.get('value')
        return ""

    def get_cookies(self):
        url = 'https://www.dns-shop.ru/'

        # Настройка драйвера
        options = uc.ChromeOptions()

        # Запуск драйвера
        driver = uc.Chrome(options=options)

        # Открытие URL
        driver.get(url)
        time.sleep(5)

        # Извлечение cookies
        self.cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies() if
                        cookie['name'] == 'qrator_jsid'}
        print(self.cookies)

        # Завершение работы драйвера
        driver.quit()

    def execute_with_retry(self, cursor, query, params=None, max_attempts=5, delay=60):
        """
        Выполняет SQL-запрос с повторными попытками в случае ошибки.
        :param cursor: Курсор PostgreSQL
        :param query: Текст SQL-запроса
        :param params: Параметры для SQL-запроса (по умолчанию None)
        :param max_attempts: Максимальное количество попыток (по умолчанию 5)
        :param delay: Задержка между попытками (в секундах, по умолчанию 60)
        """
        for attempt in range(max_attempts):
            try:
                cursor.execute(query, params)
                return  # Успех — выходим из функции
            except Exception as e:
                print(f"Ошибка при выполнении SQL-запроса: {e}")
                if attempt < max_attempts - 1:
                    print(f"Попытка {attempt + 1} из {max_attempts}. Повтор через {delay} секунд...")
                    time.sleep(delay)
                else:
                    print("Превышено количество попыток. Запрос пропущен.")
                    raise  # Выбрасываем исключение после превышения попыток

    def update_product_prices(self):
        conn = self.get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor2 = conn.cursor(cursor_factory=RealDictCursor)

        cursor2.execute("SELECT * FROM dns_shop WHERE code is not Null")
        json_data = {
            "type": "product-buy",
            "containers": []
        }
        # Ограничение количества элементов
        max_items = 1000
        item_count = 0
        # Заполняем JSON данными

        while True:
            row = cursor2.fetchone()
            if row is None:
                break

            if not self.running:  # Прерываем выполнение, если флаг False
                return

            if item_count >= max_items:
                item_count = 0
                data = 'data=' + json.dumps(json_data)
                response = self.make_repeated_request('https://www.dns-shop.ru/ajax-state/product-buy/', method="POST",
                                                      headers=self.headersPrice,
                                                      data=data)
                print(response.status_code)
                if response.status_code == 200:
                    response_json = response.json()
                    self.update_prices_and_availability(cursor, response_json['data']['states'])
                    json_data["containers"].clear()  # Сброс JSON данных
                    item_count = 0  # Сброс счетчика
                    conn.commit()
                else:
                    print(response.status_code)

            code = row["code"]
            code2 = 'as-' + str(row["code"])
            new_value = {"id": code2, "data": {"id": row["code"]}}
            json_data["containers"].append(new_value)
            item_count += 1  # Увеличиваем счетчик

        if not self.running:  # Прерываем выполнение, если флаг False
            return

        data = 'data=' + json.dumps(json_data)
        response = requests.post('https://www.dns-shop.ru/ajax-state/product-buy/', headers=self.headersPrice,
                                 data=data)
        if response.status_code == 200:
            response_json = response.json()
            self.update_prices_and_availability(cursor, response_json['data']['states'])
        else:
            print(response.status_code)
        conn.commit()

        cursor.close()
        conn.close()

    def update_prices_and_availability(self, cursor, data):
        """Обновление цен и наличия продуктов в базе данных."""
        for product in data:

            if not self.running:  # Прерываем выполнение, если флаг False
                return

            product_id = product['data']['id']

            cursor.execute(f"SELECT price FROM dns_shop WHERE id2 = %s", [product_id])

            row = cursor.fetchone()

            old_price = 0

            if row['price']:
                data = row['price']
                latest_date = max(data.keys())
                old_price = data[latest_date]

            if product.get('data', {}).get('price', {}).get('onlinePay'):
                query = f"UPDATE dns_shop SET online_pay = '{product['data']['price']['onlinePay']}' WHERE id2 = '{product_id}'"
                self.execute_with_retry(cursor, query)
            else:
                query = f"UPDATE dns_shop SET online_pay = NULL WHERE id2 = '{product_id}'"
                self.execute_with_retry(cursor, query)

            if product.get('data', {}).get('notAvail'):
                query = f"UPDATE dns_shop SET availability = False WHERE id2 = '{product_id}'"
                self.execute_with_retry(cursor, query)
            else:
                query = f"UPDATE dns_shop SET availability = True WHERE id2 = '{product_id}'"
                self.execute_with_retry(cursor, query)

                if product.get('data', {}).get('price', {}).get('current') and product.get('data', {}).get('price',
                                                                                                           {}).get(
                    'current') != old_price:
                    current_date = (datetime.now()).strftime('%Y-%m-%d')
                    query = f"UPDATE dns_shop SET price = jsonb_set(price, '{{{current_date}}}', '{product['data']['price']['current']}'::jsonb, true) WHERE id2 = '{product_id}'"
                    self.execute_with_retry(cursor, query)
                    # cursor.execute(
                    #    f"UPDATE dns_shop SET price = jsonb_set(price, '{{{current_date}}}', '{product['data']['price']['current']}'::jsonb, true) WHERE id2 = '{product_id}'"
                    # )

                if product.get('data', {}).get('price', {}).get('min') and product.get('data', {}).get('price', {}).get(
                        'min') != old_price:
                    current_date = (datetime.now()).strftime('%Y-%m-%d')
                    query = f"UPDATE dns_shop SET price = jsonb_set(price, '{{{current_date}}}', '{product['data']['price']['min']}'::jsonb, true) WHERE id2 = '{product_id}'"
                    self.execute_with_retry(cursor, query)

            if product.get('data', {}).get('avail') is not None:
                query = f"UPDATE dns_shop SET status = '{product.get('data', {}).get('avail')}' WHERE id2 = '{product['data']['id']}'"
                # Выполнение запроса
                self.execute_with_retry(cursor, query)
            else:
                query = f"UPDATE dns_shop SET status = Null WHERE id2 = '{product['data']['id']}'"
                # Выполнение запроса
                self.execute_with_retry(cursor, query)

            self.counter += 1
            print(f'{self.counter} Цена на {product_id} обновлена')





