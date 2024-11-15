import scrapy
import pymysql
from datetime import datetime

class NumberSpider(scrapy.Spider):
    name = 'number'
    allowed_domains = ['hongkonglotto.com']
    start_urls = ['https://hongkonglotto.com/update-loadball']

    def __init__(self):
        self.connection = pymysql.connect(
            host='localhost',  # e.g., 'localhost' or IP address of the MySQL server
            user='public_admin',  # Your MySQL username
            password='Publicadmin123#',  # Your MySQL password
            database='hongkong',  # The name of the database
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connection.cursor()

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS keluaran (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATETIME,
                first TEXT,
                second TEXT,
                third TEXT
            )
        ''')
        self.connection.commit()

    def parse(self, response):
        first_place_numbers = []
        second_place_numbers = []
        third_place_numbers = []

        for first in response.css('div[data-id="2526:6087"]'):
            first_place = first.css('div.frame-42234')
            number_1 = first_place.css('img::attr(alt)').getall()
            clean_number1 = [n.replace("Property 1=", "").replace(",", "") for n in number_1]  # Remove commas
            first_place_numbers = clean_number1

        for second in response.css('div[data-id="2526:6088"]'):
            second_place = second.css('div.frame-42234')
            number_2 = second_place.css('img::attr(alt)').getall()
            clean_number2 = [n.replace("Property 1=", "").replace(",", "") for n in number_2]  # Remove commas
            second_place_numbers = clean_number2

        for third in response.css('div[data-id="2526:6106"]'):
            third_place = third.css('div.frame-42234')
            number_3 = third_place.css('img::attr(alt)').getall()
            clean_number3 = [n.replace("Property 1=", "").replace(",", "") for n in number_3]  # Remove commas
            third_place_numbers = clean_number3

        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.save_to_db(current_date, first_place_numbers, second_place_numbers, third_place_numbers)

        yield {
            'keluaran': {
                'date': current_date,
                'first': first_place_numbers,
                'second': second_place_numbers,
                'third': third_place_numbers
            }
        }

    def save_to_db(self, date, first, second, third):
        # Remove commas from each element, then join into strings
        first_str = ' '.join([f.replace(",", "") for f in first])
        second_str = ' '.join([s.replace(",", "") for s in second])
        third_str = ' '.join([t.replace(",", "") for t in third])

        # Insert the data into the MySQL table
        self.cursor.execute('''
            INSERT INTO keluaran (date, first, second, third)
            VALUES (%s, %s, %s, %s)
        ''', (date, first_str, second_str, third_str))
        self.connection.commit()

    def close(self, reason):
        self.connection.close()
