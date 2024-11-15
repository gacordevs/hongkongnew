import scrapy
import pymysql
from datetime import datetime

class NumberSpider(scrapy.Spider):
    name = 'number'
    allowed_domains = ['hongkonglotto.com']
    start_urls = ['https://hongkonglotto.com/update-loadball']

    def __init__(self, *args, **kwargs):
        super(NumberSpider, self).__init__(*args, **kwargs)  # Call the parent class' __init__
        self.connection = None
        self.cursor = None

    def open_spider(self, spider):
        """Open database connection when the spider starts."""
        try:
            self.connection = pymysql.connect(
                host='localhost',  # e.g., 'localhost' or IP address of the MySQL server
                user='public_admin',  # Your MySQL username
                password='Publicadmin123#',  # Your MySQL password
                database='hongkong',  # The name of the database
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.connection.cursor()

            # Create the table if it doesn't exist
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
            self.logger.info("Database connection established successfully.")
        except Exception as e:
            self.logger.error(f"Failed to open database connection: {e}")

    def parse(self, response):
        """Parse the response and extract the numbers."""
        first_place_numbers = []
        second_place_numbers = []
        third_place_numbers = []

        # Extract data for first, second, and third places
        for first in response.css('div[data-id="2526:6087"]'):
            first_place = first.css('div.frame-42234')
            number_1 = first_place.css('img::attr(alt)').getall()
            clean_number1 = [n.replace("Property 1=", "").replace(",", "") for n in number_1]
            first_place_numbers = clean_number1

        for second in response.css('div[data-id="2526:6088"]'):
            second_place = second.css('div.frame-42234')
            number_2 = second_place.css('img::attr(alt)').getall()
            clean_number2 = [n.replace("Property 1=", "").replace(",", "") for n in number_2]
            second_place_numbers = clean_number2

        for third in response.css('div[data-id="2526:6106"]'):
            third_place = third.css('div.frame-42234')
            number_3 = third_place.css('img::attr(alt)').getall()
            clean_number3 = [n.replace("Property 1=", "").replace(",", "") for n in number_3]
            third_place_numbers = clean_number3

        # Get current date and time
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Save to DB
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
        """Save scraped data to the MySQL database."""
        if self.connection is None or self.cursor is None:
            self.logger.error("Database connection or cursor is not initialized.")
            return  # Exit early if the connection is not initialized
        
        first_str = ' '.join([f.replace(",", "") for f in first])
        second_str = ' '.join([s.replace(",", "") for s in second])
        third_str = ' '.join([t.replace(",", "") for t in third])

        try:
            # Insert the data into the MySQL table
            self.cursor.execute(''' 
                INSERT INTO keluaran (date, first, second, third)
                VALUES (%s, %s, %s, %s)
            ''', (date, first_str, second_str, third_str))
            self.connection.commit()
            self.logger.info(f"Data inserted successfully for date {date}")
        except Exception as e:
            self.logger.error(f"Failed to insert data into database: {e}")

    def close_spider(self, spider):
        """Close the database connection when the spider finishes."""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed.")
