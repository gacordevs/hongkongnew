import scrapy
import pymysql
from datetime import datetime

class NumberSpider(scrapy.Spider):
    name = 'number'
    allowed_domains = ['hongkonglotto.com']
    start_urls = ['https://hongkonglotto.com/update-loadball']

    def open_spider(self, spider):
        """Open database connection when the spider starts."""
        try:
            self.logger.info("Opening database connection...")
            self.connection = pymysql.connect(
                host='localhost', 
                user='public_admin', 
                password='Publicadmin123#', 
                database='hongkong', 
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.connection.cursor()
            self.logger.info("Database connection established successfully.")

            # Create table if it doesn't exist
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

        except pymysql.MySQLError as e:
            self.logger.error(f"MySQL error occurred: {e.args}")
        except Exception as e:
            self.logger.error(f"Failed to open database connection: {e}")

    def parse(self, response):
        """Parse the response and extract the numbers."""
        first_place_numbers = []
        second_place_numbers = []
        third_place_numbers = []

        if not hasattr(self, 'connection') or self.connection is None:
            self.logger.error("Database connection not established.")
            return

        # Extract data for first, second, and third places
        for first in response.css('div[data-id="2526:6087"]'):
            first_place = first.css('div.frame-42234')
            number_1 = first_place.css('img::attr(alt)').getall()
            first_place_numbers = [n.replace("Property 1=", "").replace(",", "") for n in number_1]

        for second in response.css('div[data-id="2526:6088"]'):
            second_place = second.css('div.frame-42234')
            number_2 = second_place.css('img::attr(alt)').getall()
            second_place_numbers = [n.replace("Property 1=", "").replace(",", "") for n in number_2]

        for third in response.css('div[data-id="2526:6106"]'):
            third_place = third.css('div.frame-42234')
            number_3 = third_place.css('img::attr(alt)').getall()
            third_place_numbers = [n.replace("Property 1=", "").replace(",", "") for n in number_3]

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
        first_str = ' '.join([f.replace(",", "") for f in first])
        second_str = ' '.join([s.replace(",", "") for s in second])
        third_str = ' '.join([t.replace(",", "") for t in third])

        try:
            if self.connection and self.cursor:
                # Insert the data into the MySQL table
                self.cursor.execute(''' 
                    INSERT INTO keluaran (date, first, second, third)
                    VALUES (%s, %s, %s, %s)
                ''', (date, first_str, second_str, third_str))
                self.connection.commit()
                self.logger.info(f"Data inserted successfully for date {date}")
            else:
                self.logger.error("Database connection or cursor is not initialized.")
        except pymysql.MySQLError as e:
            self.logger.error(f"Database insert error: {e.args}")
        except Exception as e:
            self.logger.error(f"Failed to insert data into database: {e}")

    def close_spider(self, spider):
        """Close the database connection when the spider finishes."""
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            self.logger.info("Database connection closed.")
