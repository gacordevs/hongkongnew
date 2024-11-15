import scrapy
import pymysql
from datetime import datetime
import logging

class NumberSpider(scrapy.Spider):
    name = 'number'
    allowed_domains = ['hongkonglotto.com']
    start_urls = ['https://hongkonglotto.com/update-loadball']

    def open_spider(self, spider):
    """Open database connection when the spider starts."""
    logging.info("Opening database connection...")
    try:
        # Use environment variables for credentials (ScrapeOps typically uses env vars)
        host = os.getenv('DB_HOST', 'localhost')  # Default to localhost, but can be overridden in ScrapeOps
        user = os.getenv('DB_USER', 'public_admin')
        password = os.getenv('DB_PASSWORD', 'Publicadmin123#')
        database = os.getenv('DB_NAME', 'hongkong')

        # Try to establish the database connection
        self.connection = pymysql.connect(
            host=host,  
            user=user,  
            password=password,  
            database=database,  
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        # Initialize cursor after successful connection
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
        logging.info("Database connection and table setup successful.")
    except Exception as e:
        logging.error(f"Error setting up database connection: {e}")
        raise

    # Ensure cursor initialization is confirmed
    if not hasattr(self, 'cursor'):
        logging.error("Cursor initialization failed.")
    else:
        logging.info("Cursor initialized successfully.")

    def parse(self, response):
        logging.info("Parsing response...")
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

        # Ensure save_to_db is only called after the cursor has been initialized
        if hasattr(self, 'cursor'):
            logging.info("Saving data to database...")
            self.save_to_db(current_date, first_place_numbers, second_place_numbers, third_place_numbers)
        else:
            logging.error("Cursor is not initialized. Skipping save_to_db.")

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
        try:
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
            logging.info("Data saved to database successfully.")
        except Exception as e:
            logging.error(f"Error saving data to database: {e}")

    def close_spider(self, spider):
        """Close the database connection when the spider finishes."""
        logging.info("Closing database connection...")
        try:
            self.connection.close()
            logging.info("Database connection closed successfully.")
        except Exception as e:
            logging.error(f"Error closing database connection: {e}")
