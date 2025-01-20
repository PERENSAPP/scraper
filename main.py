import requests
import pymysql
from bs4 import BeautifulSoup

# Database connection configuration
db_config = {
    "host": "127.0.0.1",  # MySQL server address
    "user": "root",       # MySQL username
    "password": "",       # MySQL password
    "database": "project-10",  # Database name
    "connection_timeout": 10  # Connection timeout in seconds
}

# Function to fetch a webpage
def fetch_page_content(page_number):
    try:
        url = f"https://www.mojo.nl/agenda?page={page_number}"
        response = requests.get(url)
        
        if response.status_code == 200:
            print(f"Successfully fetched page {page_number}")
            return response.content
        else:
            print(f"Failed to fetch page {page_number}: HTTP {response.status_code}")
            return None
    except requests.exceptions.RequestException as error:
        print(f"Error fetching page {page_number}: {error}")
        return None

# Function to scrape a specific page
def scrape_page(page_number, cursor, db):
    print(f"Processing page {page_number}")
    page_content = fetch_page_content(page_number)
    
    if not page_content:
        return False  # Return False if no content is fetched (i.e., page doesn't exist)

    soup = BeautifulSoup(page_content, 'html.parser')
    agenda_items = soup.find_all('div', class_='c-agenda-item')

    if not agenda_items:
        print(f"No agenda items found on page {page_number}")
        return False  # Return False if no agenda items are found

    # Process and insert each agenda item into the database
    for item in agenda_items:
        try:
            # Extract data for each event
            name = item.find('h3', class_='agenda-item__title').text.strip() if item.find('h3', class_='agenda-item__title') else "Unnamed Event"
            day = item.find('span', class_='agenda-item__date-day').text.strip() if item.find('span', class_='agenda-item__date-day') else "Unknown"
            month = item.find('span', class_='agenda-item__date-month').text.strip() if item.find('span', class_='agenda-item__date-month') else "Unknown"
            date = f"{day} {month}"
            location = item.find('div', class_='agenda-item__subtitle u-text--small').text.strip() if item.find('div', class_='agenda-item__subtitle u-text--small') else "Unknown Location"
            ticket_link = item.find('a', class_='agenda-item__link')['href'] if item.find('a', class_='agenda-item__link') else None

            # Extract the image URL
            image_tag = item.find('img', class_='image__default')

            if image_tag:
                # Try to extract the image URL from srcset, data-src, or src
                event_picture = (
                    image_tag.get('srcset', None) or  # Try `srcset` first
                    image_tag.get('data-src', None) or  # Fallback to `data-src` if available
                    image_tag.get('src', None)  # Fallback to `src` as a last resort
                )

                # If `srcset` is used, extract the highest-quality URL
                if event_picture and 'srcset' in image_tag.attrs:
                    srcset = image_tag['srcset']
                    # Extract the last (highest resolution) URL from `srcset`
                    event_picture = srcset.split(',')[-1].split(' ')[0].strip()
            else:
                event_picture = None

            # Check for None or empty strings, replace with NULL where necessary
            if not ticket_link:
                ticket_link = None
            if not event_picture:
                event_picture = None

            # SQL query for inserting data
            sql = """
                INSERT INTO events (event_name, event_date, location, event_type, description, ticket_link, event_picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (name, date, location, "Music", None, ticket_link, event_picture)

            # Insert the event into the database
            cursor.execute(sql, values)
            db.commit()
            print(f"Inserted: {name} ({date}) at {location} with image {event_picture}")
        except Exception as error:
            print(f"Error processing item: {error}")

    return True  # Return True if page has content

# Main function to manage the scraping process
def main():
    try:
        db = pymysql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = db.cursor()

        page_number = 1  # Start with page 1

        while True:
            # Try to scrape the current page
            if not scrape_page(page_number, cursor, db):
                print(f"Finished scraping at page {page_number}. No more content to fetch.")
                break  # Break the loop if no content is found on the page

            page_number += 1  # Move to the next page

        cursor.close()
        db.close()
        print("Scraping completed.")
    except Exception as error:
        print(f"Database connection error: {error}")

if __name__ == "__main__":
    main()
