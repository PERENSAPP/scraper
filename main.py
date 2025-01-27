import requests
import pymysql
from bs4 import BeautifulSoup

# Database connection configuration
db_config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "project-10",
    "connect_timeout": 10  # Changed to 'connect_timeout'
}

# Function to fetch a webpage
def fetch_page_content(page_number):
    url = f"https://www.mojo.nl/agenda?page={page_number}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Will raise an HTTPError for bad responses
        print(f"Successfully fetched page {page_number}")
        return response.content
    except requests.exceptions.RequestException as error:
        print(f"Error fetching page {page_number}: {error}")
        return None

# Function to extract event data from the HTML
def extract_event_data(item):
    name = item.find('h3', class_='agenda-item__title').text.strip() if item.find('h3', class_='agenda-item__title') else "Unnamed Event"
    day = item.find('span', class_='agenda-item__date-day').text.strip() if item.find('span', class_='agenda-item__date-day') else "Unknown"
    month = item.find('span', class_='agenda-item__date-month').text.strip() if item.find('span', class_='agenda-item__date-month') else "Unknown"
    date = f"{day} {month}"
    location = item.find('div', class_='agenda-item__subtitle u-text--small').text.strip() if item.find('div', class_='agenda-item__subtitle u-text--small') else "Unknown Location"
    ticket_link = item.find('a', class_='agenda-item__link')['href'] if item.find('a', class_='agenda-item__link') else None

    # Extract the image URL if available
    event_picture = None
    picture_tag = item.find('picture')
    if picture_tag:
        image_tag = picture_tag.find('source', {'media': '(min-width: 1240px)', 'type': 'image/jpeg'})
        if image_tag:
            srcset = image_tag.get('data-srcset', '').strip()
            if srcset:
                srcset_url = srcset.split(',')[0].split(' ')[0].strip()
                if srcset_url.startswith("/media/"):
                    srcset_url = f"https://www.mojo.nl{srcset_url}"  # Add the base URL
                event_picture = srcset_url
    return name, date, location, ticket_link, event_picture

# Function to insert event into the database
def insert_event(cursor, event_data):
    sql = """
        INSERT INTO events (event_name, event_date, location, event_type, description, ticket_link, event_picture)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, event_data)
    cursor.connection.commit()

# Function to scrape a specific page and process events
def scrape_page(page_number, cursor):
    print(f"Processing page {page_number}")
    page_content = fetch_page_content(page_number)
    if not page_content:
        return False

    soup = BeautifulSoup(page_content, 'html.parser')
    agenda_items = soup.find_all('div', class_='c-agenda-item') 
    if not agenda_items:
        print(f"No agenda items found on page {page_number}")
        return False

    for item in agenda_items:
        try:
            # Extract event data
            name, date, location, ticket_link, event_picture = extract_event_data(item)

            # Prepare the data for insertion
            event_data = (
                name, date, location, "Music", None, ticket_link, event_picture
            )

            # Insert the event into the database
            insert_event(cursor, event_data)
            print(f"Inserted: {name} ({date}) at {location} with image {event_picture}")
        except Exception as error:
            print(f"Error processing item: {error}")
    
    return True

# Main function to manage the scraping process
def main(max_pages= 1):
    try:
        # Database connection
        db = pymysql.connect(**db_config)
        cursor = db.cursor()

        page_number = 1
        while page_number <= max_pages:
            print(f"Scraping page {page_number} of {max_pages}...")
            if not scrape_page(page_number, cursor):  # If no content, stop scraping
                print(f"Finished scraping at page {page_number}. No more content to fetch.")
                break
            page_number += 1  # Increment the page number after successful scraping

        cursor.close()
        db.close()
        print("Scraping completed.")
    except Exception as error:
        print(f"Database connection error: {error}")

if __name__ == "__main__":
    main(max_pages=5)  # Change the number to any desired limit
