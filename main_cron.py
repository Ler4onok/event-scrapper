import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.parser import parse
import locale
import json
import logging
from sqlalchemy import create_engine, MetaData, Table, select
import re
import unicodedata

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("main.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

COMMA = ','
TODAY = 'Today'
SPACE = ' '
DASH = ' - '
COLON = ':'
DOT_SEPARATOR = ' · '
EQUALS = '='
FREE = 'Free'

DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
DF_COLUMNS = ["title", "slug", "start_date", "end_date", "categories", "location", 'address', 'description', 'short_description', 'source_link', 'source_id', 'image_link', 'organizer', 'price', 'last_inserted_at', 'last_updated_at']
EMPTY_FIELDS = ['organizer', 'price', 'short_description']

EVENTS_MADEIRA_URL = 'https://eventsmadeira.com/en/event-listing/'
EVENTS_MADEIRA_URL_PT = 'https://eventsmadeira.com/lista-de-eventos/'
EVENTBRITE_URL = 'https://www.eventbrite.com/d/portugal--ilha-da-madeira--85687345/all-events/'

EVENTS_MADEIRA_ID = 1

EVENTBRITE_ID = 2
EMPTY_FIELD = None

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore").decode("utf-8")  # remove accents
    text = re.sub(r"[^\w\s-]", "", text.lower())           # remove non-word chars
    text = re.sub(r"[\s_-]+", "-", text)                   # collapse whitespace and underscores into -
    text = re.sub(r"^-+|-+$", "", text)                    # trim leading/trailing hyphens
    return text

# get text from li elems and remove duplicities (mainly for gt-inner ul which could contain duplicities)
def get_data_text(result):
    list_items_text = [li.text for li in result.find_all('li')]
    if list_items_text and len(list_items_text) > 1:
        # remove duplicities
        return ', '.join(list(set(list_items_text)))
    return result.text

# fill data dict with date and time in datetime format
def fill_datetime(info_container, data):
    datetime_labels = [('start_date', 'gt-start-date'), ('end_date', 'gt-end-date')]
    for date_label, class_name in datetime_labels:
        datetime_scrapped = info_container.find('li', {'class': class_name})

        if datetime_scrapped is not None:
            datetime_ = datetime_scrapped.find('div', {'class': 'gt-inner'}).text
            datetime_obj = datetime.strptime(datetime_, '%d/%m/%Y %H:%M') if len(datetime_.split(' ')) > 0 else datetime.strptime(datetime_, '%d/%m/%Y')

            data[date_label].append(datetime_obj)

        else:
            data[date_label].append(EMPTY_FIELD)

# fill data dict with text from p tags from event detail page
def fill_description(page_content, data):
    description = page_content.find('div', {'class': 'gt-content'})
    data['description'].append(' '.join([p.text for p in description]).replace('\n', '') if description else EMPTY_FIELD)

# fill data dict with data from event detail page
def fill_event_data(url, data):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # get parts of page
    title_bar = soup.find('div', {'class': 'gt-page-title-bar'})
    page_content = soup.find('div', {'class': 'gt-page-content'})
    info_container = soup.find('div', {'class': 'gt-content-detail-box'})

    if not info_container or not page_content:
        return None
    else:
        fill_datetime(info_container, data)
        fill_description(page_content, data)
        
        print(title_bar.find('h1').text if title_bar else EMPTY_FIELD)
        title = title_bar.find('h1').text if title_bar else EMPTY_FIELD
        slug = slugify(title) if title else EMPTY_FIELD
        address = info_container.find('li', {'class': 'gt-address'}).find('div', {'class': 'gt-inner'}).text if info_container.find('li', {'class': 'gt-address'}) else EMPTY_FIELD
        location = get_data_text(info_container.find('li', {'class': 'gt-locations'}).find('div', {'class': 'gt-inner'})) if info_container.find('li', {'class': 'gt-locations'}) else EMPTY_FIELD
        categories = get_data_text(info_container.find('li', {'class': 'gt-categories'}).find('div', {'class': 'gt-inner'})) if info_container.find('li', {'class': 'gt-categories'}) else EMPTY_FIELD
        imageLink = page_content.find('div', {'class': 'gt-image'}).find('img')['data-src'] if page_content.find('div', {'class': 'gt-image'}) else EMPTY_FIELD

        print(f'Madeira Event title: {title} and slug: {slug}')
        data['title'].append(title)
        data['slug'].append(slug)
        data['address'].append(address)
        data['location'].append(location)
        data['categories'].append(categories)
        data['image_link'].append(imageLink)

        data['source_link'].append(url)
        data['source_id'].append(EVENTS_MADEIRA_ID)
        data['last_inserted_at'].append(datetime.now())
        data['last_updated_at'].append(datetime.now())


        for field in EMPTY_FIELDS:
            data[field].append(EMPTY_FIELD)


def get_price(soup):
    # Find all script tags and then find the one containing '__SERVER_DATA__'
    script_tags = soup.find_all('script')
    script_tag = None
    for tag in script_tags:
        if '__SERVER_DATA__' in tag.text:
            script_tag = tag
            break
    if script_tag:
        # Extract the JSON string
        json_str = script_tag.string.split(EQUALS, 1)[1].strip()
        # Load the JSON string into a Python dictionary

        data = json.loads(json_str[:-1])
        is_free = data['event_listing_response']['tickets']['ticketClasses'][0]['characteristics']['isFree']
        if is_free:
            minimum_ticket_price_rounded = FREE
        else:
            # Access the minimumTicketPriceRounded value
            minimum_ticket_price_rounded = data['event_listing_response']['tickets']['ticketClasses'][0]['cost']['display']
    else:
        minimum_ticket_price_rounded = None

    return minimum_ticket_price_rounded

def get_category(soup):
    # Find all script tags and then find the one containing '__SERVER_DATA__'
    script_tags = soup.find_all('script')
    script_tag = None
    for tag in script_tags:
        if '__SERVER_DATA__' in tag.text:
            script_tag = tag
            break
    if script_tag:
        # Extract the JSON string
        json_str = script_tag.string.split('=', 1)[1].strip()
        cleaned_json_str = json_str.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')

        # Load the JSON string into a Python dictionary
        data = json.loads(cleaned_json_str[:-1])
        category = data['event']['category']
    return category or 'Other'

def parse_weekday(date):
    weekday = date.split(SPACE)[0]
    time = date.split(SPACE)[2]
    hour = int(time.split(COLON)[0])
    minute = int(time.split(COLON)[1])

    now = datetime.now()

    target_day = DAYS.index(weekday)

    days_to_next_target_day = (target_day - now.weekday() + 7) % 7
    next_target_day = now + timedelta(days=days_to_next_target_day)

    next_target_day_at_specific_time = next_target_day.replace(hour=hour, minute=minute, second=0, microsecond=0)

    return next_target_day_at_specific_time

def parse_date(date):
    if COMMA in date:
        date_object = datetime.strptime(date, "%a, %b %d, %I:%M %p").replace(year=datetime.now().year)
    else:
        if TODAY in date:
            hour = int(date.split(SPACE)[2].split(COLON)[0])
            minute = int(date.split(SPACE)[2].split(COLON)[1])
            date_object = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        else:
            date_object = parse_weekday(date)

    return date_object

def parse_address(address):
    location = address.find('p', class_='location-info__address-text')
    event_location = location.text
    # remove button and location elements from div with address
    address.find('div', class_='map-button-toggle').decompose()
    location.decompose()
    event_address = address.text.strip()
    return event_address, event_location

def parse_event_date(event_date):
    if 'Starts on' in event_date:
        event_date = event_date.replace('Starts on ', '')
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
    if DOT_SEPARATOR in event_date:
        event_date_first_part = event_date.split(DOT_SEPARATOR)[0]
        event_date_second_part = event_date.split(DOT_SEPARATOR)[1]
        # format Saturday, August 3 · 6 - 11:59pm WEST
        if COMMA in event_date.split(DOT_SEPARATOR)[0]:
            event_month = event_date_first_part.split(SPACE)[1]
            event_day = event_date_first_part.split(SPACE)[2]
            event_month_end = event_date_first_part.split(SPACE)[1]
            event_day_end = event_date_first_part.split(SPACE)[2]
            event_start_time = event_date_second_part.split(DASH)[0] if DASH in event_date_second_part else event_date_second_part.split(SPACE)[0]
            event_end_time = event_date_second_part.split(DASH)[1].split(SPACE)[0] if DASH in event_date_second_part else event_date_second_part.split(SPACE)[0]
        # format August 20 · 7pm - August 21 · 2am WEST
        else:
            event_date_third_part = event_date.split(DOT_SEPARATOR)[2]
            event_month = event_date_first_part.split(SPACE)[0]
            event_day = event_date_first_part.split(SPACE)[1]
            event_month_end = event_date_second_part.split(DASH)[1].split(SPACE)[0]
            event_day_end = event_date_second_part.split(DASH)[1].split(SPACE)[1]
            event_start_time = event_date_second_part.split(DASH)[0]
            event_end_time = event_date_third_part.split(SPACE)[0]

        time_postfix = None

        if 'am' in event_start_time or 'pm' in event_start_time:
            time_postfix = event_start_time[-2:]
            event_start_time = event_start_time[:-2]

        if 'am' in event_end_time or 'pm' in event_end_time:
            time_postfix = event_end_time[-2:]
            event_end_time = event_end_time[:-2]

        dt_string = event_month + SPACE + event_day + SPACE + str(datetime.now().year) + SPACE + event_start_time + SPACE + (time_postfix.upper() if time_postfix else "PM")
        dt_string_end = event_month_end + SPACE + event_day_end + SPACE + str(datetime.now().year) + SPACE + event_end_time + SPACE + (time_postfix.upper() if time_postfix else "PM")
        if COLON in event_start_time:
            date_object = datetime.strptime(dt_string, '%B %d %Y %I:%M %p')
        else:
            date_object = datetime.strptime(dt_string, '%B %d %Y %I %p')

        if COLON in event_end_time:
            date_object_end = datetime.strptime(dt_string_end, '%B %d %Y %I:%M %p')
        else:
            date_object_end = datetime.strptime(dt_string_end, '%B %d %Y %I %p')
    else:
        try:
            date_object = datetime.strptime(event_date.split(DASH)[0], "%a, %d %b %Y %H:%M")
            if len(event_date.split(DASH)[1]) < 12:
                time = event_date.split(DASH)[1].split(SPACE)[0] + ':00'
                date_object = date_object.replace(hour=int(time.split(':')[0]), minute=int(time.split(':')[1]), second=0, microsecond=0)
                date_object_end = date_object
            else:
                date_object_end = datetime.strptime(event_date.split(DASH)[1][:-5], "%a, %d %b %Y %H:%M") if 'WEST' in event_date else datetime.strptime(event_date.split(DASH)[1][:-4], "%a, %d %b %Y %H:%M") if 'WET' in event_date or "EDT" in event_date else datetime.strptime(event_date.split(DASH)[1], "%a, %d %b %Y %H:%M")
        except:
            # if date is in portuguese
            locale.setlocale(locale.LC_TIME, 'pt_PT.UTF-8')
            date_format = '%a, %d de %b de %Y %H:%M'
            if DASH in event_date:
                date_object = datetime.strptime(event_date.split(DASH)[0], date_format)
                if len(event_date.split(DASH)[1]) < 12:
                    time = event_date.split(DASH)[1].split(SPACE)[0] + ':00'
                    date_object = date_object.replace(hour=int(time.split(':')[0]), minute=int(time.split(':')[1]), second=0, microsecond=0)
                    date_object_end = date_object
                else:
                    date_object_end = datetime.strptime(event_date.split(DASH)[1][:-5], date_format) if 'WEST' in event_date else datetime.strptime(event_date.split(DASH)[1], date_format)
            else:
                date_object = datetime.strptime(event_date, date_format)
                date_object_end = date_object

    return date_object, date_object_end

# print(parse_event_date('August 1 · 7pm - August 2 · 1am WEST'))
# print(parse_event_date('sáb, 10 de ago de 2024 22:00 - dom, 11 de ago de 2024 03:00 WEST'))
# print(parse_event_date('Saturday, August 3 · 6 - 11:59pm WEST'))
# print(parse_event_date('August 20 · 7pm - August 21 · 2am WEST'))
# print(parse_event_date('Fri, 29 Nov 2024 21:00 - Mon, 2 Dec 2024 23:30 WET'))
# print(parse_event_date('Thursday, August 15 · 6:30 - 9:30pm EDT'))
# print(parse_event_date('Tue, 6 Aug 2024 12:30 - 13:30 WEST'))
# print(parse_event_date('sex, 30 de ago de 2024 11:00 - 12:35 WEST'))
# print(parse_event_date('Starts on Saturday, August 31 · 10:30pm WEST'))
logger.info('Starting scraping')

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
try:
    logger.info('Starting to fetch Madeira events EN')
    response_madeira_events = requests.get(EVENTS_MADEIRA_URL, headers=headers)
    soup_madeira_events = BeautifulSoup(response_madeira_events.text, 'html.parser')
    divs = soup_madeira_events.find('div', class_='gt-event-listing').find_all('div', class_='gt-event-style-3') if soup.find('div', class_='gt-event-listing') else KeyError
except: 
    logger.info('EN failed. Starting to fetch Madeira events PT')
    response_madeira_events = requests.get(EVENTS_MADEIRA_URL_PT, headers=headers)
    soup_madeira_events = BeautifulSoup(response_madeira_events.text, 'html.parser')
    divs = soup_madeira_events.find('div', class_='gt-event-listing').find_all('div', class_='gt-event-style-3')

logger.info('Fetched Madeira events')

response_eventbrite = requests.get(EVENTBRITE_URL, headers=headers)
logger.info('Fetched Eventbrite events')

# soup_madeira_events = BeautifulSoup(response_madeira_events.text, 'html.parser')
# divs = soup_madeira_events.find('div', class_='gt-event-listing').find_all('div', class_='gt-event-style-3')
# logger.info('Parsed Madeira Events events')

soup_eventbrite = BeautifulSoup(response_eventbrite.text, 'html.parser')
events = soup_eventbrite.find_all('div', class_='discover-search-desktop-card discover-search-desktop-card--hiddeable')
logger.info('Parsed Eventbrite events')

data_madeira_events = {field: [] for field in DF_COLUMNS}

for div in divs:
    try:
        url = div.find('div', class_='gt-title').find('a')['href']
        result = fill_event_data(url, data_madeira_events)
        # logger.info(f'Scraped Madeira event: {data_madeira_events["title"][-1]}')
    except Exception as e:
        logger.error(f'Error scraping Madeira event: {e}')
        continue

data = {column: [] for column in DF_COLUMNS}
event_links = []

for event in events:
   try:
        event_card = event.find('a', class_='event-card-link')
        event_link = event_card.get('href')
        event_links.append(event_link)
   except Exception as e:
        logger.error(f'Error processing event: {e}')
        continue

logger.info('Scraped Eventbrite event links')

for link in event_links:
    try:
        res = requests.get(link, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')

        event_title = soup.find('h1', class_='event-title').text
        event_slug = slugify(event_title) if event_title else EMPTY_FIELD
        event_price = soup.find('div', class_='conversion-bar__panel-info').text if soup.find('div', class_='conversion-bar__panel-info') else get_price(soup)

        address = soup.find('div', class_='location-info__address')
        event_address, event_location = parse_address(address)

        date = soup.find('span', class_='date-info__full-datetime').text
        event_start_date, event_end_date = parse_event_date(date)

        event_description = str(soup.find('div', class_='event-description__content'))
        event_short_description = None

        event_image_link = soup.find('div', class_='event-hero').find('picture').find('img').get('src')

        event_organizer = soup.find('a', class_='descriptive-organizer-info-mobile__name-link').text if soup.find('a', class_='descriptive-organizer-info-mobile__name-link') else None
        event_source_link = link
        event_source_id = EVENTBRITE_ID

        event_last_inserted_at = datetime.now()
        event_last_updated_at = datetime.now()

        event_categories = get_category(soup)

        data['title'].append(event_title)
        data['slug'].append(event_slug)
        data['address'].append(event_address)
        data['location'].append(event_location)
        data['price'].append(event_price)
        data['start_date'].append(event_start_date)
        data['organizer'].append(event_organizer)

        data['end_date'].append(event_end_date)
        data['description'].append(event_description)
        data['short_description'].append(event_short_description)
        data['categories'].append(event_categories)
        data['image_link'].append(event_image_link)

        data['source_link'].append(event_source_link)
        data['source_id'].append(event_source_id)
        data['last_inserted_at'].append(event_last_inserted_at)
        data['last_updated_at'].append(event_last_updated_at)
        logger.info(f'Scraped Eventbrite event: {event_title} with slug {event_slug}')
    except Exception as e:
        logger.error(f'Error scraping Eventbrite event: {e}')
        continue

try:
    df_madeira_events = pd.DataFrame(data_madeira_events)
    df_madeira_events = df_madeira_events.reset_index().rename(columns={'index': 'id'})

    logger.info('Created Madeira Events DataFrame')
except Exception as e:
    logger.error(f'Error creating Madeira Events DataFrame: {e}')

try:
    df_eventbrite = pd.DataFrame(data)
    df_eventbrite.index = pd.RangeIndex(start=200, stop=200 + len(df_eventbrite))
    df_eventbrite = df_eventbrite.reset_index().rename(columns={'index': 'id'})

    logger.info('Created Eventbrite DataFrame')
except Exception as e:
    logger.error(f'Error creating Eventbrite DataFrame: {e}')


try:
    df = pd.concat([df_madeira_events, df_eventbrite])
    df.reset_index().rename(columns={'index': 'id'})
    logger.info('Concatenated DataFrames')
except Exception as e:
    logger.error(f'Error concatenating DataFrames: {e}')

try:
    alchemyEngine = create_engine('postgresql://default:qml3K7QroCMw@ep-round-field-a2kcdmjb-pooler.eu-central-1.aws.neon.tech:5432/verceldb?sslmode=require', pool_recycle=3600)
    dbConnection = alchemyEngine.connect()
    metadata = MetaData()
    logger.info('Connected to database')
except Exception as e:
    logger.error(f'Error connecting to database: {e}')


try:
    table = Table('category', metadata, autoload_with=alchemyEngine)
    result = dbConnection.execute(select(table))

    categories_df = pd.DataFrame(result, columns=table.columns.keys())

    logger.info('Fetched categories from database')
except Exception as e:
    logger.error(f'Error fetching categories from database: {e}')

try:
    event_to_category_df = pd.DataFrame(columns=['event_id', 'category_id'])
    data = []
    for index, row in df.iterrows():
        categories = row['categories'].split(', ')
        id = row['id']
        for category in categories:
            category_id = categories_df[categories_df['title'] == category]['id'].values
            if len(category_id) > 0:
                data.append({'event_id': id, 'category_id': int(category_id[0])})

    event_to_category_df = pd.DataFrame(data)
    event_to_category_df['category_id'] = event_to_category_df['category_id'].astype('Int64')

    logger.info('Created Event to Category DataFrame')
except Exception as e:
    logger.error(f'Error creating Event to Category DataFrame: {e}')

try:
    event_to_category_table = Table('event_to_category', metadata, autoload_with=alchemyEngine)
    logger.info('Fetched event_to_category table from database')
except Exception as e:
    logger.error(f'Error fetching event_to_category table from database: {e}')

try:
    event_table = Table('event', metadata, autoload_with=alchemyEngine)
    logger.info('Fetched event table from database')
except Exception as e:
    logger.error(f'Error fetching event table from database: {e}')

try:
    if event_to_category_df.shape[0] > 0:
        dbConnection.execute(event_to_category_table.delete())
        logger.info('Deleted all rows from event_to_category table')
except Exception as e:
    logger.error(f'Error deleting all rows from event_to_category table: {e}')

try:
    df_without_categories = df.drop(columns=['categories'])
    if df_without_categories.shape[0] > 0:
        dbConnection.execute(event_table.delete())
        logger.info('Deleted all rows from event table')
except Exception as e:
    logger.error(f'Error deleting all rows from event table: {e}')

try:
    dbConnection.commit()
    logger.info('Committed transaction')
except Exception as e:
    logger.error(f'Error committing transaction: {e}')

try:
    df_without_categories.to_sql('event', dbConnection, if_exists='append', index=False)
    logger.info('Inserted events into database')
except Exception as e:
    logger.error(f'Error inserting events into database: {e}')

try:
    event_to_category_df.to_sql('event_to_category', dbConnection, if_exists='append', index=False)
    logger.info('Inserted event to category into database')
except Exception as e:
    logger.error(f'Error inserting event to category into database: {e}')

try:
    dbConnection.close()
    logger.info('Closed database connection')
except Exception as e:
    logger.error(f'Error closing database connection: {e}')

try:
    requests.post('https://api.vercel.com/v1/integrations/deploy/prj_xPf9ey4sxhpr8XybMsMSed2Svl9I/u8ohSqWrYJ')
    logger.info('Redeploy triggered')
except:
    logger.error(f'Error while redeploying: {e}')

logger.info('Finished scraping')
