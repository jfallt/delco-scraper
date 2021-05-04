import csv
from database_setup import create_connection
from datetime import date
import datetime
from fake_useragent import UserAgent
from functools import wraps, partial
import glob
import json
from multiprocessing import Pool, Queue
import os
import psycopg2
from queries import insert_residential_details, insert_parcel_site_details, get_missing_data
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from string import ascii_lowercase
import sys
import timeit
import yaml


# Read config
with open('config.yaml', 'r') as stream:
    data_loaded = yaml.safe_load(stream)


class CustomError(Exception):
    pass


def list_to_csv(file_name, fields, values):
    """
    Send results of table to csv
    """
    with open(file_name, 'w') as f:

        # using csv.writer method from CSV package
        write = csv.writer(f)

        write.writerow(fields)
        write.writerows(values)


def get_selenium_res():
    """
    Randomize user agent for selenium and initiate a driver
    """
    # Create random user agent
    ua = UserAgent()
    user_agent = ua.random

    # Select options for selenium
    options = Options()
    options.headless = True
    options.add_argument('--window-size=1920,1200')
    options.add_argument(
        'user-agent={user_agent}'.format(user_agent=user_agent))

    # Add driver path and create driver
    DRIVER_PATH = data_loaded.get('DRIVER_PATH')
    return webdriver.Chrome(options=options, executable_path=DRIVER_PATH)


def bulk_csv_upload(upload_type):
    populate_queries = ['call sp_populate_dim_parcel();', 'call sp_populate_fact_sales();',
                        'call sp_populate_sales_upload_amount_errors();']
    truncate = 'TRUNCATE TABLE sales_upload'
    table = 'sales_upload'

    # Get file from data folder
    path = './data'
    extension = 'csv'
    file = os.path.join(path, '{}.{}'.format(upload_type, extension))

    conn = create_connection()
    cur = conn.cursor()
    cur.execute(truncate)

    copy_sql = """
        COPY {table} FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """.format(table=table)
    with open(file, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()

    [cur.execute(query) for query in populate_queries]
    conn.commit()
    cur.close()


def get_sales_history_by_date_range(street_name, start_date=(date.today() - datetime.timedelta(days=365)), end_date=date.today()):
    """
    Use the advanced search function of the http://delcorealestate.co.delaware.pa.us/ with the below inputs:

    Args:
        street_name (str): input street name.
        start_date (YYYY-MM-DD, optional): default is minus one year from today
        end_date (YYYY-MM-DD, optional): default is today
    """

    # Print date range if one isn't specified and convert to datetime object for later use if specified
    if start_date == date.today() - datetime.timedelta(days=365):
        print('Using default date range, no dates specified in input \nStart Date: {start_date}'.format(
            start_date=start_date))
    else:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date == date.today():
        print('End Date: {end_date}'.format(end_date=end_date))
    else:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

    # Define table fields and format
    fields = ['ParcelID', 'TaxMapID', 'OwnerName', 'PropertyAddress',
              'SalesDate', 'SalesAmount', 'LandUseDescription']
    date_format = '%m/%d/%Y'

    # Required urls
    url_home = 'http://delcorealestate.co.delaware.pa.us'
    url_search = 'http://delcorealestate.co.delaware.pa.us/pt/search/advancedsearch.aspx?mode=advanced'

    # Navigate to search page, click agree on disclaimer page
    driver = get_selenium_res()
    driver.get(url_home)
    driver.get(url_search)
    driver.find_element_by_id('btAgree').click()

    # Select search by street name, input parameters
    street_selection = Select(driver.find_element_by_id('sCriteria'))
    street_selection.select_by_visible_text(
        'Street Name (i.e. Main NOT Main St)')
    driver.find_element_by_id('txtCrit').send_keys(street_name)
    driver.find_element_by_id('btAdd').click()

    # Select search by sales date range, input start and end date
    sales_date_selection = Select(driver.find_element_by_id('sCriteria'))
    sales_date_selection.select_by_visible_text('Sales Date')
    driver.find_element_by_id('ctl01_cal1_dateInput').send_keys(
        start_date.strftime(date_format))
    driver.find_element_by_id('ctl01_cal2_dateInput').send_keys(
        end_date.strftime(date_format))
    driver.find_element_by_id('btAdd').click()
    driver.find_element_by_id('btSearch').click()
    print('Searching http://delcorealestate.co.delaware.pa.us/ for streets starting with {street_name}...'.format(
        street_name=street_name))

    # Find print preview and change to print preview window
    driver.find_element_by_id('Span1').click()
    child = driver.window_handles[-1]
    driver.switch_to.window(child)

    # Get row count of table, specify where data starts
    search_results = driver.find_elements_by_class_name('SearchResults')
    rows = len(search_results)
    row_start = 5

    print('Found {row_count} rows\nParsing...'.format(
        row_count=rows-row_start))

    # Grab results from table and insert into list
    data = []
    table_xpath = '//*[@id="Form1"]/table/tbody/tr/td/center/table/tbody/tr'

    def parse_table(table):
        """
        Get all entries from table in print preview
        """
        result = []
        [result.append(item.text) for item in table.find_elements_by_xpath(
            ".//*[self::td]") if item.text != 'Map Link']
        return result

    # Append results to data
    [data.append(parse_table(table))
     for table in driver.find_elements_by_xpath(table_xpath)]

    driver.quit()

    # Send results to .csv and upload to database
    list_to_csv(os.path.join('.', 'data', 'sales_history.csv'),
                fields, data[4:])
    print('Writing results to csv\n')

    bulk_csv_upload('sales_upload')


def sales_history_by_year_batch(year_start: int, year_end: int):
    """
    Runs get_sales_history_by_date_range for the specified year range
    """
    # Input validation
    if year_start > year_end:
        raise CustomError("End year is greater than starting year")
    if type(year_start) != int or type(year_end) != int:
        raise TypeError("Only integers allowed")

    start = timeit.default_timer()

    i = year_end
    while i >= year_start:
        for letter in ascii_lowercase:
            try:
                get_sales_history_by_date_range(
                    letter, '{year}-01-01'.format(year=i), '{year}-12-31'.format(year=i))
                bulk_csv_upload()
            # Occasionally no results will be found (streets starting with X are often a culprit)
            except:
                print('No results for streets starting with ' + letter+'\n')
        i -= 1

    stop = timeit.default_timer()

    runtime = stop - start
    print('Total runtime {runtime} seconds'.format(
        runtime=runtime))


def get_parcels_without_detail_data(missing_data_query):
    """
    Query database and find all parcels without detail entries

    Args:
        missing_data_query (either residential or parcel details)
    """
    query = get_missing_data[missing_data_query]
    conn = None
    try:
        conn = create_connection()
        cur = conn.cursor()
        cur.execute(query)
        records = [r[0] for r in cur.fetchall()]
        conn.commit()
        cur.close()
        return records
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def datalet_table_scrape(driver, parcel_id):
    """
    Scrape datalet table. The "headers" are on the side of the data and the function
    takes headers and stitches it together in dictionary for upload

    Args:
        driver: selenium driver
        parcel_id: current parcel searched
    """
    # Find all datalet headers and data elements
    data_headers = driver.find_elements_by_class_name('DataletSideHeading')
    data = driver.find_elements_by_class_name('DataletData')

    # Initialize empty list to store results and add data
    data_list = []
    data_headers_list = []
    [data_list.append(entry.text) for entry in data]
    [data_headers_list.append(entry.text) for entry in data_headers]

    # Combine lists (this is necessary so we can drop empty header entries)
    zipped_list = zip(data_headers_list, data_list)
    data_dict = dict(zipped_list)
    data_dict['parcel_id'] = parcel_id
    return data_dict


def residential_details_upload(data_dict, exclude):
    """
    Upload residential details to database

    Args:
        data_dict: a dictionary required for upload
        exclude: Some entries do not have residential details, this allows us to bypass the scraping
    """
    # Not every entry has residential detail data if it does not insert dummy data into the database to stop from searching again
    if (not data_dict) or list(data_dict.keys())[0] != exclude:
        conn = create_connection()
        cur = conn.cursor()
        truncate = 'TRUNCATE TABLE stg_parcel_residential_details'
        cur.execute(truncate)
        insert = """INSERT INTO stg_parcel_residential_details 
                (parcel_id) 
                VALUES ('{parcel_id}')""".format(parcel_id=data_dict['parcel_id'])
        cur.execute(insert)
        populate_queries = [
            'call sp_populate_dim_parcel_residential_details();']
        [cur.execute(query) for query in populate_queries]
        conn.commit()
        print('No data available for parcel {parcel_id}'.format(
            parcel_id=data_dict['parcel_id']))
    else:
        try:
            conn = create_connection()
            cur = conn.cursor()
            truncate = 'TRUNCATE TABLE stg_parcel_residential_details'
            cur.execute(truncate)
            insert = insert_residential_details.format(
                parcel_id=data_dict['parcel_id'],
                card=data_dict['Card'],
                class_input=data_dict['Class'],
                grade=data_dict['Grade'],
                cdu=data_dict['CDU'],
                style=data_dict['Style'],
                acres=data_dict['Acres'],
                year_built_effective_year=data_dict['Year Built / Effective Year'],
                remodeled_year=data_dict['Remodeled Year'],
                base_area=data_dict['Base Area'],
                finished_bsmt_area=data_dict['Finished Bsmt Area'],
                number_of_stories=data_dict['Number of Stories'],
                exterior_wall=data_dict['Exterior Wall'],
                basement=data_dict['Basement'],
                physical_condition=data_dict['Physical Condition'],
                heating=data_dict['Heating'],
                heat_fuel_type=data_dict['Heating Fuel Type'],
                heating_system=data_dict['Heating System'],
                attic_code=data_dict['Attic Code'],
                fireplaces=data_dict['Fireplaces: 1 Story/2 Story'],
                parking=data_dict['Parking'],
                total_rooms=data_dict['Total Rooms'],
                full_baths=data_dict['Full Baths'],
                half_baths=data_dict['Half Baths'],
                total_fixtures=data_dict['Total Fixtures'],
                additional_fixtures=data_dict['Additional Fixtures'],
                bed_rooms=data_dict['Bed Rooms'],
                family_room=data_dict['Family Room'],
                living_units=data_dict['Living Units']
            )
            cur.execute(insert)
            populate_queries = [
                'call sp_populate_dim_parcel_residential_details();']
            [cur.execute(query) for query in populate_queries]
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


def search_by_parcel_id(parcel_id):
    """
    Search parcel details by id on http://delcorealestate.co.delaware.pa.us/
    """
    print('Searching for parcel {parcel_id} details...'.format(
        parcel_id=parcel_id))

    # Required urls and xpaths
    url_home = 'http://delcorealestate.co.delaware.pa.us'
    url_search = 'http://delcorealestate.co.delaware.pa.us/pt/search/commonsearch.aspx?mode=parid'
    search_results_xpath = '//*[@id="searchResults"]/tbody/tr[3]'

    # Navigate to search page, this results in a disclaimer page, click agree, then submit parcel id
    driver = get_selenium_res()
    driver.get(url_home)
    driver.get(url_search)
    driver.find_element_by_id('btAgree').click()
    driver.find_element_by_id('inpParid').send_keys(parcel_id)
    driver.find_element_by_id('btSearch').click()

    # Occasionally the parcel id will return multiple results, the characteristics are the same so grab only the first
    try:
        driver.find_element_by_xpath(search_results_xpath).click()
        print('Multiple parcel id results found, navigating to first entry')
    except:
        print('Single parcel result returned for ' + parcel_id)
    return driver


def residential_details_upload(data_dict, exclude):
    # Upload to database
    conn = create_connection()
    cur = conn.cursor()

    truncate = 'TRUNCATE TABLE stg_parcel_residential_details'
    cur.execute(truncate)

    insert = insert_residential_details.format(
        parcel_id=data_dict['parcel_id'],
        card=data_dict['Card'],
        class_input=data_dict['Class'],
        grade=data_dict['Grade'],
        cdu=data_dict['CDU'],
        style=data_dict['Style'],
        acres=data_dict['Acres'],
        year_built_effective_year=data_dict['Year Built / Effective Year'],
        remodeled_year=data_dict['Remodeled Year'],
        base_area=data_dict['Base Area'],
        finished_bsmt_area=data_dict['Finished Bsmt Area'],
        number_of_stories=data_dict['Number of Stories'],
        exterior_wall=data_dict['Exterior Wall'],
        basement=data_dict['Basement'],
        physical_condition=data_dict['Physical Condition'],
        heating=data_dict['Heating'],
        heat_fuel_type=data_dict['Heating Fuel Type'],
        heating_system=data_dict['Heating System'],
        attic_code=data_dict['Attic Code'],
        fireplaces=data_dict['Fireplaces: 1 Story/2 Story'],
        parking=data_dict['Parking'],
        total_rooms=data_dict['Total Rooms'],
        full_baths=data_dict['Full Baths'],
        half_baths=data_dict['Half Baths'],
        total_fixtures=data_dict['Total Fixtures'],
        additional_fixtures=data_dict['Additional Fixtures'],
        bed_rooms=data_dict['Bed Rooms'],
        family_room=data_dict['Family Room'],
        living_units=data_dict['Living Units']
    )
    cur.execute(insert)
    populate_queries = ['call sp_populate_dim_parcel_residential_details();']
    [cur.execute(query) for query in populate_queries]
    conn.commit()


def get_parcel_residential_details(parcel_id):
    """
    Scrape residential details from the results of the parcel id search
    """
    driver = search_by_parcel_id(parcel_id)
    residential_xpath = '//*[@id="sidemenu"]/li[2]/a/span'

    # Navigate to residential tab, scrape data, and upload
    # Finding the element by xpath is necessary here since the links do not have explicit names
    try:
        # The website performance can vary, waiting until the link for res details is clickable
        WebDriverWait(driver, 60).until(EC.element_to_be_clickable(
            (By.XPATH, residential_xpath))).click()
        data_dict = datalet_table_scrape(driver, parcel_id)
        residential_details_upload(data_dict, 'Total OBY Value')
        driver.quit()
        print('{parcel_id} Complete!'.format(parcel_id=parcel_id))
    except:
        print('Website not loading parcel {parcel_id}'.format(
            parcel_id=parcel_id))


def parcel_site_information_scraper(driver, parcel_id):
    """
    Scrape parcel site information from the results of the parcel id search
    """
    data = driver.find_elements_by_id('Parcel')
    data_list = []
    [data_list.append(entry.text) for entry in data]
    processed_list = data_list[0].replace('School District', 'School District:').replace('Homestead %', 'Homestead %:').replace(
        'Homestead Approved Year', 'Homestead Approved Year:').replace('\n', ':').split(':')
    stripped_list = [s.strip() for s in processed_list]

    parcel_detail_fields = [
        'Site Location',
        'Legal Description',
        'Map Number',
        'Municipality',
        'School District',
        'Property Type',
        'Homestead Status - Next School Bill Cycle',
        'Homestead Status - Current School Bill Cycle',
        'Homestead %',
        'Homestead Approved Year',
        'Additional Info',
        "Veteran's Exemption"
    ]

    parcel_detail_data = {}

    # Parse element response
    j = 0
    for i in range(len(parcel_detail_fields)-1):
        while stripped_list[j+1] not in parcel_detail_fields:
            if parcel_detail_fields[i] not in parcel_detail_data:
                parcel_detail_data[parcel_detail_fields[i]
                                   ] = stripped_list[j+1]
            else:
                parcel_detail_data[parcel_detail_fields[i]
                                   ] = parcel_detail_data[parcel_detail_fields[i]] + ' ' + stripped_list[j+1]
            j += 1
        i += 1
        j += 1

    table_id = driver.find_element(By.ID, 'Owner History')
    rows = table_id.find_elements(By.TAG_NAME, 'tr')
    sale_data = {'owner_history': []}

    for row in rows:
        elements = row.find_elements(By.TAG_NAME, 'td')[0:]
        if elements[0].text not in ['Owner', '']:
            owner = elements[0]
            sale_date = elements[-2]
            sale_price = elements[-1]

            dict_data = {
                'owner': owner.text,
                'sale_date': sale_date.text,
                'sale_price': sale_price.text
            }

            sale_data['owner_history'].append(dict_data)

    driver.close()
    return parcel_detail_data, sale_data


def parcel_site_information_upload(parcel_id, parcel_detail_data, sale_data):
    conn = create_connection()
    cur = conn.cursor()

    #'TRUNCATE TABLE stg_parcel_site_details',
    truncate = ['TRUNCATE TABLE stg_sale_history',
                'DELETE FROM stg_parcel_site_details sg WHERE parcel_id in (select parcel_id from dim_parcel where municipality is not null)']
    [cur.execute(query) for query in truncate]

    # Add to parcel details table
    insert = insert_parcel_site_details.format(
        parcel_id=parcel_id,
        site_location=parcel_detail_data['Site Location'].replace(
            "'", "''"),
        legal_description=parcel_detail_data['Legal Description'].replace(
            "'", "''"),
        municipality=parcel_detail_data['Municipality'],
        school_district=parcel_detail_data['School District'],
        property_type=parcel_detail_data['Property Type'],
    )
    cur.execute(insert)

    # Add to sales table
    [cur.execute("""INSERT INTO stg_sale_history(
        parcel_id,
        owner,
        sale_date,
        sale_price
        )
        VALUES (
            '{parcel_id}',
            '{owner}',
            '{sale_date}',
            '{sale_price}'
            )
        """.format(parcel_id=parcel_id,
                   owner=entry['owner'].replace("'", "''"),
                   sale_date=entry['sale_date'],
                   sale_price=entry['sale_price'])
                 ) for entry in sale_data['owner_history']]

    populate_queries = [
        'call sp_populate_fact_sales_history();'
    ]
    [cur.execute(query) for query in populate_queries]
    conn.commit()


def get_parcel_site_details(parcel_id):
    """
    Scrape residential details from the results of the parcel id search
    """
    driver = search_by_parcel_id(parcel_id)
    parcel_detail_data, sale_data = parcel_site_information_scraper(
        driver, parcel_id)
    parcel_site_information_upload(parcel_id, parcel_detail_data, sale_data)
    driver.quit()
    print('{parcel_id} Complete!'.format(parcel_id=parcel_id))


def parcel_detail_pooling_scraper(missing_data_query, detail_function):
    """
    Find and scrape all missing residential details, creating a multiprocess to do multiple searches at one time
    """
    parcel_ids = get_parcels_without_detail_data(
        missing_data_query)  # 'dim_parcel_residential_details'

    if len(parcel_ids) == 0:
        print('All parcel details have been scraped!')
    # Creating a multiprocess since we can only access details for one parcel at a time
    # TODO add a yaml config so the user can specify the number of pools depending on their specs
    q = Queue()
    p = Pool(data_loaded.get('pool'))
    results = p.imap(detail_function, parcel_ids)
    successful = []
    failure_tracker = []
    retry_results = []

    # In case a child process fails this loop tracks those and retries them
    # TODO add logging for parcel errors
    # not sure this is actually doing anything?
    while len(successful) < len(parcel_ids):
        successful.extend([r for r in results if not r is None])
        successful.extend([r for r in retry_results if not r is None])
        failed_items = []
        while not q.empty():
            failed_items.append(q.get())
        if failed_items:
            failure_tracker.append(failed_items)
            retry_results = p.imap(detail_function, parcel_ids)

    p.close()
    p.join()
    p.terminate()


if __name__ == '__main__':
    # get_parcel_site_details('49110311300')
    # parcel_detail_pooling_scraper()
    parcel_detail_pooling_scraper(
        'parcel_site_details', get_parcel_site_details)
