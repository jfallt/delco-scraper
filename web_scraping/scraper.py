import csv
from datetime import date
import datetime
from fake_useragent import UserAgent
import glob
import os
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import Select
from string import ascii_lowercase
import sys
import timeit


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
    DRIVER_PATH = '/usr/local/share/chromedriver'
    return webdriver.Chrome(options=options, executable_path=DRIVER_PATH)


def db_upload():
    # Get all .csvs from data folder
    path = './data/'
    extension = 'csv'
    os.chdir(path)
    result = glob.glob('*.{}'.format(extension))

    conn = psycopg2.connect(
        user='postgres', password='password', host='localhost', database='delco_real_estate_sales')
    cur = conn.cursor()
    truncate = 'TRUNCATE TABLE sales_upload'
    cur.execute(truncate)
    for file in result:

        copy_sql = """
            COPY sales_upload FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        with open(file, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()

    populate_queries = ['call sp_populate_dim_parcel();', 'call sp_populate_fact_sales();',
                        'call sp_populate_sales_upload_amount_errors();']
    [cur.execute(query) for query in populate_queries]
    conn.commit()

    cur.close()
    print('Uploaded to database!\n')


def sales_history_by_date_range(street_name, start_date=(date.today() - datetime.timedelta(days=365)), end_date=date.today()):
    """
    Use the advanced search function of the http://delcorealestate.co.delaware.pa.us/ with the below inputs:

    Args:
        street_name (str): input street name.
        start_date (YYYY-MM-DD, optional): default is minus one year from today
        end_date (YYYY-MM-DD, optional): default is today
    """

    # Print date range if one isn't specified and convert to datetime object for later use if specified
    if start_date == date.today() - datetime.timedelta(days=365):
        print('Using default date range, no dates specified in input \nStart Date: {start_date}'.format(start_date=start_date))
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

    # Navigate to search page
    driver = get_selenium_res()
    driver.get(url_home)
    driver.get(url_search)

    # Trying to go to the url_search page results in a disclaimer page, click agree
    driver.find_element_by_id('btAgree').click()

    # Select results size
    driver.find_element_by_id('selPageSize')
    result_size = Select(driver.find_element_by_id('selPageSize'))
    result_size.select_by_visible_text('50')

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

    # Submit selections
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

    print('Found {row_count} rows\nParsing...'.format(row_count=rows-row_start))

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

    # Send results to .csv
    list_to_csv(os.path.join('.','data', 'data.csv'),
                fields, data[4:])
    print('Writing results to csv\n')

    # Upload to database
    db_upload()


def sales_history_by_year_batch(year_start, year_end):
    """
    Runs sales_history_by_date_range for the specified year range
    """
    start = timeit.default_timer()

    i = year_end
    while i >= year_start:
        for letter in ascii_lowercase:
            try:
                sales_history_by_date_range(
                    letter, '{year}-01-01'.format(year=i), '{year}-12-31'.format(year=i))
                db_upload()
            # Occasionally no results will be found (streets starting with X is often a culprit)
            except:
                print('No results for streets starting with ' + letter+'\n')
        i -= 1

    stop = timeit.default_timer()

    runtime = stop - start
    print('Total runtime {runtime} seconds'.format(
        runtime=runtime))

def delco_scraper_parcel(parcel_id):
    """
    Use the parcel id to search parcel details on http://delcorealestate.co.delaware.pa.us/
    """

    # Define table fields and format
    fields = ['ParcelID', 'TaxMapID', 'OwnerName', 'PropertyAddress',
              'SalesDate', 'SalesAmount', 'LandUseDescription']
    date_format = '%m/%d/%Y'

    # Required urls
    url_home = 'http://delcorealestate.co.delaware.pa.us'
    url_search = 'http://delcorealestate.co.delaware.pa.us/pt/search/commonsearch.aspx?mode=parid'

    # Navigate to search page
    driver = get_selenium_res()
    driver.get(url_home)
    driver.get(url_search)

    # Trying to go to the url_search page results in a disclaimer page, click agree
    driver.find_element_by_id('btAgree').click()
    
    # Submit parcel id
    driver.find_element_by_id('inpParid').send_keys(parcel_id)
    driver.find_element_by_id('btSearch').click()

    # Scrape data
    data_headers = driver.find_elements_by_class_name('DataletSideHeading')
    data = driver.find_elements_by_class_name('DataletData')
    
    data_list = []
    data_headers_list = []
    [data_list.append(entry.text) for entry in data]
    [data_headers_list.append(entry.text) for entry in data_headers]
    zipped_list = zip(data_headers_list, data_list)
    parcel_details = list(zipped_list)
    print('Complete!')
    driver.quit()