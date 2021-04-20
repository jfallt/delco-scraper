import csv
from database_setup import create_connection
from datetime import date
import datetime
from fake_useragent import UserAgent
from functools import wraps
import glob
from multiprocessing import Pool, Queue
import os
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import Select
from string import ascii_lowercase
import sys
import timeit


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
    DRIVER_PATH = '/usr/local/share/chromedriver'
    return webdriver.Chrome(options=options, executable_path=DRIVER_PATH)


def db_upload(upload_type):
    if upload_type == 'sales_history':
        populate_queries = ['call sp_populate_dim_parcel();', 'call sp_populate_fact_sales();',
                            'call sp_populate_sales_upload_amount_errors();']
        truncate = 'TRUNCATE TABLE sales_upload'
        table = 'sales_upload'
    elif upload_type == 'residential_details':
        populate_queries = ['call sp_populate_fact_parcel_details();']
        truncate = 'TRUNCATE TABLE stg_parcel_details'
        table = 'stg_parcel_details'

    # Get file from data folder
    
    path = './data'
    extension = 'csv'
    file = os.path.join(path,'{}.{}'.format(upload_type, extension))

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
    #print('Uploaded to database!\n')


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

    # Navigate to search page
    driver = get_selenium_res()
    driver.get(url_home)
    driver.get(url_search)

    # Trying to go to the url_search page results in a disclaimer page, click agree
    driver.find_element_by_id('btAgree').click()

    # Select results size
    #driver.find_element_by_id('selPageSize')
    #result_size = Select(driver.find_element_by_id('selPageSize'))
    #result_size.select_by_visible_text('50')

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

    # Send results to .csv
    list_to_csv(os.path.join('.', 'data', 'sales_history.csv'),
                fields, data[4:])
    print('Writing results to csv\n')

    db_upload('sales_upload')


def sales_history_by_year_batch(year_start: int, year_end: int):
    """
    Runs sales_history_by_date_range for the specified year range
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
                sales_history_by_date_range(
                    letter, '{year}-01-01'.format(year=i), '{year}-12-31'.format(year=i))
                db_upload()
            # Occasionally no results will be found (streets starting with X are often a culprit)
            except:
                print('No results for streets starting with ' + letter+'\n')
        i -= 1

    stop = timeit.default_timer()

    runtime = stop - start
    print('Total runtime {runtime} seconds'.format(
        runtime=runtime))


def get_parcels_without_details():
    """
    Query database and find all parcels without detail entries
    """
    query = (
        """
        SELECT P.PARCEL_ID
        FROM DIM_PARCEL P
        LEFT JOIN FACT_PARCEL_DETAILS FPD ON FPD.ID = P.ID
        GROUP BY P.PARCEL_ID
        HAVING COUNT(FPD.ID) = 0
        ORDER BY P.PARCEL_ID DESC
        """)
    conn = None
    try:
        conn = create_connection()
        cur = conn.cursor()
        cur.execute(query)
        records =  [r[0] for r in cur.fetchall()]
        cur.close()
        return records
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def parcel_details(parcel_id):
    """
    Use the parcel id to search parcel details on http://delcorealestate.co.delaware.pa.us/
    """
    def parcel_table_scrape(parcel_id):
        # Find all datalet headers and data elements
        data_headers = driver.find_elements_by_class_name('DataletSideHeading')
        data = driver.find_elements_by_class_name('DataletData')

        # Initialize empty list to store results
        data_list = []
        data_headers_list = []

        # Add data to list
        [data_list.append(entry.text) for entry in data]
        [data_headers_list.append(entry.text) for entry in data_headers]

        # Combine lists (this is necessary so we can drop empty header entries)
        zipped_list = zip(data_headers_list, data_list)
        output_list = list(zipped_list)

        exclude = 'Total OBY Value'

        if (not output_list or output_list[0][0] == exclude):
            # We have to insert null values into the database for the parcel so we don't check for it again
            # if it's missing residential data
            # TODO clean this up so it makes more sense
            print(parcel_id)
            conn = create_connection()
            cur = conn.cursor()
            truncate = 'TRUNCATE TABLE stg_parcel_details'
            cur.execute(truncate)
            insert = """INSERT INTO stg_parcel_details 
                    (parcel_id) 
                    VALUES ('{parcel_id}')""".format(parcel_id=parcel_id)
            cur.execute(insert) 
            populate_queries = ['call sp_populate_fact_parcel_details();']
            [cur.execute(query) for query in populate_queries]
            conn.commit()
            
            print('No data available for parcel ' + parcel_id)
        else:
            # Initialize lists to store results for .csv writer
            fields = ['parcel_id']
            values = [parcel_id]

            # Remove blank entries
            [fields.append(entry[0])
            for entry in output_list if not entry[0] == ' ' if not entry[0] == exclude]
            [values.append(entry[1])
            for entry in output_list if not entry[0] == ' ' if not entry[0] == exclude]
            
            # Save to csv and upload to database
            list_to_csv(os.path.join('.', 'data', 'residential_details.csv'),
                        fields, [values])
            db_upload('residential_details')

    print('Searching for parcel {parcel_id} details...'.format(
        parcel_id=parcel_id))

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

    # Scrape site information data
    #site_info = parcel_table_scrape(parcel_id)
    #print(site_info)

    # TODO scrape owner history data

    # Navigate to residential tab, scrape data, and upload
    # Finding the element by xpath is necessary here since the links do not have explicit names
    driver.find_element_by_xpath('//*[@id="sidemenu"]/li[2]/a/span').click()
    residential_info = parcel_table_scrape(parcel_id)
    driver.quit()
    print('{parcel_id} Complete!'.format(parcel_id=parcel_id))

def get_all_missing_parcel_detail():
    parcel_ids = get_parcels_without_details()
    q = Queue()
    p = Pool(10) 
    results = p.imap(parcel_details, parcel_ids)
    
    # In case a child process fails this loop tracks those and retries them
    successful = []
    failure_tracker = []
    retry_results = []

    # TODO add logging for parcel errrors
    while len(successful) < len(parcel_ids):
        successful.extend([r for r in results if not r is None])
        successful.extend([r for r in retry_results if not r is None])
        failed_items = []
        while not q.empty():
            failed_items.append(q.get())
        if failed_items:
            failure_tracker.append(failed_items)
            retry_results = p.imap(parcel_details, parcel_ids);

    # while loop was implemented when building the parcel_details function, might be unncessary now
    #while len(parcel_ids) > 0:
    #    try:
    #        p.imap(parcel_details, parcel_ids)
            #[(parcel_details(parcel)) for parcel in parcel_ids]
    #    except:
    #        print('Error in parcel details scrape\nRetrying...')
            # If the results error out we need to rerun the above process
    #        parcel_ids = get_parcels_without_details()
    #        p.terminate()
    #        p = Pool(10)
    p.close()
    p.join()
    p.terminate()

if __name__ == '__main__':
    #parcel_details('01000087900')
    get_all_missing_parcel_detail()