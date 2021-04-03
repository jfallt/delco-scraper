from datetime import date
import datetime
from scrape_delco_table import scrape_delco_table
from list_to_csv import list_to_csv
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import Select
from string import ascii_lowercase
import timeit

def delco_scraper_full(street_name):
    """
    Send results of table to csv
    """

    # Determine date parameters
    today = date.today()
    one_year_ago = today - datetime.timedelta(days=365)
    date_format = '%m/%d/%Y'

    # Define table fields
    fields = ['ParcelID', 'TaxMapID', 'OwnerName', 'PropertyAddress',
            'SalesDate', 'SalesAmount', 'LandUseDescription', 'Map']

    # select options for selenium
    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1200")

    # add driver path and create driver
    DRIVER_PATH = '/usr/local/share/chromedriver'
    driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)

    # Required urls
    url_home = 'http://delcorealestate.co.delaware.pa.us'
    url_search = 'http://delcorealestate.co.delaware.pa.us/pt/search/advancedsearch.aspx?mode=advanced'

    # Navigate to search page
    driver.get(url_home)
    driver.get(url_search)

    # Trying to go to the url_search page results in a disclaimer page, click agree
    driver.find_element_by_id('btAgree').click()

    # Select results size
    driver.find_element_by_id('selPageSize')
    result_size = Select(driver.find_element_by_id('selPageSize'))
    result_size.select_by_visible_text('50')

    # Select street name
    street_selection = Select(driver.find_element_by_id('sCriteria'))
    street_selection.select_by_visible_text('Street Name (i.e. Main NOT Main St)')
    driver.find_element_by_id('txtCrit').send_keys(street_name)
    driver.find_element_by_id('btAdd').click()

    # Select sales date range
    sales_date_selection = Select(driver.find_element_by_id('sCriteria'))
    sales_date_selection.select_by_visible_text('Sales Date')
    driver.find_element_by_id('ctl01_cal1_dateInput').send_keys(one_year_ago.strftime(date_format))
    driver.find_element_by_id('ctl01_cal2_dateInput').send_keys(today.strftime(date_format))
    driver.find_element_by_id('btAdd').click()

    # Submit selections
    driver.find_element_by_id('btSearch').click()
    print('Searching http://delcorealestate.co.delaware.pa.us/ for streets starting with {street_name}...'.format(street_name=street_name))

    start = timeit.default_timer()
  
    # Select results from first page
    list_to_csv('{street}.csv'.format(street=street_name), fields, scrape_delco_table(driver))

    # Check the last IndexLink element, if it matches Next >>
    # then click to the next page and run the above functions
    while driver.find_elements_by_class_name('IndexLink')[-1].text == 'Next >>':
        try:
            next_button = driver.find_elements_by_class_name('IndexLink')[-1]
            next_button.click()
            list_to_csv('results_1.csv', fields, scrape_delco_table(driver))
            a = pd.read_csv('{street}.csv'.format(street=street_name))
            b = pd.read_csv('results_1.csv')
            merged = pd.concat([a, b])
            merged.to_csv('{street}.csv'.format(street=street_name), index=False)
        except:
            index = merged.index
            number_of_rows = len(index)
            break
    stop = timeit.default_timer()
    runtime = stop - start
    print('Found {rows} entries for streets starting with {street_name} in {runtime} seconds'.format(rows=number_of_rows,street_name=street_name, runtime=runtime))

    driver.quit()
    print()

for letter in ascii_lowercase:
    delco_scraper_full(letter)