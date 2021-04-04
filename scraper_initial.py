from datetime import date
import datetime
from get_selenium_res import get_selenium_res
from list_to_csv import list_to_csv
import pandas as pd
from scrape_delco_table import scrape_delco_table
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import Select
import timeit


def delco_scraper_full(street_name, start_date = (date.today() - datetime.timedelta(days=365)) , end_date = date.today()):
    """
    Use the advanced search function of the http://delcorealestate.co.delaware.pa.us/ with the below inputs:

    Args:
        street_name (str): input street name.
        start_date (datetime, optional): default is minus one year from today
        end_date (datetime, optional): default is today
        year (int): if creating year datasets for batch jobs this will append the year to the .csv name
    """
    print('Start Date: {start_date}'.format(start_date=start_date))
    print('End Date: {end_date}'.format(end_date=end_date))

    # Define table fields and format
    fields = ['ParcelID', 'TaxMapID', 'OwnerName', 'PropertyAddress',
              'SalesDate', 'SalesAmount', 'LandUseDescription', 'Map']
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

    start = timeit.default_timer()

    # Select results from first page
    list_to_csv('{street}.csv'.format(street=street_name),
                fields, scrape_delco_table(driver))

    #print(driver.page_source)
    # Check the last IndexLink element, if it matches Next >>
    # then click to the next page and run the above functions until no pages are left
    while driver.find_elements_by_class_name('IndexLink')[-1].text == 'Next >>':
        try:
            next_button = driver.find_elements_by_class_name('IndexLink')[-1]
            next_button.click()
            list_to_csv('results_1.csv', fields, scrape_delco_table(driver))
            a = pd.read_csv('{street}.csv'.format(street=street_name))
            b = pd.read_csv('results_1.csv')
            merged = pd.concat([a, b])
            merged.to_csv('{street}.csv'.format(
                street=street_name), index=False)
        except:
            break
    index = merged.index
    number_of_rows = len(index)
    stop = timeit.default_timer()
    runtime = stop - start
    print('Found {rows} entries for streets starting with {street_name} in {runtime} seconds'.format(
        rows=number_of_rows, street_name=street_name, runtime=runtime))

    driver.quit()

if __name__ == "__main__":
    delco_scraper_full()