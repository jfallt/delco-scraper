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


def delco_scraper_full(street_name, start_date=(date.today() - datetime.timedelta(days=365)), end_date=date.today()):
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

    # Find print preview and change to print preview window
    driver.find_element_by_id('Span1').click()
    child = driver.window_handles[-1]
    driver.switch_to.window(child)

    # Get row count of table, specify where data starts
    search_results = driver.find_elements_by_class_name('SearchResults')
    rows = len(search_results)
    row_start = 5

    # Save results from print window to list
    results = []
    for j in range(row_start, row_start+rows):
        inner_result = []
        for i in range(1, 8):
            xpath = '/html/body/form/table/tbody/tr/td/center/table/tbody/tr[{row_number}]/td[{column_number}]'.format(
                row_number=j, column_number=i)
            xpath_text = driver.find_element_by_xpath(xpath).text
            inner_result.append(xpath_text)
        results.append(inner_result)
        print('Finished row {row_number} of {total_rows}'.format(
            row_number=(j-row_start+1), total_rows=rows))

    # Send results to .csv
    list_to_csv('{street}.csv'.format(street=street_name),
                fields, results)

    driver.quit()


if __name__ == "__main__":
    delco_scraper_full()
