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

    # Send results to .csv
    #list_to_csv('{street}.csv'.format(street=street_name),
    #            fields, results)
    #print('Writing results to csv\n')


if __name__ == "__main__":
    delco_scraper_parcel('11000061501')
