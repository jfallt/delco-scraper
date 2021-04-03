from list_to_csv import list_to_csv
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import Select

def read_table():
    """
    Read all elements of the table and save to list
    """
    results = []
    for j in range(3, 53):
        inner_result = []
        for i in range(1, 9):
            try:
                xpath = '//*[@id="searchResults"]/tbody/tr[{row_number}]/td[{cell_name}]'.format(
                    row_number=j, cell_name=i)
                xpath_map_link = '//*[@id="searchResults"]/tbody/tr[{row_number}]/td[8]/div/font/a'.format(
                    row_number=j)
                xpath_text = driver.find_element_by_xpath(xpath).text
                if xpath_text == 'Map Link':
                    xpath_link = driver.find_element_by_xpath(
                        xpath_map_link).get_attribute('href')
                    inner_result.append(xpath_link)
                elif len(xpath_text) == 0:
                    break
                else:
                    inner_result.append(xpath_text)
            except:
                break
        results.append(inner_result)
    return results


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

# Require user input for street
street = input('Input street name: ')

# Required urls
url_home = 'http://delcorealestate.co.delaware.pa.us'
url_search = 'http://delcorealestate.co.delaware.pa.us/pt/search/commonsearch.aspx?mode=address'

# Navigate to search page
driver.get(url_home)
driver.get(url_search)

# Trying to go to the url_search page results in a disclaimer page, click agree
driver.find_element_by_id('btAgree').click()

# Input street name in form, change search results to 50 and submit
driver.find_element_by_id('inpStreet').send_keys(street)
driver.find_element_by_id('selPageSize')
ddelement= Select(driver.find_element_by_id('selPageSize'))
ddelement.select_by_visible_text('50')
driver.find_element_by_id('btSearch').click()
print('Searching http://delcorealestate.co.delaware.pa.us/...')

# Select results from first page
list_to_csv('results.csv', fields, read_table())

# Check the last IndexLink element, if it matches Next >>
# then click to the next page and run the above functions
while driver.find_elements_by_class_name('IndexLink')[-1].text == 'Next >>':
    try:
        next_button = driver.find_elements_by_class_name('IndexLink')[-1]
        next_button.click()
        list_to_csv('results_1.csv')
        a = pd.read_csv('results.csv')
        b = pd.read_csv('results_1.csv')
        merged = pd.concat([a, b])
        merged.to_csv("results.csv", index=False)
    except:
        break

driver.quit()