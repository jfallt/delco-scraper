def scrape_delco_table(driver):
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

if __name__ == "__main__":
    scrape_delco_table()