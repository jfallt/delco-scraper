from web_scraping.scraper import sales_history_by_date_range, sales_history_by_year_batch

print("""
     _      _                                                    
    | |    | |                                                   
  __| | ___| | ___ ___ ______ ___  ___ _ __ __ _ _ __   ___ _ __ 
 / _` |/ _ \ |/ __/ _ \______/ __|/ __| '__/ _` | '_ \ / _ \ '__|
| (_| |  __/ | (_| (_) |     \__ \ (__| | | (_| | |_) |  __/ |   
 \__,_|\___|_|\___\___/      |___/\___|_|  \__,_| .__/ \___|_|   
                                                | |              
                                                |_|           v1.0                                                  
""")
print('Hello! Welcome to the delco scraper!')
response = input('Please enter a command!')
if response == 'history':
    street = input('Please enter a street name: ')
    sales_history_by_date_range(street)
elif response == 'batch':
    start = input('Starting Year: ')
    end = input('Ending Year: ')
    sales_history_by_year_batch(start, end)
elif response == 'parcel update':
    print('placeholder')