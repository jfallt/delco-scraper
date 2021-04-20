from scraper import sales_history_by_date_range, sales_history_by_year_batch, parcel_details

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
print('Hello, welcome to the delco scraper CLI!')

while True:
    response = input('Please enter a command! (-h for help)\n')
    if response == '-a':
        street = input('Please enter a street name: ')
        sales_history_by_date_range(street)
        break
    elif response == '-b':
        start = input('Starting Year (YYYY): ')
        end = input('Ending Year (YYYY): ')
        sales_history_by_year_batch(start, end)
        break
    elif response == '-p':
        parcel_details()
        break
    elif response == '-h':
        print("""
        Input Commands List
        ----------------------------------------------------------------------------------------
        -b for sales data web scrape between two years
        -p manual parcel updates (this should run automatically after each web scrape)
        -a basic sales scrape for the past year with street input (do not input street numbers)
        ----------------------------------------------------------------------------------------
        """)
    else:
      print('Please enter a valid command!')