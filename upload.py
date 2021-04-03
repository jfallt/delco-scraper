import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)

# Open sheet and get first worksheet
sheet = client.open('delco-scraper-results')


with open('results.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(sheet.id, data=content)

#sheet_instance = sheet.get_worksheet(0)

# get all the records of the data
#records_data = sheet_instance.get_all_records()

# view the data
#records_data

# add a sheet with 20 rows and 2 columns
#sheet.add_worksheet(rows=20,cols=2,title='runs')

# get the instance of the second sheet
#sheet_runs = sheet.get_worksheet(1)