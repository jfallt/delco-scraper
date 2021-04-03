import csv

def list_to_csv(file_name, fields, values):
    """
    Send results of table to csv
    """
    with open(file_name, 'w') as f:

        # using csv.writer method from CSV package
        write = csv.writer(f)

        write.writerow(fields)
        write.writerows(values)

if __name__ == "__main__":
    list_to_csv()