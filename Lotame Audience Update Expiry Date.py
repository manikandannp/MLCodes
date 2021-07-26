import sys
from getpass import getpass
import csv
import better_lotameapi as lotame

def get_audience_info(audience_id):
    response = lotame.get(f'audiences/{audience_id}')
    status = response.status_code
    if status != 200:
        return None
    return response.json()

def main():
    username = 'manikandan.np@in.ibm.com'
    password = 'Kmbros8085'

    # Authenticate with the Lotame API
    try:
        lotame.authenticate(username, password)
    except lotame.AuthenticationError:
        print('Error: Invalid username and/or password.')
        sys.exit()

    filename = "C:/Data/Reports/Adhoc/Audience Tribe/Lotame API/Expirydate.csv"
    audience_exp_date = "06/01/2019"

    with open(filename) as csv_file:
        reader = csv.reader(csv_file)
        # Skip header row
        next(reader)
        for row in reader:
            audience_id = row[0]
            audience_info = get_audience_info(audience_id)
            if not audience_info:
                print(f'Error: Audience {audience_id} not found')
                continue
            print(row[0])
            # Get the audience info from the Lotame API
            response = lotame.get(f'audiences/{audience_id}')
            # Pull the resulting JSON from the Response object and get the desired values from it
            audience_info = response.json()

            if audience_info.get('expirationDate'):
                print(audience_info.get('expirationDate'))
                audience_info['expirationDate'] = audience_exp_date
            else:
                print('null')
                audience_info['expirationDate'] = audience_exp_date

            status = lotame.put(f'audiences/{audience_id}', audience_info).status_code
            print(status)

    # Delete the ticket-granting ticket, now that the script is done with it
    lotame.cleanup()

if __name__ == '__main__':
    main()