import sys
import csv
from getpass import getpass
import better_lotameapi as lotame

def get_monthly_uniques(behavior_id, network, date):
    client_id = 10025
    network = bool(network == 'y')
    endpoint = f'statistics/audiences' \
               f'?audience_id={behavior_id}&ref_date={date}' \
               f'&stat_interval=FULL_MONTH&client_id={client_id}'
    response = lotame.get(endpoint)
    resp_json = response.json()
    print(resp_json)

    if resp_json['audienceStatsReports'] == []:
        print(f'Error: Couldn\'t get uniques for {behavior_id}')
        resp_json['uniques'] = 0
    else:
        resp_json = resp_json['audienceStatsReports'][0]
    status = response.status_code
    if status != 200:
        return None
    return resp_json['uniques']

def main():
    username = 'manikandan.np@in.ibm.com'
    password = 'Kmbros8085'
    lotame.authenticate(username, password)
    filename = "C:/Data/Reports/Adhoc/Audience Tribe/Lotame API/behavior_ids.txt"
    with open(filename) as behavior_file:
        behavior_ids = [behavior_id.strip() for behavior_id in behavior_file]
    year = 2019
    network = 'y'
    behavior_stats = []
    months = ['2','3','4','5','6','7']

    for month in months:
        print(month)
        if len(month) == 1:
            month = f'0{month}'
        date = f'{year}{month}01'
        print('Grabbing stats...')
        for behavior_id in behavior_ids:
            uniques = get_monthly_uniques(behavior_id, network, date)
            if not uniques:
                print(f'Error: Couldn\'t get uniques for {behavior_id}')
                continue
            else:
                print(uniques)
            behavior_stat = {
                'behavior_id': behavior_id,
                'uniques': uniques,
                'date_YYYYMMDD':date
            }
            behavior_stats.append(behavior_stat)

    lotame.cleanup()
    if not behavior_stats:
        print('Couldn\'t get any stats')
        sys.exit()
    filename = f'monthly_stats_{date}.csv'
    with open(filename, 'w') as statfile:
        writer = csv.writer(statfile, delimiter='\t')
        writer.writerow(['Audience ID', 'Monthly Uniques', 'Month'])
        for behavior_stat in behavior_stats:
            behavior_id = behavior_stat['behavior_id']
            uniques = behavior_stat['uniques']
            date_YYYYMMDD =  behavior_stat['date_YYYYMMDD']
            writer.writerow([behavior_id, uniques, date_YYYYMMDD])
    print(f'Stats written to {filename}')

if __name__ == '__main__':
    main()