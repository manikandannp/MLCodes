import sys
from getpass import getpass
import better_lotameapi as lotame

def main():
    username = 'manikandan.np@in.ibm.com'

    try:
        lotame.authenticate(username, password)
    except lotame.AuthenticationError:
        print('Error: Invalid username and/or password.')
        sys.exit()
    client_id = 10025
    audience_id = 414136
    response = lotame.get(f'audiences/{audience_id}')
    audience_info = response.json()
    audience_name = audience_info['name']
    definition = audience_info['definition']['component']
    print(definition)
    behaviors = {}
    find_behaviors(definition, behaviors)
    print('Behaviors in ' + audience_name)
    print(behaviors.keys())
    behavior = {
        'operator': 'AND',
        'complexAudienceBehavior': {
            'behavior': {
                'id': 10930626
            },
        'purchased': True
        }
    }
    definition.append(behavior)
    for behavior_id in behaviors:
        print(behavior_id + '\t' + behaviors[behavior_id])
    audience = {
        'clientId': client_id,
        'definition': {
            'component': definition
        }
    }
    new_audience = lotame.post('audiences/reachEstimate', audience).json()
    print(new_audience['reachEstimate'])
    lotame.cleanup()

def find_behaviors(definition, behaviors):
    """
    Adds all behavior IDs from an audience's component list into
    a list called behavior_list
    """
    for item in definition:
        if item['component']:
            find_behaviors(item['component'], behaviors)
        else:
            behavior_id = item['complexAudienceBehavior']['behavior']['id']
            behavior_name = item['complexAudienceBehavior']['behavior']['name']
            behaviors[behavior_id] = behavior_name

if __name__ == '__main__':
    main()