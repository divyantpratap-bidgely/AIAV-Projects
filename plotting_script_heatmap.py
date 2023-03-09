import os
import sys
import requests
import numpy as np
import pandas as pd
import matplotlib
from copy import deepcopy
matplotlib.use('Agg')
import seaborn as sns
from datetime import datetime
import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 22})


client_id = {
        'dev': 'admin',
        'ds': 'admin',
        'nonprodqa': 'admin',
        'prod-na': 'sahanam@bidgely.com',
        'prod-eu': 'sahanam@bidgely.com',
        'prod-jp': '',
        'prod-ca': 'nisha@bidgely.com',
        'prod-na-2': 'sahanam@bidgely.com',
        'preprod-na': 'admin',
        'qaperfenv': 'admin',
        'uat': 'admin',
    }

client_key = {
        'dev': 'admin',
        'ds': 'admin',
        'nonprodqa': 'admin',
        'prod-na': 'L2FVrraL',
        'prod-eu': 'UYWiUGx8',
        'prod-jp': '',
        'prod-ca': 'pNJ1y3na',
        'prod-na-2': 'FHoD0RFq',
        'preprod-na': 'admin',
        'qaperfenv': 'admin',
        'uat': 'admin',
    }


def get_env_properties(env):
    """
    Parameters:
        env             (str)               : Environment for which variables need to be extracted
    Returns:
        properties      (dict)              : Dictionary containing basic information
    """

    env_properties = {
        'dev': dict({
            'protocol': 'https://',
            'primary': 'devapi.bidgely.com',
            'aws_region': 'us-west-2'
        }),

        'ds': dict({
            'protocol': 'http://',
            'primary': 'dspyapi.bidgely.com',
            'aws_region': 'us-east-1'
        }),

        'nonprodqa': dict({
            'protocol': 'https://',
            'primary': 'nonprodqaapi.bidgely.com',
            'aws_region': 'us-west-2'
        }),
        'prod-na': dict({
            'protocol': 'https://',
            'primary': 'napyapi.bidgely.com',
            'aws_region': 'us-east-1'
        }),
        'prod-eu': dict({
            'protocol': 'https://',
            'primary': 'eupyapi.bidgely.com',
            'aws_region': 'eu-central-1'
        }),
        'prod-jp': dict({
            'protocol': 'https://',
            'primary': 'jppyapi.bidgely.com',
            'aws_region': 'ap-northeast-1'
        }),
        'prod-ca': dict({
            'protocol': 'https://',
            'primary': 'capyapi.bidgely.com',
            'aws_region': 'ca-central-1'
        }),
        'prod-na-2': dict({
            'protocol': 'https://',
            'primary': 'na2pyapi.bidgely.com',
            'aws_region': 'us-east-1'
        }),
        'preprod-na': dict({
            'protocol': 'https://',
            'primary': 'napreprodapi.bidgely.com',
            'aws_region': 'us-east-1'
        }),
        'qaperfenv': dict({
            'protocol': 'http://',
            'primary': 'awseb-e-i-awsebloa-1jk42nlshi8yb-2130246765.us-west-2.elb.amazonaws.com',
            'aws_region': 'us-west-2'
        }),
        'uat': dict({
            'protocol': 'https://',
            'primary': 'uatapi.bidgely.com',
            'aws_region': 'us-west-2'
        }),
    }

    env_prop = env_properties.get(str.lower(env))
    return env_prop


def fetch_input_data(params):
    uuid = params.get('uuid')
    t_start = params.get('t_start')
    t_end = params.get('t_end')
    env_token = params.get('env_token')
    environment = params.get('env')
    env_properties = get_env_properties(environment)

    url_raw = '{0}{1}/2.1/gb-disagg/process-request/{2}/1?start={3}&end={4}&overrideDisaggMode={5}&access_token={6}'
    url = url_raw.format(env_properties['protocol'], env_properties['primary'], uuid, t_start, t_end, '', env_token)
    res = requests.get(url)
    req_object = res.json()

    input_data = np.array(req_object['payload']['rawData'])
    input_data = input_data.astype('float')
    return input_data
    

def plot_data(input_data, user_params):
    uuid = user_params.get('uuid')
    raw = pd.DataFrame(input_data)
    cols = ['Month', 'Week', 'Day', 'DOW', 'HOD', 'Hour', 'Energy', 'sun_set', 'sun_rise']
    raw.columns = cols
    diff = datetime.utcfromtimestamp(raw['Hour'][0]).timetuple().tm_hour - raw['HOD'][0]

    # Convert the 24-hour system to 12-hour system

    if abs(diff) > int(24 // 2):
        if diff > 0:
            diff = diff - 24
        else:
            diff = diff + 24

    # Assign the timestamps to the relevant column

    raw['timestamp'] = pd.to_datetime(raw['Hour'] - diff * 3600, unit='s')
    raw['date'] = raw['timestamp'].dt.date
    raw['time'] = raw['timestamp'].dt.time

    # Cap the raw data energy to 99.9 % for avoiding distorted plots due to outliers

    raw['Energy'][raw['Energy'] > raw['Energy'].quantile(0.999)] = raw['Energy'].quantile(0.999)
    raw['Energy'][raw['Energy'] < raw['Energy'].quantile(0.001)] = raw['Energy'].quantile(0.001)

    # Make pivot table for the raw data

    heat_1 = raw.pivot_table(index='date', columns=['time'], values='Energy', aggfunc=sum)
    heat_1 = heat_1.fillna(0)
    heat_1 = heat_1.astype(int)

    fig_heatmap, axn = plt.subplots(1, 1, sharey=True)
    fig_heatmap.set_size_inches(20, 40)

    fig_heatmap.suptitle('Energy Heatmaps for: ' + uuid, fontsize=30)

    # Find the maximum values for each pivot table

    max_1 = np.max(heat_1.max())

    # Make heat map for each pivot table

    sns.heatmap(heat_1, ax=axn, cmap='jet', cbar=True, xticklabels=8, yticklabels=30, vmin=0,
                vmax=max_1)
    axn.set_title('Input Data')

    # Align the ticks and their orientation

    axn.tick_params(axis='y', labelrotation=0)

    plot_dir = user_params.get('plots_path')
    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)

    plt.savefig(plot_dir + uuid + '_heatmap.png')
    plt.close()
    

def plotting(params):

    uuids_list = pd.DataFrame([params.get('uuid')])
    if params.get('uuids_list_path') and params.get('uuids_list_path') != 'None':
        uuids_list = pd.read_csv(params.get('uuids_list_path'), header=None)

    for i in range(len(uuids_list)):
        uuid = uuids_list.iloc[i][0]
        try:
            user_params = deepcopy(params)
            user_params['uuid'] = uuid

            # fetching the input data
            input_data = fetch_input_data(user_params)

            # plotting the data
            plot_data(input_data, user_params)

            print('Plotting successful for uuid : {}'.format(uuid))
        except:
            print('Plotting unsuccessful for uuid : {}'.format(uuid))


def generating_access_token(params):
    token_params = get_env_properties(params.get('env'))
    protocol = token_params['protocol']
    primary_api = token_params['primary']
    client_id_value = client_id.get(params.get('env'))
    client_secret_value = client_key.get(params.get('env'))
    url = '{0}{1}:{2}@{3}/oauth/token?grant_type=client_credentials&scope=all'.format(protocol, client_id_value,
                                                                                      client_secret_value,
                                                                                      primary_api)
    response = requests.get(url)
    message_load = response.json()
    access_token = message_load.get('access_token')
    params['env_token'] = access_token
    return params


if __name__ == '__main__':

    # INSTRUCTIONS :-
    # uuid :- uuid of the single user to be run
    # uuids_list_path :- path to the csv file with the list of uuids for plotting
    # t_start :- starting timestamp
    # t_end :- ending timestamp
    # env :- uuid environment
    # env_token :- token for the environment
    # plots_path :- directory path where the plots should be dumped

    params = {
        'uuid': 'a2868abd-5bfc-4784-8fa9-7690cf9e942f',
        'uuids_list_path': 'uuids.csv',
        't_start': 0,
        't_end': 1700000000,
        'env': 'prod-na-2',
        'env_token': '5b48512a-fac2-415c-ac83-a731edc5ea9a',
        'plots_path': 'plots/',
    }

    # If providing the parameters through command line then these parameters are populated
    # python3 plotting_script.py uuid uuids_csv_file_path environment

    if len(sys.argv) > 1:
        params['uuid'] = sys.argv[1]
        params['uuids_list_path'] = sys.argv[2]
        params['env'] = sys.argv[3]

    # Generating the access token
    params = generating_access_token(params)

    plotting(params)
    print('Runs complete')
