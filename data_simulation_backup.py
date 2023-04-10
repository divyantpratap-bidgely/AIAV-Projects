import json
import csv
import os
import requests
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import boto3
import configparser



config = configparser.ConfigParser()
config.read('config.conf')

# Reading required parameters from [userInfo]
donor_appliance = config.get('userInfo', 'donor_appliance')
env = config.get('userInfo', 'env')
hours_of_shift = config.getint('userInfo', 'hours_of_shift',fallback=0)
acceptor_uuid_file = config.get('userInfo', 'acceptor_uuid_location')
timezone = config.get('userInfo', 'timezone')

# Reading optional parameters from [Optional]
donor_uuid_file = config.get('Optional', 'donor_uuid_location', fallback=None)
s3_disagg_output_location = config.get('Optional', 's3_location_disagg_output', fallback=None)
raw_data_file= config.get('Optional', 'simulated_user_raw_data_location', fallback=None)
heatmap_folder = config.get('Optional', 'simulated_user_heatmap_location', fallback=None)


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


def generating_access_token(env):
    token_params = get_env_properties(env)
    protocol = token_params['protocol']
    primary_api = token_params['primary']
    client_id_value = client_id.get(env)
    client_secret_value = client_key.get(env)
    url = '{0}{1}:{2}@{3}/oauth/token?grant_type=client_credentials&scope=all'.format(protocol, client_id_value,
                                                                                      client_secret_value,
                                                                                      primary_api)
    response = requests.get(url)
    message_load = response.json()
    access_token = message_load.get('access_token')
    env_token = {'env_token': access_token}
    return env_token


def compareSamplingRate(donor_timestamps,acceptor_timestamps):
    sampling_rate = (donor_timestamps.diff().iloc[1])
    return abs(sampling_rate - acceptor_timestamps) == 0


def get_disagg_data(path, donor_appliance):
    disagg_output_home1 = pd.read_csv(path)
    disagg_output_home1 = disagg_output_home1.rename(columns={'epoch': 'timestamp', donor_appliance: 'ev_output'})
    return disagg_output_home1.loc[:, ['timestamp', 'ev_output']]

   

def check_folder(folder_path, default_folder_name):
    if folder_path == '' or folder_path==None:
        folder_path = default_folder_name
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return folder_path

access_token = generating_access_token(env).get('env_token')

def getrawData(env,uuid, start, end):
    token_params = get_env_properties(env)
    protocol = token_params['protocol']
    primary_api = token_params['primary']
    raw_url = '{0}{1}/streams/users/{2}/homes/1/gws/2/gb.json?t0={3}&t1={4}'.format(protocol,primary_api, uuid, start, end)
    env_token = generating_access_token(env).get('env_token')
    header = {"Authorization": f"Bearer {env_token}"}
    response = requests.get(raw_url, headers=header)
    if len(response.text) > 0:
        raw_api = json.loads(response.text)
        return raw_api
    return None
    
def shifting_disagg_data(disagg_output,shift):
    disagg_output_home = disagg_output.copy()
    disagg_output_home['timestamp'] = disagg_output['timestamp'] + (shift * 60 * 60)
    return disagg_output_home
    
def create_new_user(disagg_output,raw_energy_data):
    merged_data = pd.merge(disagg_output, raw_energy_data, on='timestamp', how='right')
    if 'ev_output' in merged_data.columns:
        merged_data['total_output'] = merged_data['ev_output'] + merged_data['value']
        merged_data.drop(['ev_output', 'value', 'duration'], axis=1, inplace=True)
        merged_data.rename(columns={'total_output': 'value'}, inplace=True)
    else:
        merged_data['total_output'] = merged_data['value_x'] + merged_data['value_y']
        merged_data.drop(['value_x', 'value_y', 'datetimenew','date','time'], axis=1, inplace=True)
        merged_data.rename(columns={'total_output': 'value'}, inplace=True)
    return merged_data

def create_heatmap(data, title, ax=None, min_value=None, max_value=None):
    data["datetimenew"] = pd.to_datetime(data["timestamp"], unit='s')
    data['date'] = data.datetimenew.dt.tz_localize('UTC', ambiguous='infer').dt.tz_convert('America/Los_Angeles')
    data['time'] = data['date'].dt.time
    data['date'] = data['date'].dt.date
    data = data.fillna(0)
    data = data.pivot_table(index='date', columns=['time'], values='value', aggfunc=sum)
    sns.heatmap(data, cmap='jet', cbar=True, xticklabels=4, yticklabels=30, ax=ax, vmin=min_value, vmax=max_value)
    ax.set_title(title)
    ax.set_xticks(np.arange(len(data.columns))[::4])
    ax.set_xticklabels([data.columns[x].strftime('%H') for x in np.arange(len(data.columns))[::4]])
    ax.set_yticks(np.arange(len(data.index))[::30])
    ax.set_yticklabels([data.index[x].strftime('%b -%Y') for x in np.arange(len(data.index))[::30]])
    ax.set_yticklabels(ax.get_yticklabels(), rotation='horizontal')
    plt.xticks(rotation=90)
    
    
def move_to_s3(local_file_path, bucket_name, s3_file_path):
    # create an S3 client
    s3 = boto3.client('s3')

    # upload file to S3
    s3.upload_file(local_file_path, bucket_name, s3_file_path)


print(env)
print(donor_uuid_file)
print(acceptor_uuid_file)
print(donor_appliance)

accepted_appliances = ["ev", "pp", "timed_wh", "ac", "sh"]

if donor_appliance not in accepted_appliances:
    print("Error: Donor appliance is not in the list of accepted values.")

def check_env(client_id, env):
    if env not in client_id:
        return f"{env} is given wrong client_id"
    
check_env(client_id,env)

if donor_uuid_file is None or donor_uuid_file == '':
    if donor_appliance == "ev":
        donor_uuid_file = "donor_uuid_ev.csv"
    elif donor_appliance == "pp":
        donor_uuid_file = "donor_uuid_pp.csv"
    elif donor_appliance == "timed_wh":
        donor_uuid_file = "donor_uuid_timed_wh.csv"
    elif donor_appliance == "ac":
        donor_uuid_file = "donor_uuid_ac.csv"
    elif donor_appliance == "sh":
        donor_uuid_file = "donor_uuid_sh.csv"



if s3_disagg_output_location is None or s3_disagg_output_location =='' :
    
    if donor_appliance == "ev":
        s3_disagg_output_location="s3://bidgely-ds/divyant/data_simulation/test/ev/data_simulation_test_ev_2023-04-06_12:16:45"
    elif donor_appliance == "pp":
        s3_disagg_output_location="s3://bidgely-ds/divyant/data_simulation/test/ev/data_simulation_test_ev_2023-04-06_12:16:45"
    elif donor_appliance == "timed_wh":
        s3_disagg_output_location="s3://bidgely-ds/divyant/data_simulation/test/ev/data_simulation_test_ev_2023-04-06_12:16:45"
    elif donor_appliance == "ac":
        s3_disagg_output_location="s3://bidgely-ds/divyant/data_simulation/test/ev/data_simulation_test_ev_2023-04-06_11:39:13"
    elif donor_appliance == "sh":
        s3_disagg_output_location="s3://bidgely-ds/divyant/data_simulation/test/ev/data_simulation_test_ev_2023-04-06_11:39:13"



acceptor_uuids = pd.read_csv(acceptor_uuid_file,header = None)
donor_uuids= pd.read_csv(donor_uuid_file,header = None)

for donor_uuid in donor_uuids[0]:
    s3_path = f"{s3_disagg_output_location}/tou_disagg/{donor_uuid}_tou.csv" 
    os.system(f"aws s3 cp {s3_path} .")
    disagg_output_home = get_disagg_data(f"{donor_uuid}_tou.csv",donor_appliance)
    df_x = disagg_output_home
    shifted_disagg_output = shifting_disagg_data(df_x, hours_of_shift)
    #print(shifted_disagg_output.head())
    start = disagg_output_home['timestamp'].min()
    end = disagg_output_home['timestamp'].max()
    print(donor_uuid,start,end)
    print("Donor UUID sampling rate is",disagg_output_home['timestamp'][0:2].diff().iloc[1])
    for uuid in acceptor_uuids[0]:
        print(uuid)
        raw_data = getrawData(env,uuid, int(start), int(end))

        raw_data_folder = check_folder(raw_data_file, "raw_data")
        raw_file = os.path.join(raw_data_folder, f"raw_data_{uuid}.csv")

        if raw_data:
            with open(raw_file, "w") as file:
                writer = csv.writer(file)
                writer.writerow(["timestamp", "value", "duration"])
                for data in raw_data:
                    writer.writerow([data["time"], data["value"], data["duration"]])

            raw_energy_data = pd.read_csv(raw_file)
            print(raw_energy_data.head())
            acceptor_sampling_rate = raw_energy_data["duration"][1]
            print("Acceptor UUID Sampling Rate is" ,acceptor_sampling_rate)
            if compareSamplingRate(disagg_output_home['timestamp'][0:2], acceptor_sampling_rate):
                print(shifted_disagg_output.head(),raw_energy_data.head())
                merged_data = create_new_user(shifted_disagg_output, raw_energy_data)
                #print(merged_data.head())
                fig, axs = plt.subplots(1, 4, figsize=(30, 15), gridspec_kw={'wspace': 0.5})
                #print(df_x.head())
                df_x.rename(columns={'ev_output': 'value'}, inplace=True)
                create_heatmap(df_x, "Donor User Appliance Usage", ax=axs[0])
                new_disagg = shifted_disagg_output.rename(columns={'ev_output': 'value'})
                
                create_heatmap(new_disagg, "Shifted Usage", ax=axs[1])
                
                min_value = min(raw_energy_data['value'].min(), merged_data['value'].min())
                max_value = max(raw_energy_data['value'].max(), merged_data['value'].max())

                create_heatmap(raw_energy_data, "Acceptor User Usage", ax=axs[2], min_value=min_value, max_value=max_value)
                
                create_heatmap(merged_data, "Created User", ax=axs[3], min_value=min_value, max_value=max_value)


                fig.suptitle('UUID: {}, Acceptor Timestamp: {}'.format(uuid+ "  x  " +donor_uuid, acceptor_sampling_rate), fontsize=18)

                heatmap_folder = check_folder(heatmap_folder, "heatmap_data")
                heatmap_file = os.path.join(heatmap_folder, f"heatmap_{uuid}_x_{donor_uuid}.png")
                fig.savefig(heatmap_file)
                del new_disagg
                print(f"Heatmap File printed successfully at {heatmap_file} for {uuid}_x_{donor_uuid}")

            else:
                print(f"Sampling Rates are not matching for {uuid}")