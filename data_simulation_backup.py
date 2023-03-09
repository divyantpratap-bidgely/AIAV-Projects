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


def compareSamplingRate(donor_timestamps,acceptor_timestamps):
    sampling_rate = (donor_timestamps.diff().iloc[1])
    return abs(sampling_rate - acceptor_timestamps) == 0


def get_disagg_data(path):
    disagg_output_home1 = pd.read_csv(path)
    return disagg_output_home1.loc[:, ['timestamp', 'ev_output']]
   

def check_folder(folder_path, default_folder_name):
    if folder_path is None:
        folder_path = default_folder_name
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return folder_path
 
def getrawData(uuid, start, end):
    raw_url = 'https://naapi.bidgely.com/streams/users/{}/homes/1/gws/2/gb.json?t0={}&t1={}'.format(uuid, start, end)
    header = {"Authorization": "Bearer 4a6c7a25-624c-412b-b085-504298d042fc"}
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
    merged_data = pd.merge(disagg_output, raw_energy_data, on='timestamp', how='inner')
    merged_data['total_output'] = merged_data['ev_output'] + merged_data['value']
    merged_data.drop(['ev_output', 'value', 'duration'], axis=1, inplace=True)
    merged_data.rename(columns={'total_output': 'value'}, inplace=True)
    return merged_data


def create_heatmap(data, title, ax=None):
    data["datetimenew"] = pd.to_datetime(data["timestamp"], unit='s')
    data['date'] = data.datetimenew.dt.tz_localize('UTC', ambiguous='infer').dt.tz_convert('America/Los_Angeles')
    data['time'] = data['date'].dt.time
    data['date'] = data['date'].dt.date
    data = data.fillna(0)

    data = data.pivot_table(index='date', columns=['time'], values='value', aggfunc=sum)
    sns.heatmap(data, cmap='jet',cbar=True,xticklabels= 4,yticklabels=30,ax=ax)
    ax.set_title(title)
    ax.set_xticks(np.arange(len(data.columns))[::4])
    ax.set_xticklabels([data.columns[x].strftime('%H') for x in np.arange(len(data.columns))[::4]])
    ax.set_yticks(np.arange(len(data.index))[::30])
    ax.set_yticklabels([data.index[x].strftime('%b -%Y') for x in np.arange(len(data.index))[::30]])
    plt.xticks(rotation=90)
    plt.yticks(rotation=0)
    
    
def move_to_s3(local_file_path, bucket_name, s3_file_path):
    # create an S3 client
    s3 = boto3.client('s3')

    # upload file to S3
    s3.upload_file(local_file_path, bucket_name, s3_file_path)

uuid_file = pd.read_csv('/Users/divyantpratap/Desktop/Data_Simulation/users.csv',header = None)


raw_data_file = None
heatmap_folder = None  

for uuid in uuid_file[0]:
     
    disagg_output_home = get_disagg_data("/Users/divyantpratap/Desktop/Data_Simulation/disagg_output.csv")
    
    df_x = disagg_output_home
    
   
    #print(df_x.head())
    shift = 6
    shifted_disagg_output = shifting_disagg_data(df_x,shift)
    #print(shifted_disagg_output.head())
    start = disagg_output_home['timestamp'].min()
    end =disagg_output_home['timestamp'].max() 
    print(uuid,start,end)
    raw_data = getrawData(uuid, start, end)
    raw_data_folder = check_folder(raw_data_file,"raw_data")
    raw_file = os.path.join(raw_data_folder, f"raw_data_{uuid}.csv")
    
    
    if raw_data:
        with open(raw_file, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["timestamp", "value", "duration"])
            for data in raw_data:
                writer.writerow([data["time"], data["value"], data["duration"]])

    raw_energy_data = pd.read_csv(raw_file)
    print(raw_energy_data.head())
    acceptor_sampling_rate= raw_energy_data["duration"][1]
    
    if compareSamplingRate(disagg_output_home['timestamp'][0:2],acceptor_sampling_rate):
        
        merged_data = create_new_user(shifted_disagg_output,raw_energy_data)
        #print(merged_data.head())
        fig, axs = plt.subplots(1, 4, figsize=(30,15), gridspec_kw={'wspace': 0.5})
        #print(df_x.head())
        df_x.rename(columns={'ev_output': 'value'}, inplace=True)
        create_heatmap(df_x, "Donor User Appliance Usage", ax=axs[0])
        shifted_disagg_output.rename(columns={'ev_output': 'value'}, inplace=True)
        create_heatmap(shifted_disagg_output, "Shifted Usage", ax=axs[1])
    
        create_heatmap(raw_energy_data, "Acceptor User Usage", ax=axs[2])

        create_heatmap(merged_data, "Created User", ax=axs[3])

        fig.suptitle('UUID: {}, Acceptor Timestamp: {}'.format(uuid, acceptor_sampling_rate), fontsize=18)

        heatmap_folder = check_folder(heatmap_folder, "heatmap_data")
        heatmap_file = os.path.join(heatmap_folder, f"heatmap_{uuid}.png")
        fig.savefig(heatmap_file)
        print(f"Heatmap File printed succesfully at {heatmap_file} for {uuid}")
    
    else:
        print(f"Sampling Rates are not matching for {uuid}")
