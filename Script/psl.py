import urllib.request
import zipfile
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
pd.set_option('display.max_columns', 500)
import json
import os
from prefect import flow, task
from prefect_gcp import GcpCredentials

@task(log_prints=True)
def fetch_zip_data(url, filename):
    """ download zip file from cricsheet website """
    url = url
    filename = filename
    urllib.request.urlretrieve(url, filename)

@task(log_prints=True)
def unzip_data():
    """ unzip data into unzip_data folder """
    with zipfile.ZipFile("./raw_data/psl_json.zip","r") as zip_ref:
        zip_ref.extractall("./unzip_data/")


@task(log_prints=True)
def matches(df):
    """ transforming json files into pandas dataframe """
    overs = []
    balls = []
    batsman = []
    bowler = []
    batsman_runs = []
    extras = []
    total_runs = []
    kind = []
    fielder = []
    batting_team = []
    try:
        for x in range(2):
            for i in range(len(df['innings'][0][x]['overs'])):
                l = len(df['innings'][0][x]['overs'][i]['deliveries'])
                for z in range(l):
                    try:
                        batting_team.append(df['innings'][0][x]['team'])
                        overs.append(i+1) # overs
                        balls.append(z+1)
                        batsman.append(df['innings'][0][x]['overs'][i]['deliveries'][z]['batter'])
                        bowler.append(df['innings'][0][x]['overs'][i]['deliveries'][z]['bowler'])
                        batsman_runs.append(df['innings'][0][x]['overs'][i]['deliveries'][z]['runs']['batter'])
                        extras.append(df['innings'][0][x]['overs'][i]['deliveries'][z]['runs']['extras'])
                        total_runs.append(df['innings'][0][x]['overs'][i]['deliveries'][z]['runs']['total'])
                        if 'wickets' in df['innings'][0][x]['overs'][i]['deliveries'][z].keys():
                            kind.append(df['innings'][0][x]['overs'][i]['deliveries'][z]['wickets'][0]['kind'])
                            if 'fielders' in df['innings'][0][x]['overs'][i]['deliveries'][z]['wickets'][0]:
                                fielder.append(df['innings'][0][x]['overs'][i]['deliveries'][z]['wickets'][0]['fielders'][0]['name'])
                            else:
                                fielder.append(np.nan)
                        else:
                            kind.append(np.nan)
                            fielder.append(np.nan)
                    except:
                        pass
    except:
        pass
    dt = pd.DataFrame(data={'batting_team': batting_team, 'overs' : overs, 'balls': balls,'batsman' : batsman, 'bowler': bowler,'batsman_runs': batsman_runs, 'extras': extras,
            'total_runs': total_runs, 'kind' : kind, 'fielder' : fielder})
    dt['date'] = pd.to_datetime(df['info.dates'][0][0]).date()
    dt['toss_winner'] = df['info.toss.winner'][0]
    dt['toss_decision'] = df['info.toss.decision'][0]
    dt['venue'] = df['info.venue'][0]
    ##
    if 'info.player_of_match' in df.columns:
        dt['player_of_match'] = df['info.player_of_match'][0][0]
    else:
        dt['player_of_match'] = np.nan
    ##
    if 'info.event.stage' in df.columns:
        dt['event_stage'] = df['info.event.stage'][0]
    else:
        dt['event_stage'] = np.nan
    ##
    if 'info.event.match_number' in df.columns:
        dt['match_number'] = df['info.event.match_number'][0]
    else:
        dt['match_number'] = np.nan
    ##
    if 'info.outcome.winner' in df.columns:
        dt['outcome_winner'] =df['info.outcome.winner'][0]
    else:
        dt['outcome_winner'] = df['info.outcome.result'][0]
    ##
    if 'info.outcome.by.wickets' in df.columns:
        dt['outcome_by_wickets'] =df['info.outcome.by.wickets'][0]
    else:
        dt['outcome_by_wickets'] = np.nan
    ##
    if 'info.outcome.by.runs' in df.columns:
        dt['outcome_by_runs'] = df['info.outcome.by.runs'][0]
    else:
        dt['outcome_by_runs'] = np.nan
    ##
    if 'info.outcome.method' in df.columns:
        dt['outcome_by_method'] =df['info.outcome.method'][0]
    else:
        dt['outcome_by_method'] = np.nan
    
    return dt

@flow(name="Consolidate data", log_prints=True)
def consolidate_dataframe():
    """consolidating data into one dataframe"""
    z = 0
    dl = pd.DataFrame(columns= ['batting_team', 'overs', 'balls', 'batsman', 'bowler', 'batsman_runs',
        'extras', 'total_runs', 'kind', 'fielder', 'date', 'toss_winner',
        'toss_decision', 'venue', 'player_of_match', 'outcome_winner',
        'event_stage', 'outcome_by_runs','outcome_by_wickets'])
    for c in os.listdir(r"{}\unzip_data".format(os.getcwd())):
        if c.endswith('.json'):
            json_dict  = json.load(open("unzip_data/{}".format(c)))
            df = json_normalize(json_dict)
            print(z)
            z = z+1
            dt = matches(df = df)
            dl = pd.concat([dl,dt])
    dl = dl.reset_index()
    dl = dl.sort_values(by = ['date','index'])
    dl['date'] = pd.to_datetime(dl['date'])
    # dl['index'] = dl['index'].astype('str')
    dl['event_stage'] = dl['event_stage'].astype('str')
    # blankIndex=[''] * len(dl)
    # dl.index=blankIndex
    dl[(dl['player_of_match'].isnull()) & (dl.outcome_winner == 'Lahore Qalandars')]['player_of_match'] = 'S Lamichhane'
    dl = dl.astype(str)
    return dl

@task(retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("psl-gcp-creds")

    df.to_gbq(
        destination_table="psl.RawData",
        project_id="prime-ember-376405",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow(name="Ingest Data",log_prints=True)
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    url = "https://cricsheet.org/downloads/psl_json.zip"
    filename = "./raw_data/psl_json.zip"

    fetch_zip_data(url, filename)
    unzip_data()
    df = consolidate_dataframe()
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
