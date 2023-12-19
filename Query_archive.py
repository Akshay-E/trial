import requests
import os
import sys
import pandas as pd
from astropy.table import Table
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
from ampel.ztf.t0.load.ZTFArchiveAlertLoader import ZTFArchiveAlertLoader
import logging
import argparse
import time 


def do_conesearch_stream(ra, dec, r=3): #r in arcmin

    query = {
        "cone": {
            "ra": ra,
            "dec": dec,
            "radius": r / 60
        },
        "candidate": {
            "rb": {
                "$gt": 0.3
            },
            "ndethist": {
                "$gt": 1,
                "$lte": 10000
            },
            "isdiffpos": {"$in": ["t", "1"]},
        }
    }
    header = {"Authorization": "bearer "+token}
    endpoint = 'https://ampel.zeuthen.desy.de/api/ztf/archive/v3/streams/from_query?programid=1'
    response = requests.post(endpoint, headers=header, json=query)

    if not response.ok:
        print('Query creation failed.')

    resume_token = response.json()['resume_token']
    return (resume_token)


def collect_alert(s_index, e_index, r_offset, d_offset, mode):

    alert_file= os.path.join(args.op, 'alerts.txt')
    rerun_file= os.path.join(args.op, 'rerun.txt')
    flag_file= os.path.join(args.op, 'flag.txt')

    with open(alert_file, mode) as f1, open(rerun_file, mode) as f2, open(flag_file, mode) as f3:

        for index in range(s_index, e_index):

            resume_token = do_conesearch_stream(
                1, dec_final[index] + d_offset)  # final 156
    #         resume_token=do_conesearch_stream(Ra_arr[index], Dec_arr[index]) #cluster

            config = {'archive': "https://ampel.zeuthen.desy.de/api/ztf/archive/v3",
                      "stream": resume_token}
            obj = []

            while (True): #forever retry
                try:
                    # alertlist = []
                    alertloader = ZTFArchiveAlertLoader(**config)
                    alerts = alertloader.get_alerts()
                    for alert in alerts:
                        # alertlist.append(alert)
                        obj.append(alert['objectId'])
                    
                    logging.info(f" Processed catID {index} with {len(set(obj))} unique alerts")
                    if obj:
                        f1.write('\n'.join(set(obj))+'\n')

                except requests.exceptions.HTTPError as e:
                    status_code = e.response.status_code
                    if status_code == 423:
                        logging.info(
                            'HTTP error {}: Trying again@ {}.'.format(status_code, index))
                        # retry_for.append(index)
                        f2.write(str(index)+'\n')
                        time.sleep(3)
                        
                        continue

                    else:
                        logging.info(f"flagged cat ID {index}")
                        # f3.write(f"{cl_df.ObID[index]}, {index}"+'\n')
                        f3.write(str(index)+'\n')

                    
                break

if __name__== "__main__":

    # set the logger
    logging.basicConfig( level=logging.INFO)
    logger = logging.getLogger()
    fhandler = logging.FileHandler(filename='query.log', mode='w')
    logger.addHandler(fhandler)
    formatter = logging.Formatter(
        ' %(levelname)s - %(lineno)s - %(message)s')
    fhandler.setFormatter(formatter)

    # CLI
    parser = argparse.ArgumentParser(description="Query ZTF archive for alerts around galaxy clusters")
    parser.add_argument('-s','--start', type=int, default=0, help="starting index of galaxy cluster")
    parser.add_argument('-op', type=str, default=os.getcwd(), help="path of the output directory")
    parser.add_argument('-ra_off', type=float, default=0.0, help="RA offset in degrees")
    parser.add_argument('-dec_off', type=float, default=0.0, help="DEC offset in degrees")
    parser.add_argument('-m', '--mode', type=str, default='w', help="Open file in mode w/a")

    parser.add_argument('-e','--end', type=int, help="ending index of galaxy cluster")
    args = parser.parse_args()
    # print(args.start)
    # access the archive token
    token = os.environ["TOKEN"]

    # # Catalogue of final 156 alerts
    # path_final = '/home/flying_dutchman/Downloads/MMA/clumpr/fp/results/cut:x1+chisq+x1err/m'
    # csv = 'cluster_info_for_alerts.csv'
    # cl_df = pd.read_csv(os.path.join(path_final, csv))
    # ra_final, dec_final = cl_df['Ra'], cl_df['Dec']

    # # Full catalogue import
    # path = "/home/flying_dutchman/Downloads/MMA/clumpr/CLUMPR_DESI.fits"
    # table_fits = Table.read(path)
    # Ra_arr, Dec_arr = table_fits['RA_central'], table_fits['DEC_central']

    # ra, dec of full catalogue
    csv_path = '/home/flying_dutchman/Downloads/MMA/clumpr/bg-rate/Ra_Dec.csv'
    df_cord = pd.read_csv(csv_path)
    ra_final, dec_final= df_cord['RA_central'], df_cord['DEC_central']


    # start, end = sys.argv[0], sys.argv[1]
    # start, end = 0, 2
    collect_alert(args.start, args.end, args.ra_off, args.dec_off, args.mode)
