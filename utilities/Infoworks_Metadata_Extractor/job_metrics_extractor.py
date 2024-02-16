import os
import subprocess
import pkg_resources
import logging
import sys
import argparse
import json
import warnings
warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S',
                    )

if __name__ == "__main__":
    required = {'pandas', 'infoworkssdk==4.0a14'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed

    if missing:
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    import pandas as pd
    import warnings

    warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
    from infoworks.sdk.client import InfoworksClientSDK
    try:
        parser = argparse.ArgumentParser(description='Captures Jobs executed in Infoworks')
        parser.add_argument('--config_file', required=True, help='Fully qualified path of the configuration file')
        parser.add_argument('--time_range_for_jobs_in_mins', required=True, type=str,
                            help='Time Period to fetch the executed jobs')

        args = parser.parse_args()
        config_file_path = args.config_file
        if not os.path.exists(config_file_path):
            raise Exception(f"{config_file_path} not found")
        with open(config_file_path) as f:
            config = json.load(f)
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(config.get("protocol", "https"), config.get("host", None),
                                                   config.get("port", 443), config.get("refresh_token", None))

        job_metrics_response = iwx_client.get_abc_job_metrics(
            time_range_for_jobs_in_mins=args.time_range_for_jobs_in_mins)

        if job_metrics_response is not None:
            job_metrics_df = pd.DataFrame(job_metrics_response)
            user_details = iwx_client.get_user_details()
            if user_details['result'].get('response', {}).get('result'):
                user_details_df = pd.DataFrame(user_details['result']['response']['result'])
                user_details_df['email'] = user_details_df['profile'].apply(lambda x: x.get('email',''))
                user_details_df['name'] = user_details_df['profile'].apply(lambda x: x.get('name',''))
                user_details_df = user_details_df[["id", "name", "email"]]
                final_df = pd.merge(job_metrics_df, user_details_df, left_on='job_created_by', right_on='id', how='left') \
                    .drop('id', axis=1)
            else:
                logging.error("Failed to get users list!")
                final_df = job_metrics_df
            logging.info("Saving Output as JobMetrics.csv")
            pd.DataFrame(final_df).to_csv("JobMetrics.csv")

    except Exception as error:
        logging.error("Failed to capture Job Metrics \nError: {error}".format(error=repr(error)))
