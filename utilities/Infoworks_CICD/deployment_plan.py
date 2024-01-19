import pandas
import configparser
config = configparser.ConfigParser()
config.read("./config.ini")
config_dict=config._sections
#print(config._sections)
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

for artifact in ["source","pipeline","pipeline_group","workflow"]:
    df = pandas.read_csv(f"./configurations/modified_files/{artifact}.csv",header=None,names=["artifact_name"])
    for index, row in df.iterrows():
        if artifact == "pipeline":
            domain_name,artifact_name=row['artifact_name'].split("#")
            artifact_name = artifact_name.replace(".json","").replace(f"{artifact}_","")
            message = f"Infoworks {artifact} {artifact_name} will be created/updated under {domain_name} based on below changes."
            print(bcolors.BOLD + bcolors.WARNING + message + bcolors.ENDC + bcolors.ENDC)
            relavant_keys = [key for key in config_dict.keys() if key.startswith("configuration$pipeline_configs")]
            for key in relavant_keys:
                last_key = key.split("$")[-1]
                for k, v in config_dict[key].items():
                    print(f"Changed {last_key}: from {k} to {v}")
        elif artifact == "workflow":
            domain_name, artifact_name = row['artifact_name'].split("#")
            artifact_name = artifact_name.replace(".json", "").replace(f"{artifact}_", "")
            message = f"Infoworks {artifact} {artifact_name} will be created/updated under {domain_name} based on below changes."
            print(bcolors.BOLD + bcolors.WARNING + message + bcolors.ENDC + bcolors.ENDC)
            relavant_keys = [key for key in config_dict.keys() if key.startswith("configuration$workflow_configs")]
            for key in relavant_keys:
                last_key = key.split("$")[-1]
                for k, v in config_dict[key].items():
                    print(f"Changed {last_key}: from {k} to {v}")
        else:
            artifact_name = row['artifact_name']
            artifact_name = artifact_name.replace(".json","").replace(f"{artifact}_","")
            message = f"Infoworks {artifact} {artifact_name} will be created/updated.Tables and Table groups under it will be configured based on below changes."
            print(bcolors.BOLD + bcolors.WARNING + message + bcolors.ENDC + bcolors.ENDC)
            relavant_keys = [key for key in config_dict.keys() if key.startswith("configuration$source_configs")]
            for key in relavant_keys:
                last_key=key.split("$")[-1]
                for k,v in config_dict[key].items():
                    print(f"Changed {last_key}: from {k} to {v}")
