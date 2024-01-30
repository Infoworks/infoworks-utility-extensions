import os
import subprocess
import sys
import traceback

script_base_path = os.path.dirname(os.path.abspath(__file__))
print(f"Script's base path: {script_base_path}")


def run_ssh_commands(hostname, username, private_key_path, commands):
    os.system(f"chmod 400 {private_key_path}")
    ssh_command = [
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-i',
        private_key_path,
        f'{username}@{hostname}'
    ]

    for command in commands:
        ssh_command.append(command)

    try:
        subprocess.run(ssh_command, check=True)

    except Exception as e:
        print(f"Error executing SSH commands: {e}")
        sys.exit(1)


def get_commands_to_execute(input_dir, archive_dir, destination_dir, cmd=None):
    commands_to_run = [f"rm -rf {destination_dir};", f"mkdir -p {destination_dir};", f"mkdir -p {archive_dir};"]
    if cmd is not None:
        commands_to_run.append(cmd)
    cmd = f"""    
    cd {input_dir};
    for file in *.gpg; do
        if [ -f "$file" ]; then
            echo "Processing $file..."
            gpg --yes --pinentry-mode=loopback --passphrase  "" --decrypt -o {destination_dir}/$(basename "$file" .gpg) $file
            if [ $? -eq 0 ]; then
                echo "Decrypted $file successfully."
                mv -f $file {archive_dir}/$file
            else
                echo "Failed to decrypt $file."
            fi
        fi
    done
    """
    commands_to_run.append(cmd)
    return commands_to_run


def get_env_variables():
    INPUT_DIR, ARCHIVE_DIR, DESTINATION_DIR = None, None, None
    jobType = os.getenv('jobType', None)
    tableNameAtSource = os.getenv('tableNameAtSource', None)
    sourceBasePath = os.getenv('sourceBasePath', None)
    if None not in {jobType, tableNameAtSource, sourceBasePath}:
        INPUT_DIR = os.path.join(sourceBasePath, tableNameAtSource, 'gpg_encrypted_data')
        ARCHIVE_DIR = os.path.join(sourceBasePath, tableNameAtSource, 'gpg_encrypted_data_archive')
        DESTINATION_DIR = os.path.join(sourceBasePath, tableNameAtSource, 'gpg_decrypted_data')
    return jobType, INPUT_DIR, ARCHIVE_DIR, DESTINATION_DIR


if __name__ == "__main__":
    try:
        # Ensure proper usage
        if len(sys.argv) < 4:
            print("Usage: python script.py <hostname> <username> <private_key_file_name>")
            sys.exit(1)

        hostname = sys.argv[1]
        username = sys.argv[2]
        private_key_file_name = sys.argv[3]
        private_key_path = os.path.join(script_base_path, private_key_file_name)
        jobType, INPUT_DIR, ARCHIVE_DIR, DESTINATION_DIR = get_env_variables()
        commands_to_execute = ["echo 'hello'"]
        if None not in {INPUT_DIR, ARCHIVE_DIR, DESTINATION_DIR}:
            if jobType.lower() == "source_structured_crawl":
                commands_to_execute = get_commands_to_execute(INPUT_DIR, ARCHIVE_DIR, DESTINATION_DIR,
                                                              cmd=f'cp -r {ARCHIVE_DIR.rstrip("/")}/* {INPUT_DIR};')
            elif jobType.lower() == "source_structured_cdc_merge":
                commands_to_execute = get_commands_to_execute(INPUT_DIR, ARCHIVE_DIR, DESTINATION_DIR)

            # for item in commands_to_execute:
            #     print(item)
            run_ssh_commands(hostname, username, private_key_path, commands_to_execute)
        else:
            print("Either input_dir or destination_dir is None. Skipping the prehook.")
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        sys.exit(1)
