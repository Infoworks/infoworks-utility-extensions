#!/bin/bash

SCRIPT_PATH=$(readlink -f "$0")
CURRENT_DIR=$(dirname "$SCRIPT_PATH")

# Variables for SSH connection
USERNAME=$2
SERVER=$1
private_key_file_name=$3
private_key_path="${CURRENT_DIR}/${private_key_file_name}"
chmod 400 $private_key_path
INPUT_DIR="${sourceBasePath}${tableNameAtSource}/gpg_encrypted_data/"
DESTINATION_DIR="${sourceBasePath}${tableNameAtSource}/gpg_decrypted_data/"
echo $INPUT_DIR
echo $DESTINATION_DIR
# SSH into the server, list .gpg files, and decrypt them
ssh -o StrictHostKeyChecking=no -i $private_key_path "$USERNAME@$SERVER" '
    rm -rf '"$DESTINATION_DIR"';
    mkdir -p '"$DESTINATION_DIR"';
    cd '"$INPUT_DIR"'
    for file in *.gpg; do
        if [ -f "$file" ]; then
            echo "Processing $file..."
            gpg --pinentry-mode=loopback --passphrase  "" --decrypt -o '$DESTINATION_DIR'$(basename "$file" .gpg) $file
            if [ $? -eq 0 ]; then
                echo "Decrypted $file successfully."
            else
                echo "Failed to decrypt $file."
            fi
        fi
    done
'