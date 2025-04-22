#!/usr/bin/env python3
import os
import sys
import ipdb
from datetime import datetime, timezone
import pdb
import paramiko
from scp import SCPClient
import base64

from pynpdc import (
    AUTH_STAGING_ENTRYPOINT,
    DATASET_STAGING_ENTRYPOINT,
    APIException,
    AuthClient,
    DatasetClient,
)

def read_basic_auth_from_env():
    """Parse base64-encoded basic auth from APP_TOKEN environment variable and return username and password."""
    try:
        token = os.environ.get("APP_TOKEN")
        if not token:
            raise ValueError("APP_TOKEN is not set")

        decoded = base64.b64decode(token).decode("utf-8")

        if ":" not in decoded:
            raise ValueError("Decoded token must be in the format 'username:password'")

        username, password = decoded.split(":", 1)
        return username.strip(), password.strip()
    except Exception as e:
        print(f"Error reading APP_TOKEN: {e}")
        return None, None


def check_local_file_exists(filename, directory):
    """Check if a file exists in the specified local directory."""
    return os.path.isfile(os.path.join(directory, filename))


def get_remote_attachments(dataset_id, client, **kwargs):
    """Fetch the list of remote attachments for a dataset."""
    try:
        attachments = client.get_attachments(dataset_id, **kwargs)
        return [
                {
                    "id": attachment.id,
                    "description": attachment.description,
                    "prefix": attachment.prefix,
                    "title": attachment.title,
                    "released": attachment.released,
                    "filename": attachment.filename
            }
            for attachment in attachments ]
    except Exception as e:
        print(f"Error fetching remote attachments: {e}")
        return {}


def update_released_date(client, dataset_id, attachment, current_date):
    """Update the released date of an attachment."""
    try:
        client.update_attachment(
            dataset_id,
            attachment["id"],
            description=attachment["description"],
            filename=attachment["filename"],
            prefix="/products/",
            released=current_date,
            title=attachment["title"],
        )
        print(f"Updated released date for: {attachment['filename']}")
    except APIException as e:
        print(f"Failed to update released date for {attachment['filename']}: {e}")


def sync_local_to_remote(dataset_id, local_directory, client):
    """Synchronize local files to the remote dataset."""
    current_date = datetime.now(timezone.utc)
    for local_file in os.listdir(local_directory):
        local_file_path = os.path.join(local_directory, local_file)
        if not os.path.isfile(local_file_path):
            continue

        try:
            client.upload_attachment(dataset_id, local_file_path, prefix='/products/', released=current_date)
            print(f"Uploaded local file: {local_file}")
        except APIException as e:
            print(f"Failed to upload local file {local_file}: {e}")

def clean_remote_files(local_files, dataset_id, client):
    for local_file in local_files:
        remote_files = get_remote_attachments(dataset_id, client, q=f'{local_file}', prefix='/products/')
        [ client.delete_attachment(dataset_id, x['id']) for x in remote_files ]

def sync_attachments_and_released_date(dataset_id, local_directory, auth_file):
    """Main function to synchronize attachments and released dates."""
    username, password = read_basic_auth_from_env()
    if not username or not password:
        print("Failed to read authentication credentials.")
        return
    try:
        auth_client = AuthClient(AUTH_ENTRYPOINT)
        account = auth_client.login(username, password)
    except APIException:
        print("Login failed. Please check your credentials.")
        return
    client = DatasetClient(DATASET_ENTRYPOINT, auth=account)
    local_files = os.listdir(local_directory)
    clean_remote_files(local_files, dataset_id, client)
    sync_local_to_remote(dataset_id, local_directory, client)

def create_ssh_client_with_key(hostname, username, key_file, passphrase=None):
    """
    Create an SSH client and connect to the server using an SSH key.
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Load the SSH private key
    private_key = paramiko.RSAKey.from_private_key_file(key_file, password=passphrase)
    client.connect(hostname, username=username, pkey=private_key)
    return client

def list_remote_csv_files(ssh_client, remote_directory):
    """
    List all .csv files in a remote directory using SSH.
    """
    stdin, stdout, stderr = ssh_client.exec_command(f"ls {remote_directory}/*.csv")
    files = stdout.read().decode().splitlines()
    error = stderr.read().decode()
    if error:
        raise Exception(f"Error listing files: {error}")
    return files

def download_csv_files_via_ssh_key(hostname, username, key_file, remote_directory, local_directory, passphrase=None):
    """
    Download all .csv files from a remote server via SSH using an SSH key.
    """
    try:
        ssh_client = create_ssh_client_with_key(hostname, username, key_file)
        scp_client = SCPClient(ssh_client.get_transport())

        # List all .csv files in the remote directory
        csv_files = list_remote_csv_files(ssh_client, remote_directory)
        print(f"Found {len(csv_files)} CSV files in {remote_directory}: {csv_files}")

        # Download each .csv file
        for remote_file in csv_files:
            local_file = f"{local_directory}/{remote_file.split('/')[-1]}"
            scp_client.get(remote_file, local_file)
            print(f"Downloaded {remote_file} to {local_file}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'scp_client' in locals():
            scp_client.close()
        if 'ssh_client' in locals():
            ssh_client.close()


if __name__ == "__main__":
    DATASET_ID = os.getenv("DATASET_ID", "55d8c50d-24f3-4f2e-92d5-58b099fcab0b")
    LOCAL_DIRECTORY = "./products"
    AUTH_FILE = "./auth.txt"
    AUTH_ENTRYPOINT = os.getenv("AUTH_ENTRYPOINT", AUTH_STAGING_ENTRYPOINT)
    DATASET_ENTRYPOINT = os.getenv("DATASET_ENTRYPOINT", DATASET_STAGING_ENTRYPOINT)

    print(AUTH_ENTRYPOINT, DATASET_ENTRYPOINT)

    hostname = "iridium1.npolar.io"            # Remote server hostname or IP address
    username = "eds"                   # SSH username
    key_file = "/home/mikhail/.ssh/ssh-eds-key"  # Path to your SSH private key
    remote_directory = "/home/eds/EXTRACTIONS"  # Path to the remote directory containing CSV files
    local_directory = "./products"    # Path to save the CSV files locally


    if not os.path.isdir(local_directory):
        os.mkdir(local_directory)

    download_csv_files_via_ssh_key(hostname, username, key_file, remote_directory, local_directory)

    if not os.path.exists(LOCAL_DIRECTORY):
        os.makedirs(LOCAL_DIRECTORY)

    sync_attachments_and_released_date(DATASET_ID, LOCAL_DIRECTORY, AUTH_FILE)
