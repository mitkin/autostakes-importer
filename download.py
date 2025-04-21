import paramiko
from scp import SCPClient

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
    # Replace these values with your server's details
    hostname = "iridium1.npolar.io"            # Remote server hostname or IP address
    username = "eds"                   # SSH username
    key_file = "/home/mikhail/.ssh/ssh-eds-key"  # Path to your SSH private key
    remote_directory = "/home/eds/EXTRACTIONS"  # Path to the remote directory containing CSV files
    local_directory = "./measurements"    # Path to save the CSV files locally

    download_csv_files_via_ssh_key(hostname, username, key_file, remote_directory, local_directory)
