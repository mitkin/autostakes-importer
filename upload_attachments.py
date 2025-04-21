import os
from datetime import datetime, timezone
from pynpdc import (
    AUTH_STAGING_ENTRYPOINT,
    DATASET_STAGING_ENTRYPOINT,
    APIException,
    AuthClient,
    DatasetClient,
)

def read_basic_auth(file_path):
    """
    Reads basic authentication credentials (username and password) from a file.

    Args:
        file_path (str): The path to the file containing the credentials.

    Returns:
        tuple: A tuple containing the username and password.
    """
    try:
        with open(file_path, "r") as file:
            lines = file.read().strip().split("\n")
            if len(lines) < 2:
                raise ValueError("File must contain at least two lines: username and password")

            username = lines[0].strip()
            password = lines[1].strip()
            return username, password
    except Exception as e:
        print(f"Error reading credentials file: {e}")
        return None, None

def check_local_files(attachment_name, local_directory):
    """
    Checks if a file with the given name exists in the local directory.

    Args:
        attachment_name (str): The name of the attachment to check.
        local_directory (str): The local directory to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.exists(os.path.join(local_directory, attachment_name))

def get_remote_attachments(dataset_id, client):
    """
    Fetches the list of remote attachment filenames, IDs, and their released dates for a dataset.

    Args:
        dataset_id (str): The ID of the dataset.
        client (DatasetClient): The DatasetClient instance.

    Returns:
        dict: A dictionary mapping filenames to a tuple of (attachment ID, released date).
    """
    try:
        attachments = client.get_attachments(dataset_id, query=f"{'as_kng'}")
        import ipdb; ipdb.set_trace()
        # attachements can have same filename and different id, so we need to have a list of attachments by filename
        # as key in the dictionary
        attachments = {attachment.id: attachment for attachment in attachments}
        return {attachment.filename: (attachment.description, attachment.id, attachment.prefix, attachment.title, attachment.released) for attachment in attachments}
    except Exception as e:
        print(f"Error fetching remote attachments: {e}")
        return {}



def sync_attachments_and_released_date(dataset_id, local_directory, auth_file):
    """
    Synchronizes attachments between local files and the dataset:
    - If a file exists locally and remotely, delete the remote one and upload the local one.
    - If a file exists locally but not remotely, upload it.
    - If a file does not exist locally, do nothing.
    - Checks remote attachments' released date and updates it to the current date if not set.

    Args:
        dataset_id (str): The ID of the dataset to sync attachments for.
        local_directory (str): The directory containing local files.
        auth_file (str): The path to the file containing authentication credentials.
    """
    # Read authentication credentials
    username, password = read_basic_auth(auth_file)
    if not username or not password:
        print("Failed to read authentication credentials.")
        return

    # Authenticate using pynpdc
    auth_client = AuthClient(AUTH_STAGING_ENTRYPOINT)
    try:
        account = auth_client.login(username, password)
    except APIException:
        print("Login failed. Please check your credentials.")
        return

    # Initialize DatasetClient
    client = DatasetClient(DATASET_STAGING_ENTRYPOINT, auth=account)

    try:
        # Fetch remote attachments
        remote_files = get_remote_attachments(dataset_id, client)
        print(f"Remote files: {list(remote_files.keys())}")

        # Current date in ISO format
        current_date = datetime.now(timezone.utc)

        # Check local files
        for local_file in os.listdir(local_directory):
            local_file_path = os.path.join(local_directory, local_file)
            if not os.path.isfile(local_file_path):
                continue  # Skip directories or non-files

            if local_file in remote_files:
                # File exists both locally and remotely
                remote_description, remote_id, remote_prefix, remote_title, remote_released_date = remote_files[local_file]

                # Update the released date if not set or not equal to the current date
                if not remote_released_date:
                    try:
                        client.update_attachment(dataset_id, remote_id, description=remote_description, filename=local_file, prefix='/products/', released=current_date, title=remote_title)
                        print(f"Updated released date for remote file: {local_file}")
                    except APIException as e:
                        print(f"Failed to update released date for {local_file}: {e}")

                # Delete the file from the dataset
                try:
                    client.delete_attachment(dataset_id, remote_id)
                    print(f"Deleted remote file: {local_file}")
                except APIException as e:
                    print(f"Failed to delete remote file {local_file}: {e}")
                    continue

                # Upload the local file back to the dataset
                try:
                    client.upload_attachment(dataset_id, local_file_path)
                    print(f"Uploaded local file: {local_file}")
                except APIException as e:
                    print(f"Failed to upload local file {local_file}: {e}")
            else:
                # File exists locally but not remotely
                try:
                    client.upload_attachment(dataset_id, local_file_path)
                    print(f"Uploaded local file: {local_file}")
                except APIException as e:
                    print(f"Failed to upload local file {local_file}: {e}")

        # Update released date of remote-only files
        for remote_file, (remote_id, released_date) in remote_files.items():
            if remote_file not in os.listdir(local_directory):
                # Update the released date if not set or not equal to the current date
                if not released_date:
                    try:
                        client.update_attachment(dataset_id, remote_id, released=current_date)
                        print(f"Updated released date for remote file: {remote_file}")
                    except APIException as e:
                        print(f"Failed to update released date for {remote_file}: {e}")

    except Exception as e:
        print(f"An error occurred during synchronization: {e}")


if __name__ == "__main__":
    # Replace these values with your specific needs
    DATASET_ID = "f446a2ad-37a7-45e7-8728-2f13be3444bb"  # Replace with your dataset ID
    LOCAL_DIRECTORY = "./measurements"   # Replace with your local directory path
    AUTH_FILE = "auth.txt"            # Replace with your authentication file path

    # Ensure the local directory exists
    if not os.path.exists(LOCAL_DIRECTORY):
        os.makedirs(LOCAL_DIRECTORY)

    # Manage the dataset attachments
    sync_attachments_and_released_date(DATASET_ID, LOCAL_DIRECTORY, AUTH_FILE)
