import os
from google.oauth2.service_account import Credentials
import dotenv

class CredManager:
    def __init__(self):
        # Load environment variables
        dotenv.load_dotenv()

    @staticmethod
    def get_aws_credentials():
        """
        Get AWS credentials from environment variables.
        :return: A tuple of (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        """
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError("AWS credentials are missing from environment variables.")

        return aws_access_key_id, aws_secret_access_key

    @staticmethod
    def get_slack_token():
        """
        Get Slack token from environment variables.
        :return: Slack token
        """
        slack_token = os.getenv('SLACK_TOKEN')
        if not slack_token:
            raise ValueError("Slack token is missing from environment variables.")
        return slack_token

    @staticmethod
    def get_google_credentials():
        """
        Get Google credentials from a service account file defined in environment variables.
        :return: Google OAuth2 Credentials object
        """
        google_creds_key_path = os.getenv('GOOGLE_CREDS_KEY_PATH')
        if not google_creds_key_path:
            raise ValueError("Google credentials path is missing from environment variables.")

        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        return Credentials.from_service_account_file(google_creds_key_path, scopes=scopes)

    @staticmethod
    def get_db_credentials():
        """
        Get Redshift database credentials from environment variables.
        :return: A dictionary containing Redshift credentials
        """
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')

        if not db_user or not db_password or not db_host or not db_name:
            raise ValueError("Database credentials are missing from environment variables.")

        return {
            'user': db_user,
            'password': db_password,
            'host': db_host,
            'db_name': db_name
        }
