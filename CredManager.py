from pydantic import BaseModel, Field, ValidationError
import dotenv
from google.oauth2.service_account import Credentials
from typing import Optional


class CredManagerConfig(BaseModel):
    aws_access_key_id: str = Field(..., env='AWS_ACCESS_KEY_ID')
    aws_secret_access_key: str = Field(..., env='AWS_SECRET_ACCESS_KEY')
    slack_token: Optional[str] = Field(None, env='SLACK_TOKEN')
    google_creds_key_path: Optional[str] = Field(None, env='GOOGLE_CREDS_KEY_PATH')
    db_user: Optional[str] = Field(None, env='DB_USER')
    db_password: Optional[str] = Field(None, env='DB_PASSWORD')
    db_host: Optional[str] = Field(None, env='DB_HOST')
    db_name: Optional[str] = Field(None, env='DB_NAME')

    class Config:
        env_file = '.env'

class CredManager:
    def __init__(self):
        dotenv.load_dotenv()
        self.config = CredManagerConfig()

    def get_aws_credentials(self):
        """
        Get AWS credentials from environment variables.
        :return: A tuple of (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        """
        return self.config.aws_access_key_id, self.config.aws_secret_access_key

    def get_slack_token(self):
        """
        Get Slack token from environment variables.
        :return: Slack token
        """
        return self.config.slack_token

    def get_google_credentials(self):
        """
        Get Google credentials from a service account file defined in environment variables.
        :return: Google OAuth2 Credentials object
        """
        if not self.config.google_creds_key_path:
            return None

        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        return Credentials.from_service_account_file(self.config.google_creds_key_path, scopes=scopes)

    def get_db_credentials(self):
        """
        Get Redshift database credentials from environment variables.
        :return: A dictionary containing Redshift credentials
        """
        return {
            'user': self.config.db_user,
            'password': self.config.db_password,
            'host': self.config.db_host,
            'db_name': self.config.db_name
        }
