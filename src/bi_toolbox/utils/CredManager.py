from pydantic.v1 import BaseModel, Field
import dotenv
from google.oauth2.service_account import Credentials
from typing import Optional


class CredManagerConfig(BaseModel):
    """
    Configuration model for managing credentials.

    Attributes
    ----------
    aws_access_key_id : Optional[str]
        AWS access key ID, loaded from the environment variable 'AWS_ACCESS_KEY_ID'.
    aws_secret_access_key : Optional[str]
        AWS secret access key, loaded from the environment variable 'AWS_SECRET_ACCESS_KEY'.
    slack_token : Optional[str]
        Slack token, loaded from the environment variable 'SLACK_TOKEN'.
    google_creds_key_path : Optional[str]
        Path to the Google credentials key file, loaded from the environment variable 'GOOGLE_CREDS_KEY_PATH'.
    db_user : Optional[str]
        Database user, loaded from the environment variable 'DB_USER'.
    db_password : Optional[str]
        Database password, loaded from the environment variable 'DB_PASSWORD'.
    db_host : Optional[str]
        Database host, loaded from the environment variable 'DB_HOST'.
    db_name : Optional[str]
        Database name, loaded from the environment variable 'DB_NAME'.

    """
    aws_access_key_id: Optional[str] = Field(None, env='AWS_ACCESS_KEY_ID')
    aws_secret_access_key: Optional[str] = Field(None, env='AWS_SECRET_ACCESS_KEY')
    slack_token: Optional[str] = Field(None, env='SLACK_TOKEN')
    google_creds_key_path: Optional[str] = Field(None, env='GOOGLE_CREDS_KEY_PATH')
    db_user: Optional[str] = Field(None, env='DB_USER')
    db_password: Optional[str] = Field(None, env='DB_PASSWORD')
    db_host: Optional[str] = Field(None, env='DB_HOST')
    db_name: Optional[str] = Field(None, env='DB_NAME')

    class Config:
        """
        Pydantic configuration class for `CredManagerConfig`.

        Attributes
        ----------
        env_file : str
            Specifies the environment file to load variables from.
        """
        env_file = '.env'

class CredManager:
    """
    Credential Manager for loading and accessing various credentials.
    """
    def __init__(self):
        """
        Initialize the CredManager and load environment variables from `.env` file using `CredManagerConfig`.
        """
        dotenv.load_dotenv()
        self.config = CredManagerConfig()

    def get_aws_credentials(self):
        """
        Get AWS credentials from environment variables.

        Returns
        -------
        tuple
            A tuple of (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
        """
        return self.config.aws_access_key_id, self.config.aws_secret_access_key

    def get_slack_token(self):
        """
        Get Slack token from environment variables.

        Returns
        -------
        str or None
            Slack token if present, otherwise None.
        """
        return self.config.slack_token

    def get_google_credentials(self):
        """
        Get Google credentials from a service account file defined in environment variables.

        Returns
        -------
        google.oauth2.service_account.Credentials or None
            Google OAuth2 Credentials object if credentials are available, otherwise None.
        """
        if not self.config.google_creds_key_path:
            return None

        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        return Credentials.from_service_account_file(self.config.google_creds_key_path, scopes=scopes)

    def get_db_credentials(self):
        """
        Get database credentials from environment variables.

        Returns
        -------
        dict
            A dictionary containing database credentials with keys 'user', 'password', 'host', and 'db_name'.
        """
        return {
            'user': self.config.db_user,
            'password': self.config.db_password,
            'host': self.config.db_host,
            'db_name': self.config.db_name
        }
