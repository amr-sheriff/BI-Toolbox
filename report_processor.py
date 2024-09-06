import os
from typing import Optional
import logging
import dotenv
import boto3
import pandas as pd
from datetime import date, timedelta
from slack_sdk import WebClient
import gspread
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials


class GenericReportProcessor:
    logger = logging.getLogger('GenericReportProcessor')
    logger.setLevel(logging.INFO)
    log_handler = logging.FileHandler('default.log')
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_handler)

    def __init__(self):
        self.s3 = None
        self.s3_client = None
        self.slack = None
        self.gc = None
        self.drive = None
        self.db_conn = None
        self.current_week: Optional[date] = None
        self.last_week: Optional[date] = None
        self.second_week_before: Optional[date] = None
        self.third_week_before: Optional[date] = None


    @classmethod
    def factory(cls, services: Optional[list] = None):
        """
        Factory method to initialize the GenericReportProcessor with selected services.

        :param services: List of services to initialize (like 's3', 'slack', 'google_drive', 'redshift').
                         If None, initialize all services.
        :return: GenericReportProcessor instance
        """
        instance = cls()
        instance.setup_environment(services)
        return instance


    def setup_environment(self, services: Optional[list] = None) -> None:
        """
        Loads environment variables and initializes connections for Slack, S3, Google Sheets, and Redshift.
        Also calculates week and date attributes for easier reference throughout the class.
        If no services are specified, initializes all available services.

        :raises ValueError: If essential environment variables are missing.
        """
        dotenv.load_dotenv()

        # Calculate week and date attributes
        today = date.today()
        self.current_week = today - timedelta(days=today.weekday())
        self.last_week = self.current_week - timedelta(days=7)
        self.second_week_before = self.current_week - timedelta(days=14)
        self.third_week_before = self.current_week - timedelta(days=21)

        # Slack setup
        if not services or 'slack' in services:
            slack_token = os.getenv('SLACK_TOKEN')
            if slack_token:
                self.slack = WebClient(slack_token)
            else:
                self.logger.warning("Slack token not found. Slack notifications will be unavailable.")

        # AWS S3 setup using boto3 resource
        if not services or 's3' in services:
            aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            if aws_access_key_id and aws_secret_access_key:
                self.s3 = boto3.resource(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )
            else:
                self.logger.warning("AWS S3 credentials missing. S3 data retrieval will be unavailable.")

        # Google Sheets setup
        if not services or 'google_drive' in services:
            google_creds_key_path = os.getenv('GOOGLE_CREDS_KEY_PATH')
            if google_creds_key_path:
                scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
                google_creds = Credentials.from_service_account_file(google_creds_key_path, scopes=scopes)
                self.gc = gspread.authorize(google_creds)
                gauth = GoogleAuth()
                self.drive = GoogleDrive(gauth)
            else:
                self.logger.warning("Google credentials missing. Google Sheets and Drive functionality will be unavailable.")

        # Redshift connection setup
        if not services or 'redshift' in services:
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_host = os.getenv('DB_HOST')
            db_name = os.getenv('DB_NAME')
            if db_user and db_password and db_host and db_name:
                try:
                    engine = create_engine(f'redshift+psycopg2://{db_user}:{db_password}@{db_host}:5439/{db_name}',
                                           connect_args={"keepalives": 1, "keepalives_idle": 60, "keepalives_interval": 60})
                    self.db_conn = engine.connect()
                except Exception as e:
                    self.logger.error(f"Error connecting to Redshift: {e}")
                    raise ConnectionError(f"Failed to connect to Redshift: {e}")
            else:
                self.logger.warning("Redshift credentials missing. Database functionality will be unavailable.")


    @classmethod
    def set_logger_path(cls, path: str) -> None:
        """
        Class method to change the logger file path.

        :param path: str - The new path for the log file.
        :return: None
        """
        cls.logger.removeHandler(cls.log_handler)
        cls.log_handler = logging.FileHandler(path)
        cls.log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        cls.logger.addHandler(cls.log_handler)
        cls.logger.info(f'Logger path changed to {path}')


    def retrieve_data(self, data_source_type: str, *, bucket_name: str = None, prefix: str = None, query: str = None, file_path: str = None) -> pd.DataFrame:
        """
        Retrieves data from S3, a database, or a local file based on the data_source_type provided.

        :param data_source_type: str - The source of the data (required - 's3', 'database', or 'local').
        :param bucket_name: str - The S3 bucket name. (required if data_source_type is 's3').
        :param prefix: str - The prefix for filtering S3 objects. (required if data_source_type is 's3').
        :param query: str - The SQL query for retrieving data from the database. (required if data_source_type is 'database').
        :param file_path: str - The path to the local file. (required if data_source_type is 'local').

        :return: pd.DataFrame - The retrieved data as a Pandas DataFrame.
        """
        self.logger.info(f"Retrieving data from {data_source_type}")
        try:
            if data_source_type == 's3':
                if not bucket_name or not prefix:
                    raise ValueError("Both 'bucket_name' and 'prefix' are required for S3 data retrieval.")
                return self._retrieve_from_s3(bucket_name, prefix)

            elif data_source_type == 'database':
                if not query:
                    raise ValueError("The 'query' is required for database data retrieval.")
                return self._retrieve_from_database(query)

            elif data_source_type == 'local':
                if not file_path:
                    raise ValueError("The 'file_path' is required for local file data retrieval.")
                return self._retrieve_from_local(file_path)

            else:
                raise ValueError(f"Unsupported data source type: {data_source_type}")

        except (ValueError, TypeError) as e:
            self.logger.error(f"Error during data retrieval: {str(e)}")
            raise

    def _retrieve_from_s3(self, bucket_name: str, prefix: str) -> pd.DataFrame:
        """
        Retrieves the latest file from an S3 bucket based on the prefix and last modified time.
        Supports both CSV and Parquet file formats.

        :arg:
        - bucket_name (str): The S3 bucket name.
        - prefix (str): The prefix for filtering S3 objects.

        :returns: pd.DataFrame: The data from the latest file as a Pandas DataFrame.
        """
        self.logger.info(f"Retrieving data from S3 bucket: {bucket_name} with prefix: {prefix}")

        try:
            bucket = self.s3.Bucket(bucket_name)
            objects = list(bucket.objects.filter(Prefix=prefix))
            if not objects:
                self.logger.error(f"No files found in S3 bucket {bucket_name} with prefix {prefix}")
                raise ValueError(f"No files found in S3 bucket {bucket_name} with prefix {prefix}")

            objects.sort(key=lambda o: o.last_modified)
            latest_file_key = objects[-1].key
            obj = bucket.Object(latest_file_key).get()

            self.logger.info(f"Latest file retrieved from S3: {latest_file_key}")

            if latest_file_key.endswith('.csv'):
                return pd.read_csv(obj['Body'])
            elif latest_file_key.endswith('.parquet'):
                return pd.read_parquet(obj['Body'], engine='pyarrow')
            else:
                self.logger.error(f"Unsupported file type for file: {latest_file_key}")
                raise ValueError(f"Unsupported file type for file: {latest_file_key}")
        except Exception as e:
            self.logger.error(f"Error retrieving data from S3: {str(e)}")
            raise

    def _retrieve_from_database(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query on the database and returns the result as a Pandas DataFrame.

        :arg:
        - query (str): The SQL query to execute.

        :returns: pd.DataFrame: The result of the query as a Pandas DataFrame.
        """
        self.logger.info(f"Executing SQL query")
        try:
            df = pd.read_sql(query, self.db_conn)
            self.logger.info(f"SQL query executed successfully")
            return df
        except Exception as e:
            self.logger.error(f"Error executing SQL query: {e}")
            raise ValueError(f"Error executing SQL query: {e}")

    @staticmethod
    def _retrieve_from_local(file_path: str) -> pd.DataFrame:
        """
        Loads data from a local CSV or Parquet file into a Pandas DataFrame.

        :arg:
        - file_path (str): The path to the local file.

        :returns: pd.DataFrame: The data from the local file as a Pandas DataFrame.
        """
        GenericReportProcessor.logger.info(f"Retrieving data from local file: {file_path}")
        try:
            if file_path.endswith('.csv'):
                return pd.read_csv(file_path)
            elif file_path.endswith('.parquet'):
                return pd.read_parquet(file_path, engine='pyarrow')
            else:
                GenericReportProcessor.logger.error(f"Unsupported file type for file: {file_path}")
                raise ValueError(f"Unsupported file type for file: {file_path}")
        except Exception as e:
            GenericReportProcessor.logger.error(f"Error retrieving data from local file: {str(e)}")
            raise

    @staticmethod
    def process_data(dataframes: dict[str, pd.DataFrame], processing_functions: dict[str, callable]) -> dict[str, pd.DataFrame]:
        """
        Processes multiple DataFrames using their respective custom processing functions.

        :arg:
        - dataframes (dict[str, pd.DataFrame]): A dictionary where keys are DataFrame names and values are the DataFrames to process.
        - processing_functions (dict[str, callable]): A dictionary where keys are DataFrame names and values are the custom processing functions.

        :returns: dict[str, pd.DataFrame]: A dictionary of processed DataFrames, with the same keys as input.
        """
        GenericReportProcessor.logger.info(f"Processing data using processing functions")
        processed_dataframes = {}

        try:
            for name, dataframe in dataframes.items():
                if name in processing_functions:
                    processed_dataframes[name] = processing_functions[name](dataframe)
                else:
                    processed_dataframes[name] = dataframe  # If no processing function, return the DataFrame as is
            GenericReportProcessor.logger.info(f"Data processed successfully")
            return processed_dataframes
        except Exception as e:
            GenericReportProcessor.logger.error(f"Error processing data: {e}")
            raise ValueError(f"Error processing data: {e}")

    @staticmethod
    def combine_data(processed_dataframes: dict[str, pd.DataFrame], final_processing_function: callable) -> pd.DataFrame:
        """
        Applies a custom function to combine multiple processed DataFrames into a final DataFrame.

        :arg:
        - processed_dataframes (dict[str, pd.DataFrame]): A dictionary of processed DataFrames.
        - final_processing_function (callable): A custom function to merge or combine the processed DataFrames.

        :returns: pd.DataFrame: The final combined DataFrame.
        """
        GenericReportProcessor.logger.info(f"Combining processed data using final processing function")
        try:
            df = final_processing_function(processed_dataframes)
            GenericReportProcessor.logger.info(f"Data combined successfully")
            return df
        except Exception as e:
            GenericReportProcessor.logger.error(f"Error combining data: {e}")
            raise ValueError(f"Error combining data: {e}")

    @staticmethod
    def calculate_metrics(data: pd.DataFrame, metrics_definitions: dict) -> pd.DataFrame:
        """
        Calculates metrics based on user-defined logic.

        :arg:
        - data (pd.DataFrame): The processed data.
        - metrics_definitions (dict): A dictionary where keys are metric names and values are functions to calculate those metrics.

        :returns: pd.DataFrame: A DataFrame containing the calculated metrics.
        """
        GenericReportProcessor.logger.info(f"Calculating metrics")
        metrics = {}
        try:
            for metric_name, metric_function in metrics_definitions.items():
                metrics[metric_name] = metric_function(data)

            df = pd.DataFrame([metrics])
            GenericReportProcessor.logger.info(f"Metrics calculated successfully")
            return df
        except Exception as e:
            GenericReportProcessor.logger.error(f"Error calculating metrics: {e}")
            raise ValueError(f"Error calculating metrics: {e}")

    def export_data(self, data: pd.DataFrame, export_type: str, *, file_path: str = None, sheet_id: str = None, sheet_name: str = None, first_time: bool = False, date_columns: list = None) -> None:
        """
        Exports data to various destinations such as a CSV file or a Google Sheets document.

        :arg: (Required):
        - data (pd.DataFrame): The data to be exported.
        - export_type (str): The type of export ('csv', 'parquet', or 'google_sheets').

        :arg: (Optional based on export type):
        - file_path (str): The path to save the CSV or Parquet file (required if export_type is 'csv' or 'parquet').
        - sheet_id (str): The Google Sheets document ID (required if export_type is 'google_sheets').
        - sheet_name (str): The worksheet name within the Google Sheets document (required if export_type is 'google_sheets').
        - first_time (bool): Whether this is the first time the data is being populated in Google Sheets.
        - date_columns (list): List of columns containing date values to be cast to strings.

        :returns: None
        :raises: ValueError if the export type is not supported or if required parameters are missing.
        """
        self.logger.info(f"Exporting data to {export_type}")

        try:
            if not isinstance(data, pd.DataFrame):
                self.logger.error("The data provided must be a pandas DataFrame.")
                raise TypeError("The data provided must be a pandas DataFrame.")

            if export_type in ['csv', 'parquet']:
                if not file_path:
                    self.logger.error("The 'file_path' is required for CSV or Parquet export.")
                    raise ValueError("The 'file_path' is required for CSV or Parquet export.")
                self._export_to_csv_parquet(data, file_path)

            elif export_type == 'google_sheets':
                if not sheet_id or not sheet_name:
                    self.logger.error("Both 'sheet_id' and 'sheet_name' are required for Google Sheets export.")
                    raise ValueError("Both 'sheet_id' and 'sheet_name' are required for Google Sheets export.")
                self._export_to_google_sheets(data, sheet_id, sheet_name, first_time, date_columns)

            else:
                self.logger.error(f"Unsupported export type: {export_type}")
                raise ValueError(f"Unsupported export type: {export_type}")

        except Exception as e:
            self.logger.error(f"Error exporting data: {e}")
            raise

    @staticmethod
    def _export_to_csv_parquet(data: pd.DataFrame, file_path: str) -> None:
        """
        Exports data to either a CSV or Parquet file depending on the file extension.

        :args:
        - data (pd.DataFrame): The data to be exported.
        - file_path (str): The path to save the file. The file extension determines the format:
            - '.csv' for CSV export.
            - '.parquet' for Parquet export.

        :returns: None
        :raises: ValueError if the file extension is not supported.
        """
        try:
            if file_path.endswith('.csv'):
                data.to_csv(file_path, index=False)
                GenericReportProcessor.logger.info(f"Data successfully exported to CSV file: {file_path}")
            elif file_path.endswith('.parquet'):
                data.to_parquet(file_path, index=False, engine='pyarrow')
                GenericReportProcessor.logger.info(f"Data successfully exported to Parquet file: {file_path}")
            else:
                raise ValueError(f"Unsupported file extension for file: {file_path}. Supported extensions are '.csv' and '.parquet'.")
        except (ValueError, Exception) as e:
            GenericReportProcessor.logger.error(f"Error exporting data to file {file_path}: {e}")
            raise

    def _export_to_google_sheets(self, data: pd.DataFrame, sheet_id: str, sheet_name: str, first_time: bool = False, date_columns: list = None) -> None:
        """
        Exports data to a Google Sheets document.

        :args:
        - data: pd.DataFrame, The data to be exported.
        - sheet_id: str, The Google Sheets document ID.
        - sheet_name: str, The worksheet name within the document.
        - first_time: bool, Whether this is the first time the data is being populated in Google Sheets.
        - date_columns: list, List of columns containing date values to be cast to strings.

        :returns: None
        :raises: Exception if an error occurs during the export process
        """
        try:
            # Cast specified date columns to string
            if date_columns:
                for col in date_columns:
                    if col in data.columns:
                        data[col] = data[col].astype(str)
            else:
                # Automatically detect and convert datetime columns
                date_columns_auto = data.select_dtypes(include=['datetime']).columns
                data[date_columns_auto] = data[date_columns_auto].astype(str)

            gs = self.gc.open_by_key(sheet_id)
            worksheet = gs.worksheet(sheet_name)

            if first_time:
                # Clear the sheet and populate it from scratch
                worksheet.clear()
                gs.set_with_dataframe(worksheet=worksheet, dataframe=data, include_index=False, include_column_header=True, resize=True)
            else:
                # Append data to Google Sheet
                data_values = data.values.tolist()
                gs.values_append(sheet_name, {'valueInputOption': 'RAW'}, {'values': data_values})

            self.logger.info(f"Data exported to Google Sheets document: {sheet_id} - {sheet_name}")
        except Exception as e:
            self.logger.error(f"Error exporting data to Google Sheets: {e}")
            raise


    def send_slack_notification(self, channel_name, message):
        """
        Sends a notification to a Slack channel.

        :args:
        - channel_name: str, Slack channel to send the message to
        - message: str, the message content

        :returns: None
        :raises: ValueError if the Slack token is missing.
        """
        self.logger.info(f"Sending Slack notification to channel: {channel_name}")
        try:
            if self.slack:
                full_message = f"{message} for the week of {self.current_week.strftime('%Y-%m-%d')}"
                self.slack.chat_postMessage(channel=channel_name, text=full_message)
                self.logger.info(f"Slack notification sent successfully")
            else:
                self.logger.error("Slack token not found. Cannot send Slack notification.")
                raise ValueError("Slack token not found. Cannot send Slack notification.")
        except Exception as e:
            self.logger.error(f"Error sending Slack notification: {e}")
            raise