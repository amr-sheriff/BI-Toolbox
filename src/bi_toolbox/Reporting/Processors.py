import os
from typing import Optional, Callable, Any
import logging
from logging.handlers import RotatingFileHandler
import boto3
import pandas as pd
from datetime import date, timedelta
from slack_sdk import WebClient
import gspread
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from sqlalchemy import create_engine
from bi_toolbox.utils.CredManager import CredManager
from gspread_dataframe import set_with_dataframe#, get_as_dataframe


class GenericReportProcessor:
    logger = None
    log_file_path = None
    log_level = None
    max_log_size = None
    backup_count_size = None

    def __init__(self, cred_manager: callable = CredManager(), log_file_path: str = 'logs/default.log', log_level: str = 'INFO',
                 max_log_size: int = 10485760, backup_count_size: int = 3):
        """
        Initialize the processor with a configurable log file path, log level, and log rotation
        based on file size.

        :param log_file_path: Path to the log file.
        :param log_level: Logging level as a string (e.g., 'DEBUG', 'INFO').
        :param max_log_size: Maximum log file size in bytes before rotating based on size.
        :param backup_count_size: Number of backup log files to retain after rotation based on size.
        """
        log_dir = os.path.dirname(log_file_path)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            print(f"Created log directory: {log_dir}")

        if GenericReportProcessor.logger is None:
            self._initialize_logger(log_file_path, log_level, max_log_size, backup_count_size)

        self.cred_manager = cred_manager
        self._s3 = None
        self._slack = None
        self._gc = None
        self._drive = None
        self._db_conn = None
        self.current_week: Optional[date] = None
        self.last_week: Optional[date] = None
        self.second_week_before: Optional[date] = None
        self.third_week_before: Optional[date] = None

    @classmethod
    def _initialize_logger(cls, log_file_path: str, log_level: str, max_log_size: int, backup_count_size: int) -> None:
        """
        Class method to initialize the logger with file size-based rotation.

        :param log_file_path: Path to the log file.
        :param log_level: Logging level as a string (e.g., 'DEBUG', 'INFO').
        :param max_log_size: Maximum log file size in bytes before rotating based on size.
        :param backup_count_size: Number of backup log files to retain after size-based rotation.
        """
        cls.logger = logging.getLogger(__name__)
        cls.logger.setLevel(logging.getLevelName(log_level.upper()))

        if not cls.logger.handlers:
            file_size_handler = RotatingFileHandler(log_file_path, maxBytes=max_log_size, backupCount=backup_count_size)
            file_size_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_size_handler.setFormatter(file_size_format)
            cls.logger.addHandler(file_size_handler)

            console_handler = logging.StreamHandler()
            console_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_format)
            cls.logger.addHandler(console_handler)

            cls.log_file_path = log_file_path
            cls.log_level = log_level
            cls.max_log_size = max_log_size
            cls.backup_count_size = backup_count_size

            cls.logger.info(f"Logger initialized. Logging to file: {log_file_path} with both size-based and time-based rotation, and console.")

    @classmethod
    def set_logger_path(cls, path: str) -> None:
        """
        Class method to change the logger file path.

        :param path: str - The new path for the log file.
        :return: None
        """
        if cls.logger is not None:
            for handler in cls.logger.handlers[:]:
                if isinstance(handler, RotatingFileHandler):
                    cls.logger.removeHandler(handler)

            file_size_handler = RotatingFileHandler(path, maxBytes=cls.max_log_size, backupCount=cls.backup_count_size)
            file_size_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            cls.logger.addHandler(file_size_handler)

            cls.logger.info(f"Logger file path changed to {path}")

    @classmethod
    def factory(cls, services: Optional[list] = None, eager: bool = False):
        """
        Factory method to initialize the GenericReportProcessor with selected services.

        :param services: List of services to initialize (like 's3', 'slack', 'google_drive', 'redshift').
                         If None, initialize all services.
        :param eager: Boolean flag to initialize all services upfront if True (lazy initialization by default).
        :return: GenericReportProcessor instance
        """
        instance = cls()
        instance.setup_environment(services)

        if eager:
            instance.logger.info("Eager initialization mode enabled. Initializing all services upfront.")
            for service in services:
                if service == 's3':
                    _ = instance.s3
                if service == 'slack':
                    _ = instance.slack
                if service == 'google_drive':
                    _ = instance.gc
                    _ = instance.drive
                if service == 'redshift':
                    _ = instance.db_conn
            instance.logger.info("All services initialized eagerly.")
        else:
            instance.logger.info("Lazy initialization mode enabled. Services will be initialized on demand.")

        return instance

    @property
    def s3(self):
        if self._s3 is None:
            self.logger.debug("Initializing S3 service.")
            aws_access_key_id, aws_secret_access_key = self.cred_manager.get_aws_credentials()
            if aws_access_key_id and aws_secret_access_key:
                self._s3 = boto3.resource(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )
                self.logger.info("S3 initialized successfully.")
            else:
                self.logger.warning("AWS S3 credentials missing. S3 data retrieval will be unavailable.")
                self._s3 = None
        return self._s3

    # Lazy initialization
    @property
    def slack(self):
        if self._slack is None:
            self.logger.debug("Initializing Slack client.")
            slack_token = self.cred_manager.get_slack_token()
            if slack_token:
                self._slack = WebClient(slack_token)
                self.logger.info("Slack client initialized successfully.")
            else:
                self.logger.warning("Slack token not found. Slack notifications will be unavailable.")
                self._slack = None
        return self._slack

    @property
    def gc(self):
        if self._gc is None:
            self.logger.debug("Initializing Google Sheets client.")
            google_creds = self.cred_manager.get_google_credentials()
            if google_creds:
                self._gc = gspread.authorize(google_creds)
                self.logger.info("Google Sheets client initialized successfully.")
            else:
                self.logger.warning("Google credentials missing. Google Sheets functionality will be unavailable.")
                self._gc = None
        return self._gc

    @property
    def drive(self):
        if self._drive is None:
            self.logger.debug("Initializing Google Drive client.")
            gauth = GoogleAuth()
            self._drive = GoogleDrive(gauth)
            self.logger.info("Google Drive client initialized successfully.")
        return self._drive

    @property
    def db_conn(self):
        if self._db_conn is None:
            self.logger.debug("Initializing Redshift connection.")
            db_credentials = self.cred_manager.get_db_credentials()
            if db_credentials:
                try:
                    engine = create_engine(f'redshift+psycopg2://{db_credentials["user"]}:{db_credentials["password"]}@{db_credentials["host"]}:5439/{db_credentials["db_name"]}',
                                           connect_args={"keepalives": 1, "keepalives_idle": 60, "keepalives_interval": 60})
                    self._db_conn = engine.connect()
                    self.logger.info("Redshift connection initialized successfully.")
                except Exception as e:
                    self.logger.error(f"Error connecting to Redshift: {e}")
                    raise ConnectionError(f"Failed to connect to Redshift: {e}")
            else:
                self.logger.warning("Redshift credentials missing. Database functionality will be unavailable.")
                self._db_conn = None
        return self._db_conn

    def calculate_dates(self):
        """
        Calculate week and date attributes for easier reference throughout the class.
        """
        today = date.today()
        self.current_week = today - timedelta(days=today.weekday())
        self.last_week = self.current_week - timedelta(days=7)
        self.second_week_before = self.current_week - timedelta(days=14)
        self.third_week_before = self.current_week - timedelta(days=21)

    def setup_environment(self, services: Optional[list] = None) -> None:
        """
        Loads environment variables and sets up configurations.
        Services (S3, Slack, Google Sheets, Redshift) are not initialized here,
        but will be lazily initialized when accessed.

        :param services: Optional list of services to configure (e.g., ['s3', 'slack', 'google_drive']).
                         If None, it assumes all services should be configured.
        """
        self.logger.info("Setting up the environment for the required services.")
        self.calculate_dates()

        if services is None:
            services = ['s3', 'slack', 'google_drive', 'redshift']

        for service in services:
            try:
                if service == 'slack' and not self.cred_manager.get_slack_token():
                    self.logger.warning("Slack is not properly configured. SLACK_TOKEN is missing.")
                if service == 's3' and (not self.cred_manager.get_aws_credentials()[0] or not self.cred_manager.get_aws_credentials()[1]):
                    self.logger.warning("S3 is not properly configured. AWS credentials are missing.")
                if service == 'google_drive' and not self.cred_manager.get_google_credentials():
                    self.logger.warning("Google Drive/Sheets is not properly configured. Google credentials are missing.")
                if service == 'redshift' and (not self.cred_manager.get_db_credentials().get('user')
                        or not self.cred_manager.get_db_credentials().get('password')
                        or not self.cred_manager.get_db_credentials().get('host')
                        or not self.cred_manager.get_db_credentials().get('db_name')):
                    self.logger.warning("Redshift is not properly configured. Redshift credentials are missing.")
            except ValueError as e:
                self.logger.error(f"Error setting up environment for service {service}: {e}")
                raise

        self.logger.info("Environment setup complete. Services will be initialized lazily when accessed.")

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
        # TODO: Implement the data retrieval logic from google sheets
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
        self.logger.debug(f"Attempting to retrieve data from S3 bucket: {bucket_name} with prefix: {prefix}")
        if not self.s3:
            raise ValueError("S3 is not initialized. Cannot retrieve data.")

        try:
            bucket = self.s3.Bucket(bucket_name)
            objects = list(bucket.objects.filter(Prefix=prefix))
            if not objects:
                self.logger.error(f"No files found in S3 bucket {bucket_name} with prefix {prefix}")
                raise ValueError(f"No files found in S3 bucket {bucket_name} with prefix {prefix}")

            objects.sort(key=lambda o: o.last_modified)
            latest_file_key = objects[-1].key
            self.logger.debug(f"Latest file found in S3: {latest_file_key}")

            obj = bucket.Object(latest_file_key).get()

            if latest_file_key.endswith('.csv'):
                return pd.read_csv(obj['Body'])
            elif latest_file_key.endswith('.parquet'):
                return pd.read_parquet(obj['Body'], engine='pyarrow')
            else:
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
        self.logger.debug("Attempting to execute SQL query.")
        if not self.db_conn:
            raise ValueError("Redshift connection is not initialized. Cannot retrieve data.")

        try:
            df = pd.read_sql(query, self.db_conn)
            self.logger.info(f"SQL query executed successfully")
            return df
        except Exception as e:
            self.logger.error(f"Error executing SQL query: {e}")
            raise

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

    # TODO: Implement _retrieve_from_google_sheets method

    @staticmethod
    def process_data(dataframes: dict[str, pd.DataFrame], processing_functions: dict[str, Callable[[pd.DataFrame], pd.DataFrame]]) -> dict[str, pd.DataFrame]:
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
                    processed_dataframes[name] = dataframe
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
    def calculate_metrics(data: pd.DataFrame, metrics_definitions: dict[str, Callable[[pd.DataFrame], Any]]) -> pd.DataFrame:
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
        self.logger.debug(f"Attempting to export data to Google Sheets: {sheet_name} in document {sheet_id}")

        if not self.gc:
            raise ValueError("Google Sheets client is not initialized. Cannot export data.")
        if not self.drive:
            raise ValueError("Google Drive client is not initialized. Cannot export data.")

        try:
            if date_columns:
                for col in date_columns:
                    if col in data.columns:
                        data[col] = data[col].astype(str)
            else:
                date_columns_auto = data.select_dtypes(include=['datetime']).columns
                data[date_columns_auto] = data[date_columns_auto].astype(str)

            gs = self.gc.open_by_key(sheet_id)
            worksheet = gs.worksheet(sheet_name)

            if first_time:
                worksheet.clear()
                set_with_dataframe(worksheet=worksheet, dataframe=data, include_index=False, include_column_header=True, resize=True)
            else:
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
        if not self.slack:
            raise ConnectionError("Slack is not initialized. Cannot send notification.")

        try:
            full_message = f"{message} for the week of {self.current_week.strftime('%Y-%m-%d')}"
            self.slack.chat_postMessage(channel=channel_name, text=full_message)
            self.logger.info(f"Slack notification sent successfully")
        except Exception as e:
            self.logger.error(f"Error sending Slack notification: {e}")
            raise
