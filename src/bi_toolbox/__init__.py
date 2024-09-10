# Define package version
__version__ = "0.1.0a1"

# Import key submodules or classes from the package
from .utils import CredManager, CredManagerConfig
from .Reporting import GenericReportProcessor

# Define the public API for the package
__all__ = ["CredManager", "CredManagerConfig", "GenericReportProcessor"]
