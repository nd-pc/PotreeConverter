
import logging
from logging.handlers import RotatingFileHandler


class LoggingWrapper:
    def __init__(self, logFile):
        self.BLUE = '\033[94m'
        self.GREEN = '\033[92m'
        self.YELLOW = '\033[93m'
        self.RED = '\033[91m'
        self.BOLD = '\033[1m'
        self.ENDC = '\033[0m'
        self.logFile = logFile

        self.log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M')
        self.logger = logging.getLogger('logger')
        self.logger.setLevel(logging.DEBUG)

        # Console handler
        self.console_handler = logging.StreamHandler()
        self.console_handler.setFormatter(self.log_formatter)
        self.logger.addHandler(self.console_handler)

        # File handler
        self.file_handler = RotatingFileHandler(self.logFile, mode='a', maxBytes=5 * 1024 * 1024, backupCount=2,
                                                encoding=None, delay=False)
        self.file_handler.setFormatter(self.log_formatter)
        self.logger.addHandler(self.file_handler)

    def colors(self, color):
        if color == "blue":
            return self.BLUE
        elif color == "green":
            return self.GREEN
        elif color == "yellow":
            return self.YELLOW
        elif color == "red":
            return self.RED
        else:
            return ""

    def info(self, text, color = "",  bold = False):
        if bold:
            self.logger.info(self.colors(color) + self.BOLD + text + "\n" + self.ENDC)
        else:
            self.logger.info(self.colors(color) + text + "\n" + self.ENDC)

    def error(self, text, color ="red", bold = False):
        if bold:
            self.logger.error(self.colors(color)+ self.BOLD + text + "\n" + self.ENDC)
        else:
            self.logger.error(self.colors(color) + text + "\n" + self.ENDC)

    def warning(self, text, color = "yellow", bold = False):
        if bold:
            self.logger.warning(self.colors(color) + self.BOLD + text + "\n" + self.ENDC)
        else:
            self.logger.warning(self.colors(color) + text + "\n" +  self.ENDC)