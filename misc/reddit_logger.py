import logging

root_logger = logging.getLogger(__name__)

FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
file_handler = logging.FileHandler(__name__)
file_handler.setFormatter(logging.Formatter(FORMAT))
root_logger.addHandler(file_handler)

# logging.basicConfig(format=FORMAT)

stdout_formatter = logging.Formatter(FORMAT)
stdout_handler = logging.StreamHandler()
stdout_handler.setFormatter(stdout_formatter)
root_logger.addHandler(stdout_handler)

root_logger.setLevel(logging.INFO)
