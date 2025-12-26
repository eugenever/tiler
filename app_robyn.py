import logging.config
import logging

from server.robyn.app import app

if __name__ == "__main__":
    logging.config.fileConfig("log_app.ini", disable_existing_loggers=False)
    app.start(host="127.0.0.1")
