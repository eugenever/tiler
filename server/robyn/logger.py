import logging


class Logger:

    def __init__(self):
        self.logger = logging.getLogger("Robyn")
        ch = logging.StreamHandler()
        cf = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(cf)
        self.logger.addHandler(ch)

    def _format_msg(
        self,
        msg: str,
    ):
        return msg

    def error(
        self,
        msg: str,
        *args,
    ):
        self.logger.error(self._format_msg(msg), *args)

    def warn(
        self,
        msg: str,
        *args,
    ):
        self.logger.warn(self._format_msg(msg), *args)

    def info(
        self,
        msg: str,
        *args,
    ):
        self.logger.info(self._format_msg(msg), *args)

    def debug(
        self,
        msg: str,
        *args,
    ):
        self.logger.debug(self._format_msg(msg), *args)


logger = Logger()
