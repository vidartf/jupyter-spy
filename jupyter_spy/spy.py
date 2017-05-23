
import sys
import json
import queue
import functools

import jupyter_client
from zmq.utils import jsonapi


def filter_comms(message):
    return message['header']['msg_type'] in ('comm_open', 'comm_msg', 'comm_close')

def filter_comms_ids(ids, message):
    return filter_comms(message) and (ids is None or message['content']['comm_id'] in ids)

def json_packer(obj):
    return jsonapi.dumps(obj, default=jupyter_client.session.date_default,
                  ensure_ascii=False, allow_nan=False).decode('utf-8')

def json_packer_pretty(obj):
    return jsonapi.dumps(obj, default=jupyter_client.session.date_default,
                  ensure_ascii=False, allow_nan=False, indent='  ').decode('utf-8')


class Spy:
    """Listener/logger for Jupyter messages."""

    def __init__(self, info=None):
        """Create a new spy.

        Either supply connection information here, or using the
        `connect` method.
        """
        self.client = jupyter_client.BlockingKernelClient()
        self._first = False
        if info is not None:
            self.connect(info)


    def connect(self, info):
        """Connect to an existing kernel to inspect.

        info should either be a dict with connection info, or a kernel
        file name like "kernel-<GUID>.json". This information can normally
        be gotten by the magic `%connect_info`.
        """
        if isinstance(info, dict):
            self.client.load_connection_info(info)
        else:
            path = jupyter_client.find_connection_file(info)
            self.client.load_connection_file(path)

    def log_all(self, output=sys.stdout):
        raise NotImplementedError('Not implemented yet!')

    def _gen_messages(self, channel):
        try:
            while True:
                try:
                    message = channel.get_msg(timeout=0.2)
                    yield message
                except queue.Empty:
                    pass
        except KeyboardInterrupt:
            pass

    def _json_dump(self, message, output, pretty):
        packer = json_packer_pretty if pretty else json_packer
        s = packer(message)
        if not self._first:
            s = ',\n' + s
        print(s, file=output, end='')

    def _log_X(self, channel, output, pretty, filter_function=None):
        if pretty is None:
            pretty = output == sys.stdout
        print('[', end=('\n' if pretty else ''), file=output)
        self._first = True
        it = self._gen_messages(channel)
        if filter_function:
            it = filter(filter_function, it)
        for message in it:
            self._json_dump(message, output, pretty)
            self._first = False
        print('\n]' if pretty else ']', file=output)

    def log_iopub(self, output=sys.stdout, pretty=None):
        """Log all messages on the IOPUB channel.

        Parameters
        ----------
        output : file
            A file-like object to log the messages to. Defaults to stdout.

        pretty: bool | None
            Whether to use pretty logging (line-endings and indentation).
            If None (default), it will only use pretty print if `output` is
            equal to stdout.
        """
        return self._log_X(self.client.iopub_channel, output, pretty)

    def log_shell(self, output=sys.stdout, pretty=None):
        """Log all messages on the SHELL channel.

        Parameters
        ----------
        output : file
            A file-like object to log the messages to. Defaults to stdout.

        pretty: bool | None
            Whether to use pretty logging (line-endings and indentation).
            If None (default), it will only use pretty print if `output` is
            equal to stdout.
        """
        return self._log_X(self.client.shell_channel, output, pretty)

    def log_stdin(self, output=sys.stdout, pretty=None):
        """Log all messages on the STDIN channel.

        Parameters
        ----------
        output : file
            A file-like object to log the messages to. Defaults to stdout.

        pretty: bool | None
            Whether to use pretty logging (line-endings and indentation).
            If None (default), it will only use pretty print if `output` is
            equal to stdout.
        """
        return self._log_X(self.client.stdin_channel, output, pretty)

    def log_comms(self, only_id=None, output=sys.stdout, pretty=None):
        """Log Comm messages.

        Parameters
        ----------
        only_id: string | string[]
            A Comm ID, or sequence of IDs to filter for.

        output : file
            A file-like object to log the messages to. Defaults to stdout.

        pretty: bool | None
            Whether to use pretty logging (line-endings and indentation).
            If None (default), it will only use pretty print if `output` is
            equal to stdout.
        """
        if isinstance(only_id, str):
            only_id = [only_id]
        if pretty is None:
            pretty = output == sys.stdout

        self._log_X(self.client.iopub_channel, output, pretty,
                    functools.partial(filter_comms_ids, only_id))

