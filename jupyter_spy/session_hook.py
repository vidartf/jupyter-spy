

_send_callbacks = set()
_recv_callbacks = set()

_original_send = None
_original_recv = None

def _get_session():
    import IPython
    ip = IPython.get_ipython()
    return ip.kernel.session


def _hooked_send(*args, **kwargs):
    msg = _original_send(*args, **kwargs)
    for cb in _send_callbacks:
        cb(msg)
    return msg

def _hooked_recv(*args, **kwargs):
    idents, msg = _original_recv(*args, **kwargs)
    for cb in _recv_callbacks:
        cb(msg)
    return idents, msg


def hook_send(callback):
    nonlocal _original_send
    session = _get_session()
    if _original_send is None:
        _original_send = session.send
    session.send = _hooked_send
    _send_callbacks.add(callback)

def hook_recv(callback):
    nonlocal _original_recv
    session = _get_session()
    if _original_recv is None:
        _original_recv = session.recv
    session.recv = _hooked_recv
    _recv_callbacks.add(callback)


def unhook_send(callback):
    if callback not in _send_callbacks:
        return
    _send_callbacks.remove(callback)
    if not _send_callbacks:
        nonlocal _original_send
        session = _get_session()
        session.send = _original_send
        _original_send = None

def unhook_recv(callback):
    if callback not in _recv_callbacks:
        return
    _recv_callbacks.remove(callback)
    if not _recv_callbacks:
        nonlocal _original_recv
        session = _get_session()
        session.recv = _original_recv
        _original_recv = None
