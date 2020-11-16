import contextlib
import os


@contextlib.contextmanager
def set_env(**env_vars):
    """
    Context manager to temporarily set environment variables. Restores previous
    values on leaving

    Parameters
    ----------
    **env_vars : Dict(str, str or None)
        Environment variables to set temporarily. If None, the environment
        variable will be tempoarily removed.
    """
    old_environ = dict(os.environ)
    for key, value in env_vars.items():
        if value is None:
            if key in os.environ:
                del os.environ[key]
        else:
            os.environ[key] = value

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)
