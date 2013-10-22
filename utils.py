from functools import wraps
import tornado.concurrent


def to_future(fun):
    @wraps(fun)
    def wrapper(*args, **kwargs):
        future_callback = kwargs.pop('callback')
        def _callback(result, error=None):
            if error:
                raise error

            future_callback(result)

        kwargs['callback'] = _callback
        fun(*args, **kwargs)

    return tornado.concurrent.return_future(wrapper)