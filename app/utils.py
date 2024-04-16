import secrets
import string
from typing import Any, Iterable, Optional, Tuple

def print_action(role: str, action: str, message: Optional[str | bytes] = None):
    try:
        print(role + ':', action, 
            message if type(message) is str else
            message.replace(b'\r\n', b' ').decode() if type(message) is bytes
            else ''
        )
    except (UnicodeDecodeError, TypeError):
        print(role + ':', action, message)

def random_string(length) -> str:
    alphanumerics = string.ascii_letters + string.digits
    random_string = ''.join(secrets.choice(alphanumerics) for _ in range(length))
    return random_string

def inc_id(id: str) -> str:
    ts, seq = id.split('-')
    return f'{ts}-{int(seq) + 1}'

def to_pairs(array: list) -> Iterable[Tuple[Any, Any]]:
    half = len(array) // 2
    keys, values = array[:half], array[half:]
    return zip(keys, values)
