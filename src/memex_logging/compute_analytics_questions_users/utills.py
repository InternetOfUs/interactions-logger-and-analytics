import json
from json import JSONDecodeError

from emoji.core import emojize


def reconstruct_string(raw_text: str) -> str:
    """
    json.loads is used to reconstruct non-ascii characters previously encoded using json.dumps
    emojize it used to reconstruct emojies previously encoded using demojize
    """
    try:
        decoded_text = json.loads(raw_text)
    except JSONDecodeError:
        decoded_text = raw_text

    return emojize(str(decoded_text), use_aliases=True)
