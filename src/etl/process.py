"""Python module to process data"""

from gensim.utils import tokenize


def flatten_json(y):
    """ Flatten a dict (so it can be stored as JSONL file)"""
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def tokenize_text(text: str) -> list[str]:
    """Tokenize text

    Parameters
    ----------
    text : str
          Text to tokenize
    """

    tokens = list(tokenize(text, deacc=True, lower=True))

    return tokens
