""" Engineer or extract new features """

import re

from gensim.utils import simple_preprocess


def extract_keywords(keywords: list[str], text: str) -> list[str]:
    """Extract keywords based on static list from text

    Parameters
    ----------
    keywords : list[str]
          List of keywords to extract
    text : str
          Text to extract keywords from
    """

    matches = []
    tokens = simple_preprocess(text, deacc=True, min_len=2, max_len=20)

    for k in keywords:
        if k.lower() in tokens:
            matches.append(k)

    return matches


def identify_extension_type(value, pattern) -> str:
    """Identify extension type based on value

    Parameters
    ----------
    value : str
          Value to identify extension type from
    pattern : re.Pattern
          Regex Pattern to identify extension type from
    """

    if value in ["Vollzeit", "Teilzeit", "Praktikum"]:
        return "employment_type"
    elif isinstance(re.search(pattern, value), re.Match):
        return "posted_n_periods_ago"
    else:
        return "other"
