from collections import Counter
from functools import lru_cache
from urllib.parse import urlencode, unquote

import requests

# not sure if all fields are needed but whatever

URL_BY_COMPANY_NAME = "https://db.offeneregister.de/openregister-ef9e802.json?sql=select+company.rowid+as+rowid%2C+company.company_number%2C+company.name%2C+company.registered_address%2C+officer.name%2C+officer.start_date%2C+officer.end_date+from+company%0D%0Aleft+join+officer+on+officer.company_id%3Dcompany.company_number%0D%0Awhere+rowid+in+%28select+rowid+from+company_fts+where+company_fts+match+%3Asearch%29&"
URL_BY_OFFICER_NAME = "https://db.offeneregister.de/openregister-ef9e802.json?sql=select+comp_name%2C+officer.rowid+as+rowid+from+officer+left+join+%28select+company.company_number+as+cn%2C+company.name+as+comp_name+from+company%29++on+officer.company_id%3Dcn+%0D%0Awhere+rowid+in+%28select+rowid+from+officer_fts+where+officer_fts+match+%3Asearch%29+order+by+id+limit+101&"


def quote(text):
    text = '"' + text + '"'
    text = urlencode({"search": text})
    return text


# TODO: pagination
def get_by_company_name(query):
    query = unquote(query)
    url = f"{URL_BY_COMPANY_NAME}{quote(query)}"
    # print(url)
    res = requests.get(url)
    if res.ok:
        return res.json()["rows"]
    print(url)
    return None


# TODO: pagination
@lru_cache(maxsize=1024)
def get_by_officier_name(query):
    url = f"{URL_BY_OFFICER_NAME}{quote(query)}"
    # print(url)
    res = requests.get(url)
    if res.ok:
        return res.json()["rows"]
    print(url)
    return None


def to_d3_format(data):
    comp2idx = {}
    off2idx = {}
    max_idx = 0

    counter = Counter()

    nodes = []
    edges = []

    for d in data:
        added = False
        if not d["comp_name"] in comp2idx:
            comp2idx[d["comp_name"]] = max_idx
            nodes.append({"id": max_idx, "name": d["comp_name"], "type": "comp"})
            max_idx += 1
            added = True

        if not d["off_name"] in off2idx:
            off2idx[d["off_name"]] = max_idx
            nodes.append({"id": max_idx, "name": d["off_name"], "type": "off"})
            max_idx += 1
            added = True

        # prevent duplicates
        if added:
            edges.append(
                {
                    "source": comp2idx[d["comp_name"]],
                    "target": off2idx[d["off_name"]],
                    "value": 1,
                }
            )
            counter[comp2idx[d["comp_name"]]] += 1
            counter[off2idx[d["off_name"]]] += 1

    # add count
    for n in nodes:
        n["count"] = counter[n["id"]]

    return {"nodes": nodes, "edges": edges}


@lru_cache(maxsize=256)
def by_company_name(name):
    officiers = get_by_company_name(name)
    if officiers is None:
        return None

    data = [{"comp_name": x[2], "off_name": x[4]} for x in officiers]
    for off in set([o[4] for o in officiers if not o[4] is None]):
        res = get_by_officier_name(off)
        if res is None:
            continue
        data += [{"comp_name": comp[0], "off_name": off} for comp in res]
    return to_d3_format(data)
