from MongoDB_to_PostgreSQL import *

buids = []


def init():
    get_buids()


def get_buids():
    for entry in db["profiles"].find():
        if "buids" in entry:
            if len(entry["buids"]) > 0:
                buids.append((entry["buids"][0], str(entry["_id"])))
    buids.sort()
    print("Done with the setup of the buids.")


def link_profile_session(entry):
    low = 0
    high = len(buids) - 1
    session_id = entry["buid"][0]

    while low <= high:
        middle = (low + high) // 2
        if buids[middle][0] == session_id:
            return buids[middle][1]
        elif buids[middle][0] > session_id:
            high = middle - 1
        else:
            low = middle + 1

    return -1
