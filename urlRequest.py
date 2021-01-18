import requests # a python package for url web communication interfaces
import json
def loginRequest(namePass):
    # namePass should be a string in form of "name:password"
    url = 'https://qa.pure.mpdl.mpg.de/rest/login'
    response = requests.post(url, data=namePass)
    if response.ok:
        return response.headers['Token']
    else:
        response.raise_for_status()

def affRequest(name, ouID_MPI):
    name = name.lower()
    for symb in "?/;:!~[]":
        name = name.replace(symb,'')
    name = name.replace(',',' ').replace('-',' ')
    name_part = name.split()
    internalFlag = False
    if "mpi" in name_part:
        internalFlag = True
        name_part.append("Max Planck Institute")
        name_part[0] += "^2"
        name = " ".join(name_part)
    elif (sum(["max" in p for p in name_part]) and sum(["planck" in p for p in name_part]) and sum(["institut" in p for p in name_part])):
        internalFlag = True
        name_part.append("MPI")
        name_part[0] += "^2"
        name = " ".join(name_part)
    else:
        name = " AND ".join(name_part)
    queryText = name
    query_string = {"fields": ["metadata.name", "name", "alternativeNames", "parentAffailiations"], "query": queryText}
    data = {"query": {"query_string":query_string},"size" : "5"}
    # -------- send url request to search for the ouId --------
    if ('xxx' not in ouID_MPI) and internalFlag:
        return ouID_MPI
    else:
        url = 'https://qa.pure.mpdl.mpg.de/rest/ous/search' 
        response = requests.post(url, data=json.dumps(data), headers={"Content-Type": "application/json"})
        if response.ok:
            jData = (response.json())
            if 'records' in jData.keys():
                ouId = jData['records'][0]['data']['objectId']
            elif internalFlag:
                ouId = 'ou_persistent13'
            else:
                ouId = 'ou_persistent22'
            return ouId
        else:
        # If response code is not ok (200), write the error in logging
            return ouID_MPI

def upfileRequest(Token, filePath, filename):
    # Token: Authorization Token got in the login process
    # filename: the name without extension of the file wanted to upload
    url = 'https://qa.pure.mpdl.mpg.de/rest/staging/' + filename 
    headers = {'Authorization' : Token}
    try: 
        files = {'file': open(filePath, 'rb')}
    except FileNotFoundError: # deal with the case that the corresponding pdf does not exist
        return "No PDF"
    res = requests.post(url, files = files, headers = headers) 
    if res.ok:
        return res.text
    else:
        return res

def itemsRequest(Token, jsonfile):
    """
    send request to push the json metadata into pure (into pending state)
    """
    url = 'https://qa.pure.mpdl.mpg.de/rest/items' 
    headers = {'Authorization' : Token, 'Content-Type' : 'application/json'}
    res = requests.post(url, data = jsonfile, headers = headers)
    if not res.ok:
        return res
    else:
        resjson = res.json()
        return itemsSubmit(Token, resjson['objectId'], resjson['modificationDate'])

def itemsSubmit(Token, objId, modiDate):
    """
    performing item submission after item pushing, i.e., when the item is in pending state.
    """
    url = 'https://qa.pure.mpdl.mpg.de/rest/items/'+ objId +'/submit'
    headers = {'Authorization' : Token, 'Content-Type' : 'application/json'}
    data = {"comments":"Automatic submission of " + objId + " from RSC", "lastModificationDate" : modiDate}
    res = requests.put(url, data = json.dumps(data), headers = headers)
    return res
    