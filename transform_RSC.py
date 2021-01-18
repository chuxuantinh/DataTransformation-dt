# automatic transformation for publications from RSC
import json # packadge used for json files
import xmltodict # packadge used for xml files
import copy # packadge to copy nested dict variables
import os, send2trash
from pyExcelReader import pyExlDict, from_DOI # functions to read from .xlsx or .csv files
from urlRequest import loginRequest, affRequest, upfileRequest, itemsRequest # functions to interact with PuRe via REST API
import logging
logging.basicConfig( filename="RSC.log",
                     filemode='a',
                     level=logging.INFO,
                     format= '%(asctime)s - %(levelname)s - %(message)s',
                   )
# ==== supplement mapping files ====
fileMonthNum = './/subsidiary_doc//Month.xlsx'
fileAbbrRSC = './/subsidiary_doc//Abbr-RSC.xlsx'
fileDOIaff = './/30759//rsc_201701-201807.csv' # file name need to be changed
dictMonthNum = pyExlDict(fileMonthNum)
dictAbbrJournal = pyExlDict(fileAbbrRSC)

desiredPath = './30759' # folder name need to be changed

# ==== query: log in - get token ====
namePass = "name:password"
Token = loginRequest(namePass)

# ==== help function for transformation ====
def findByValue(value, orglist):
    """
    Return the affaliation dict whose attribute id = value.
    orglist is a list of all affaliation dicts.
    """
    if type(orglist)!=list:
        orglist = [orglist]
    for org in orglist:
        if org['@id'] == value:
            return org
def xmlNamesPaths(desiredPath):
    """
    Return the iterative list of file names, filepaths and folder paths in the desiredPath folder
    """
    for root, _, filenames in os.walk(desiredPath):
        for f in filenames:
            if f.endswith('.xml'):
                filepath = os.path.join(root, f)
                yield os.path.splitext(f)[0], filepath, root
def flatten(lst):
    """
    Put a nested list into a flat list
    """
    new_lst = []
    flatten_helper(lst, new_lst)
    return new_lst

def flatten_helper(lst, new_lst):
	for element in lst:
		if isinstance(element, list):
			flatten_helper(element, new_lst)
		else:
			new_lst.append(element)

# ==== read the text of doi of transformed files
with open("transformed_RSC.txt", "r") as text_file:
    doi_list_done = text_file.read().split('\n')
    
# ==== process iteratively for all the .xml in the folders and subforders
for name, filePath, folderPath in xmlNamesPaths(desiredPath):
    # print(name, filePath, folderPath)                
    transformedFileName = name

    # ==== load xml metadata dict to read from ====
    with open(filePath, 'r', encoding="utf8") as f:
        xmlString = f.read()
    xmlDict = json.loads(json.dumps(xmltodict.parse(xmlString), indent = 2))# dict of xml content
    xmlArt = xmlDict['article']
    xmlAdmin = xmlArt['art-admin']
    xmlFront = xmlArt['art-front']
    # ==== load json metadata template dict to write in ====
    with open("tempjson.json", 'r') as fj:
        jsonString = fj.read()
    jsondict = json.loads(jsonString) # dict of json template
    # jsondict['context']['objectId'] = 'ctx_persistent3' #ctxID # 

    # ==== transformation process ====
    metaData = jsondict['metadata']
    
    # ---- doi ----
    DOI = xmlAdmin['doi']
    ctxID, ouID = from_DOI(fileDOIaff, xmlAdmin['doi'])
    """
    ctx id of the one who will receive this publication item: 
    ctx_persistent3 is mpdl inner Id for testing purpose
    ctxID is the one to whom the item should be sent to
    change 'ctx_persistent3' to ctxID in the following line when doing real data transformation and uploading
    """
    print(DOI)
    if DOI in doi_list_done:
        """
        if doi is in doi_list_done, this means it has already been transformed and uploaded.
        """
        send2trash.send2trash(folderPath)
        logging.warning("File: %s, DOI: %s has been uploaded." % (transformedFileName, DOI))
        continue
    if ('xxx' in ctxID):
        """
        'xxx' in ctxID means there's no matching for this doi, so do not do transformation or uploading
        """
        logging.warning("File: %s, DOI: %s has no ctxID matching." % (transformedFileName, DOI))
        continue 
        
    metaData['identifiers'][0]['id'] = xmlAdmin['doi']
    jsondict['context']['objectId'] = ctxID
    # ---- search for title ----
    if isinstance(xmlFront['titlegrp']['title'], dict):
        metaData['title'] = xmlFront['titlegrp']['title']['#text']
    else:
        metaData['title'] = xmlFront['titlegrp']['title']
    print('title: %s' % metaData['title'])

    # ---- add authors ----
    authors = xmlFront['authgrp']['author']
    orgs = xmlFront['authgrp']['aff']
    creator = metaData['creators'][0]
    metaData['creators'] = []
    if isinstance(authors, list): # for the case there are multiple authors
        for author in authors:
            creatorTemp = creator.copy()
            creatorTemp['person']['givenName'] = author['person']['persname']['fname']
            if isinstance(author['person']['persname']['surname'], dict):
                creatorTemp['person']['familyName'] = author['person']['persname']['surname']['#text']
            else:
                creatorTemp['person']['familyName'] = author['person']['persname']['surname']
            aff = author['@aff'].split()
            creatorTemp['person']['organizations'] = []
            for affele in aff:
                org = findByValue(affele, orgs)
                name = org['org']['orgname']['nameelt']
                if isinstance(name, list):
                    try:
                        name = ', '.join(name)
                    except TypeError as e:
                        name = ''
                        logging.error(e)
                # --== query: affiliation Id ==--
                ouId = affRequest(name, ouID)
                address = ', '.join(flatten(list(org['address'].values())))
                creatorTemp['person']['organizations'].append(
                    {'identifier': ouId, 'name': name, 
                    'address':address})
            storeCreator = copy.deepcopy(creatorTemp)
            metaData['creators'].append(storeCreator)
    else:   
        author = authors # for the case there is only one author
        creatorTemp = creator.copy()
        creatorTemp['person']['givenName'] = author['person']['persname']['fname']
        if isinstance(author['person']['persname']['surname'], dict):
            creatorTemp['person']['familyName'] = author['person']['persname']['surname']['#text']
        else:
            creatorTemp['person']['familyName'] = author['person']['persname']['surname']
        aff = author['@aff'].split()
        creatorTemp['person']['organizations'] = []
        for affele in aff:
            org = findByValue(affele, orgs)
            name = org['org']['orgname']['nameelt']
            if isinstance(name, list):
                name = ', '.join(name)
            # --== query: affiliation Id ==--
            ouId = affRequest(name, ouID)
            address = ', '.join(flatten(list(org['address'].values())))
            creatorTemp['person']['organizations'].append(
                {'identifier': ouId, 'name': name, 
                'address':address})
        storeCreator = copy.deepcopy(creatorTemp)
        metaData['creators'].append(storeCreator)
    # ---- dates ----
    # -- dateSubmitted --
    xmlReceived = xmlAdmin['received']
    year = xmlReceived['date']['year']
    month = dictMonthNum[xmlReceived['date']['month']]
    day = xmlReceived['date']['day']
    if len(day) < 2:
        day = '0' + day
    metaData['dateSubmitted'] = year +'-' + month + '-' + day
    # print((metaData['dateSubmitted']))
    
    # -- dateAcceptd --
    Flg_dateAccept = True
    try:
        xmlDate = xmlAdmin['date']
        year = xmlDate['year']
        month = dictMonthNum[xmlDate['month']]
        day = xmlDate['day']
        if len(day) < 2:
            day = '0' + day
        metaData['dateAccepted'] = year +'-' + month + '-' + day
    except KeyError:
        logging.info("File: %s, DOI: %s has no dateAccepted!" % (transformedFileName, DOI))
        del metaData['dateAccepted']
    # print(metaData['dateAccepted'])
    
    # -- dateModiefied --
    del metaData['dateModified'] # no information can be assigned
    
    # -- datePublishedOnline -- 
    xmlPub = xmlArt['published']
    year = xmlPub[0]['pubfront']['date']['year']
    if 'month' in xmlPub[0]['pubfront']['date'].keys():
        monthxml = xmlPub[0]['pubfront']['date']['month']
        try:
            month = dictMonthNum[monthxml]
        except:
            month = xmlPub[0]['pubfront']['date']['month']
        if len(month) < 2:
            month = '0' + month
    else:
        month = "Unassigned"
    if 'day' in xmlPub[0]['pubfront']['date'].keys():
        day = xmlPub[0]['pubfront']['date']['day']
        if len(day) < 2:
            day = '0' + day
    else:
        day = "Unassigned"
        
    if (year == "Unassigned" or month == "Unassigned" or day == "Unassigned"):
        del metaData['datePublishedOnline']
    else:
        logging.info("File: %s, DOI: %s has no datePublishedOnline!" % (transformedFileName, DOI))
        metaData['datePublishedOnline'] = year +'-' + month + '-' + day
    # print(metaData['datePublishedOnline'])
    
    # -- datePublishedInPrint -- 
    year = xmlPub[1]['pubfront']['date']['year']
    if 'month' in xmlPub[1]['pubfront']['date'].keys():
        monthxml = xmlPub[1]['pubfront']['date']['month']
        try:
            month = dictMonthNum[monthxml]
        except:
            month = xmlPub[1]['pubfront']['date']['month']
        if len(month) < 2:
            month = '0' + month
    else:
        month = "Unassigned"
    if 'day' in xmlPub[1]['pubfront']['date'].keys():
        day = xmlPub[1]['pubfront']['date']['day']
        if len(day) < 2:
            day = '0' + day
    else:
        day = "Unassigned"
    
    if (year == "Unassigned" or month == "Unassigned" or day == "Unassigned"):
        logging.info("File: %s, DOI: %s has no datePublishedInPrint!" % (transformedFileName, DOI))
        del metaData['datePublishedInPrint']
    else:
        metaData['datePublishedInPrint'] = year +'-' + month + '-' + day
    # print(metaData['datePublishedInPrint'])
    
    # ---- event ----
    del metaData['event'] # no infomation can be assigned
    
    # ---- sources ----
    source = metaData['sources'][0]
    source['title'] = dictAbbrJournal[xmlPub[0]['journalref']['link']]
    source['volume'] = xmlPub[1]['volumeref']['link']
    source['issue'] = xmlPub[1]['issueref']['link']
    source['startPage'] = xmlPub[1]['pubfront']['fpage']
    source['endPage'] = xmlPub[1]['pubfront']['lpage']
    # print(source)

    # ---- freeKeywords ----
    if isinstance(xmlArt['art-front']['art-toc-entry']['ictext'], dict):
        metaData['freeKeywords'] =  xmlArt['art-front']['art-toc-entry']['ictext']['#text']
    else:
        metaData['freeKeywords'] =  xmlArt['art-front']['art-toc-entry']['ictext']

    # ---- abstracts ----list
    if isinstance(xmlArt['art-front']['abstract']['p'], dict):
        metaData['abstracts'][0]['value'] = xmlArt['art-front']['abstract']['p']['#text']
    else:
        metaData['abstracts'][0]['value'] = xmlArt['art-front']['abstract']['p']
    
    # ---- subjects ----
    del metaData['subjects'] # no information can be assigned

    # ---- num of pages ----
    metaData['totalNumberOfPages'] = xmlPub[1]['pubfront']['no-of-pages']

    # ---- projectInfo ----list
    if 'art-links' in xmlArt.keys():
        xmlLinks = xmlArt['art-links']
        if 'fundgrp' in xmlLinks.keys():
            project = metaData['projectInfo'][0]
            if isinstance(xmlLinks['fundgrp']['funder'], dict):
                project['grantIdentifier']['id'] = xmlLinks['fundgrp']['funder']['award-number']
                project['fundingInfo']['fundingOrganization']['title'] = xmlLinks['fundgrp']['funder']['funder-name']
            elif isinstance(xmlLinks['fundgrp']['funder'], list):
                metaData['projectInfo'] = []
                for funder in xmlLinks['fundgrp']['funder']:
                    projectTemp = project.copy()
                    grantId = funder['award-number']
                    if isinstance(grantId, list):
                        grantId = ','.join(grantId)
                    projectTemp['grantIdentifier']['id'] = grantId
                    projectTemp['fundingInfo']['fundingOrganization']['title'] = funder['funder-name']
                    projectStore = copy.deepcopy(projectTemp)
                    metaData['projectInfo'].append(projectStore)
            else:
                pass
        else:
            logging.info("File: %s, DOI: %s has no projectInfo" % (transformedFileName, DOI))
            del metaData['projectInfo']
    else:
        logging.info("File: %s, DOI: %s has no projectInfo" % (transformedFileName, DOI))
        del metaData['projectInfo']
    
    # ---- files ---- list
    pdfName = transformedFileName.upper() + '.pdf'
    jsondict['files'][0]['metadata']['title'] = pdfName
    
    # --== query: staging - uploading files ==-- 
    pdfPath = folderPath +'\\' + pdfName
    upfileId = upfileRequest(Token, pdfPath, pdfName)
    if upfileId == "No PDF":
        logging.info("File: %s, DOI: %s has no PDF attached" % (transformedFileName, DOI))
        del jsondict['files']
    elif isinstance(upfileId, str):
        jsondict['files'][0]['content'] = upfileId
    else:
        logging.error("File: %s, DOI: %s has PDF staging error" % (transformedFileName, DOI))

    jsonwrite = json.dumps(jsondict, indent=2) # conver json obj to string to upload via REST API
    
    # --== query: items - publication ==--
    res = itemsRequest(Token, jsonwrite)
    if not res.ok:
        logging.error("File: %s, DOI: %s" % (transformedFileName, DOI) +res.text, exc_info=True)
    else:
        logging.info("File: %s, DOI: %s is submitted successfully" % (transformedFileName, DOI))
        # ==== add the doi and name of successful transformed file in to the list ====
        with open("transformed_RSC.txt", "a") as text_file:
            text_file.write("%s\n" % DOI)
        # ==== remove the folder of the xml metadata and pdf, since they are succussfully uploaded ====
        send2trash.send2trash(folderPath)