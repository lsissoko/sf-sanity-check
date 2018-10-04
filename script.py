import json
import string
import random
import requests
import re
from collections import defaultdict, Counter


def doAuthenticate(salesforce_config):
    url = "{0}/services/oauth2/token".format(salesforce_config["salesforceUrl"])
    payload = {
        "grant_type": "password",
        "client_id": salesforce_config["clientId"],
        "client_secret": salesforce_config["clientSecret"],
        "username": salesforce_config["clientUserName"],
        "password": salesforce_config["clientPassword"]
    }

    r = requests.post(url, params=payload)
    # print("doAuthenticate()")
    # print(r.status_code)
    # print(r.json())
    if r.status_code != requests.codes.ok:
        raise ValueError("Failed to authenticate.\n{}".format(r.json()["error_description"]))
    return r.json()


def getImplementationResponse(salesforce_config, policy_number, partner, year=None):
    authorizationResponse = doAuthenticate(salesforce_config)

    def process_year(year=None):
        if year:
            return "AND Rally_Launch_Year__c = '{}'".format(year)
        else:
            return ""

    query = """
    SELECT Id,
           Rally_Launch_Year__c,
            (SELECT Id, Segmentation_IDs__c FROM Client_Affiliations__r)
     FROM Milestone1_Project__c
     WHERE Primary_Policy_Number__c = '{0}' AND Partner_Name__c='{1}'
     ORDER BY Rally_Launch_Year__c DESC
    """.format(policy_number, partner)

    # ORDER BY CreatedDate DESC
    # LIMIT 1

    payload = { "q": query }
    headers = { "Authorization": "{0} {1}".format(authorizationResponse["token_type"], authorizationResponse["access_token"]) }
    req_url = "{0}/services/data/{1}/query".format(authorizationResponse["instance_url"], salesforce_config["version"])

    r = requests.get(req_url, params=payload, headers=headers)
    # print("getImplementationResponse()")
    # print(r.url)
    # print(r.status_code)
    # print(r.json())
    # print("\n\n")
    return r.json()


def getPolicyNumbers(salesforce_config, limit=1, offset=0):
    authorizationResponse = doAuthenticate(salesforce_config)

    query = """
    SELECT Id, Primary_Policy_Number__c, Partner_Name__c
    FROM Milestone1_Project__c
    WHERE Primary_Policy_Number__c <> '' AND Partner_Name__c <> ''
    ORDER BY CreatedDate DESC
    LIMIT {}
    OFFSET {}
    """.format(max(1, limit), max(0, offset))

    payload = { "q": query }
    headers = { "Authorization": "{0} {1}".format(authorizationResponse["token_type"], authorizationResponse["access_token"]) }
    req_url = "{0}/services/data/{1}/query".format(authorizationResponse["instance_url"], salesforce_config["version"])

    r = requests.get(req_url, params=payload, headers=headers)
    # print("getPolicyNumbers()")
    # print(r.url)
    # print(r.status_code)
    # print(r.json())
    # print("\n\n")
    return r.json()


def getPVRCCodesForAffiliation(salesforce_config, affiliationId):
    """
    getAffiliationMappingResponse is a better name, but copying the Scala code for now
    """
    authorizationResponse = doAuthenticate(salesforce_config)

    query = """
    SELECT Identifier_Values__c FROM Affiliation_Mapping__c
    WHERE Affiliation_Logic_Source__c = 'PVRC' AND Client_Affiliation__c='{0}'
    """.format(affiliationId)

    payload = { "q": query }
    headers = { "Authorization": "{0} {1}".format(authorizationResponse["token_type"], authorizationResponse["access_token"]) }
    req_url = "{0}/services/data/{1}/query".format(authorizationResponse["instance_url"], salesforce_config["version"])

    r = requests.get(req_url, params=payload, headers=headers)
    # print("getPVRCCodesForAffiliation()")
    # print(r.url)
    # print(r.status_code)
    # print(r.json())
    return r.json()


if __name__ == "__main__":
    with open('data/config.json', 'r') as f:
        salesforce_config = json.load(f)['salesforce']

    pvrcPattern = re.compile("[0-9]{8}")
    LIMIT = 25
    OFFSET = 50
    MAX_LIMIT = 1513
    print("limit: {}, offset: {}\n".format(LIMIT, OFFSET))

    tab2 = "\t\t "
    tab4 = "\t\t\t\t "

    # TODO - read this from command line
    load_data = False

    if load_data:
        with open("data/policyNumbersResponse_{}_{}.json".format(LIMIT, OFFSET), "r") as f:
            policyNumbersResponse = json.load(f)

        with open("data/implementations_{}_{}.json".format(LIMIT, OFFSET), "r") as f:
            implementations = json.load(f)
    else:
        policyNumbersResponse = getPolicyNumbers(salesforce_config, limit=LIMIT, offset=OFFSET)

        with open("data/policyNumbersResponse_{}_{}.json".format(LIMIT, OFFSET), "w") as outfile:
            json.dump(policyNumbersResponse, outfile, indent=4, sort_keys=True)

        implementations = []
        for i, record in enumerate(policyNumbersResponse["records"]):
            policyNumber = record["Primary_Policy_Number__c"]
            partnerName = record["Partner_Name__c"]
            print("i: {}\tpolicyNumber: {}, partnerName: '{}'".format(i, policyNumber, partnerName))
            implementationResponse = getImplementationResponse(salesforce_config, policyNumber, partnerName)
            implementations.append({
                "policyNumber": policyNumber,
                "partnerName": partnerName,
                "implementationResponse": implementationResponse
            })

        print("")

        with open("data/implementations_{}_{}.json".format(LIMIT, OFFSET), "w") as outfile:
            json.dump(implementations, outfile, indent=4, sort_keys=True)

    for impl in implementations[:4]: # TODO remove this indexing
        partnerName = impl["partnerName"]
        policyNumber = impl["policyNumber"]
        print("\n--------------------------------")
        print("PrimaryPolicyNumber: {}".format(policyNumber))
        print("PartnerName: {}".format(partnerName))
        print("")

        implementationResponse = impl["implementationResponse"]
        year_hist = Counter([record["Rally_Launch_Year__c"] for record in implementationResponse["records"]])
        year_to_implRecord = {record["Rally_Launch_Year__c"]: record for record in implementationResponse["records"]}

        for year in sorted(year_hist, reverse=True):
            print(year)
            freq = year_hist[year]
            if freq != 1:
                print("\t BAD! {} ImplementationRecords (should be 1)".format(year_hist[year]))
            else:
                implementationRecord = year_to_implRecord[year]
                # print(implementationRecord)
                affiliationRelationResponse = implementationRecord["Client_Affiliations__r"]
                if affiliationRelationResponse is None or affiliationRelationResponse["totalSize"] == 0:
                    print("\t BAD! there are no AffiliationRelationResponse objects")
                else:
                    # print(affiliationRelationResponse)
                    for affiliationRecord in affiliationRelationResponse["records"]:
                        affiliationId = affiliationRecord["Id"]
                        segmentationId = affiliationRecord["Segmentation_IDs__c"]
                        print("--")
                        print("affiliation id: {}".format(affiliationId))
                        print("segmentation id: {}".format(segmentationId))

                        if affiliationId is None or affiliationId == "":
                            print("\t BAD! affiliation id is null")
                        elif segmentationId is None or segmentationId == "":
                            print("\t BAD! segmentationId is null")
                        else:
                            affiliationMappingResponses = getPVRCCodesForAffiliation(salesforce_config, affiliationId)
                            affiliationMappingResponseCount = affiliationMappingResponses["totalSize"]
                            print("affiliationMappingResponses count: {}".format(affiliationMappingResponseCount))
                            if affiliationMappingResponseCount != 1:
                                print("\t BAD! {} affiliationMappingResponses (should be 1)".format(affiliationMappingResponseCount))
                            else:
                                """
                                TODO - report if the pvrc codes are not all \r\n separated without any junk characters
                                """
                                affiliationMappingResponse = affiliationMappingResponses["records"][0]
                                pvrcCodes = pvrcPattern.findall(affiliationMappingResponse["Identifier_Values__c"])
                                # print(tab4 + "{}".format(pvrcCodes[:30]))
                                print("\t pvrc codes count: {}".format(len(pvrcCodes)))
            print("")
