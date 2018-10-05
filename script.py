from collections import defaultdict, Counter
import json
import optparse
import re
import requests


def clean_string(s):
    """
    Removes leading and trailing whitespace, as well as non-breaking spaces (https://www.fileformat.info/info/unicode/char/00a0/index.htm)
    """
    return s.replace(u"\\u00a0", " ").strip()


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

    if r.status_code != requests.codes.ok:
        err_trace = r.json()["error_description"]
        raise ValueError("Failed to authenticate.\n{}".format(err_trace))

    return r.json()


def getImplementationResponse(salesforce_config, policy_number, partner, year=None):
    authorizationResponse = doAuthenticate(salesforce_config)

    query = """
    SELECT Id,
           Rally_Launch_Year__c,
            (SELECT Id, Segmentation_IDs__c FROM Client_Affiliations__r)
     FROM Milestone1_Project__c
     WHERE Primary_Policy_Number__c = '{0}' AND Partner_Name__c='{1}'
     ORDER BY Rally_Launch_Year__c DESC, CreatedDate DESC
    """.format(policy_number, partner)

    payload = { "q": query }
    headers = { "Authorization": "{0} {1}".format(authorizationResponse["token_type"], authorizationResponse["access_token"]) }
    req_url = "{0}/services/data/{1}/query".format(authorizationResponse["instance_url"], salesforce_config["version"])

    r = requests.get(req_url, params=payload, headers=headers)
    return r.json()


def getPolicyNumbers(salesforce_config, limit=None, offset=None):
    authorizationResponse = doAuthenticate(salesforce_config)

    query = """
    SELECT Id, Rally_Launch_Year__c, Primary_Policy_Number__c, Partner_Name__c
    FROM Milestone1_Project__c
    WHERE Primary_Policy_Number__c <> '' AND Partner_Name__c <> ''
    ORDER BY CreatedDate DESC
    """

    if limit:
        query += " LIMIT {}".format(max(1, limit))
    if offset:
        query += " OFFSET {}".format(max(0, offset))

    payload = { "q": query }
    headers = { "Authorization": "{0} {1}".format(authorizationResponse["token_type"], authorizationResponse["access_token"]) }
    req_url = "{0}/services/data/{1}/query".format(authorizationResponse["instance_url"], salesforce_config["version"])

    r = requests.get(req_url, params=payload, headers=headers)
    return r.json()


def getAffiliationMappingResponse(salesforce_config, affiliationId):
    authorizationResponse = doAuthenticate(salesforce_config)

    query = """
    SELECT Identifier_Values__c FROM Affiliation_Mapping__c
    WHERE Affiliation_Logic_Source__c = 'PVRC' AND Client_Affiliation__c='{0}'
    """.format(affiliationId)

    payload = { "q": query }
    headers = { "Authorization": "{0} {1}".format(authorizationResponse["token_type"], authorizationResponse["access_token"]) }
    req_url = "{0}/services/data/{1}/query".format(authorizationResponse["instance_url"], salesforce_config["version"])

    r = requests.get(req_url, params=payload, headers=headers)
    return r.json()


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('--load', action="store_true", default=False)
    parser.add_option('-l', action="store", type="int")
    parser.add_option('-o', action="store", type="int")
    options, args = parser.parse_args()

    LIMIT = options.l
    OFFSET = options.o
    print("limit: {}, offset: {}\n".format(LIMIT, OFFSET))

    pvrcPattern = re.compile("[0-9]{8}")
    tab2 = "\t\t "
    tab4 = "\t\t\t\t "

    with open('data/config.json', 'r') as f:
        salesforce_config = json.load(f)['salesforce']

    if options.load:
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
            policyNumber = clean_string(record["Primary_Policy_Number__c"]) # remove potential \u00a0 from the string
            partnerName = record["Partner_Name__c"]
            print("i: {}\timplementationId: {}, launchYear: {}, policyNumber: {}, partnerName: '{}'".format(
                i, record["Id"], record["Rally_Launch_Year__c"], policyNumber, partnerName))
            implementationResponse = getImplementationResponse(salesforce_config, policyNumber, partnerName)
            implementations.append({
                "policyNumber": policyNumber,
                "partnerName": partnerName,
                "implementationResponse": implementationResponse
            })

        print("")

        with open("data/implementations_{}_{}.json".format(LIMIT, OFFSET), "w") as outfile:
            json.dump(implementations, outfile, indent=4, sort_keys=True)

    """
    TODO
        There ~1500 entries here; the following for loop won't complete because we eventually get an auth error
    """
    print(len(implementations))

    import sys  # TODO remove
    sys.exit(1) # TODO remove

    for impl in implementations:
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
                            affiliationMappingResponses = getAffiliationMappingResponse(salesforce_config, affiliationId)
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
