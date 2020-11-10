from ibm_watson import DiscoveryV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
import threading
from ibm_watson import ApiException
from dotenv import load_dotenv
load_dotenv()
import os

environmentId = os.getenv("ENVIRONMENTID")
collectionId = os.getenv("COLLECTIONID")
discoveryVersion = os.getenv("VERSION")
discoveryApiKey = os.getenv("IAMAPIKEY")
discoveryURL = os.getenv("DISCOVERYURL")

authenticator = IAMAuthenticator(discoveryApiKey)

discovery = DiscoveryV1(
    version=discoveryVersion,
    authenticator=authenticator
)

discovery.set_service_url(discoveryURL)

collection = discovery.get_collection(environmentId, collectionId).get_result()

totalDocuments = collection['document_counts']['available']
print("**Total number of documents in collection " + collectionId + ": " + str(totalDocuments))

# From https://github.com/IBM/discovery-files/blob/master/discofiles.py
def pmap_helper(fn, output_list, input_list, i):
    output_list[i] = fn(input_list[i])

def pmap(fn, input):
    input_list = list(input)
    output_list = [None for _ in range(len(input_list))]
    threads = [threading.Thread(target=pmap_helper,
                                args=(fn, output_list, input_list, i),
                                daemon=True)
               for i in range(len(input_list))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return output_list

def all_document_ids(discovery,
                     environmentId,
                     collectionId):
    """
    Return a list of all of the document ids found in a
    Watson Discovery collection.

    The arguments to this function are:
    discovery      - an instance of DiscoveryV1
    environment_id - an environment id found in your Discovery instance
    collection_id  - a collection id found in the environment above
    """
    doc_ids = []
    alphabet = "0123456789abcdef"   # Hexadecimal digits, lowercase
    chunk_size = 10000

    def maybe_some_ids(prefix):
        """
        A helper function that does the query and returns either:
        1) A list of document ids
        2) The `prefix` that needs to be subdivided into more focused queries
        """
        need_results = True
        while need_results:
            try:
                response = discovery.query(environmentId,
                                           collectionId,
                                           count=chunk_size,
                                           filter="extracted_metadata.sha1::"
                                           + prefix + "*",
                                           return_fields="extracted_metadata.sha1").get_result()
                need_results = False
            except Exception as e:
                print("will retry after error", e)

        if response["matching_results"] > chunk_size:
            return prefix
        else:
            return [item["id"] for item in response["results"]]

    prefixes_to_process = [""]
    while prefixes_to_process:
        prefix = prefixes_to_process.pop(0)
        prefixes = [prefix + letter for letter in alphabet]
        # `pmap` here does the requests to Discovery concurrently to save time.
        results = pmap(maybe_some_ids, prefixes)
        for result in results:
            if isinstance(result, list):
                doc_ids += result
            else:
                prefixes_to_process.append(result)

    return doc_ids

allDocIds = all_document_ids(discovery,
                           environmentId,
                           collectionId)

print("All document IDs in collection ", collectionId)
print(allDocIds)
