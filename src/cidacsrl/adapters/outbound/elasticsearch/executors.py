from typing import Dict, List

from cidacsrl.ports.linkage.search_executor import SearchExecutor


class SingleSearchExecutor(SearchExecutor):
    def execute(self, es_client, index: str, queries: List[Dict]) -> List[Dict]:        
        responses = []

        for q in queries:
            response = es_client.search(index=index, body=q)
            responses.append(response)

        return responses

class MultiSearchExecutor(SearchExecutor):
    def execute(self, es_client, index: str, queries: List[Dict]) -> List[Dict]:        
        msearch_payload = []
        for q in queries:
            msearch_payload.extend([{"index": index}, q])
        
        response = es_client.msearch(body=msearch_payload)
        
        responses = response.get("responses", [])

        if len(responses) != len(queries):
            raise ValueError(f"Expected {len(queries)} responses, but got {len(responses)}. Check the msearch response for details.")
                
        
        return responses