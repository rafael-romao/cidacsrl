import logging
from typing import List, Dict, Any, Optional, Callable

from cidacsrl_rlp.cidacsrl.domain.models.rules import ComparisonRule
from cidacsrl_rlp.cidacsrl.infra.configs.models.indexed_dataset_filter import IndexedDatasetFilterItem

logger = logging.getLogger(__name__)



class ElasticsearchQueryBuilder:
    """    
    Translates linkage blocking rules into Elasticsearch Query DSL format.
    """

    def __init__(self,
                 phase_rules: List[ComparisonRule],
                 target_fields: List[str],
                 candidate_limit: int,
                 static_filter: Optional[List[IndexedDatasetFilterItem]] = None):
        
        self.phase_rules = phase_rules
        self.target_fields = target_fields
        self.candidate_limit = candidate_limit
        self.static_filter = static_filter

        self._query_strategies: Dict[str, Callable[[str, Any, ComparisonRule], Dict[str, Any]]] = {
            'term': self._build_term_clause,
            'match': self._build_match_clause,
            'match_phrase': self._build_match_phrase_clause,
            'prefix': self._build_prefix_clause
        }

    
    def _build_term_clause(self, target_col: str, value: Any, rule: ComparisonRule) -> Dict[str, Any]:
        params = {"value": value}
        if rule.boost is not None:
            params["boost"] = rule.boost
        return {"term": {target_col: params}}

    def _build_match_clause(self, target_col: str, value: Any, rule: ComparisonRule) -> Dict[str, Any]:
        params = {"query": str(value)}
        if rule.is_fuzzy:
            params["fuzziness"] = "AUTO"
        if rule.boost is not None:
            params["boost"] = rule.boost
        return {"match": {target_col: params}}

    def _build_match_phrase_clause(self, target_col: str, value: Any, rule: ComparisonRule) -> Dict[str, Any]:
        params = {"query": str(value)}
        if rule.boost is not None:
            params["boost"] = rule.boost
        return {"match_phrase": {target_col: params}}
    
    def _build_prefix_clause(self, target_col: str, value: Any, rule: ComparisonRule) -> Dict[str, Any]:
        params = {"value": str(value)}
        if rule.boost is not None:
            params["boost"] = rule.boost
        return {"prefix": {target_col: params}}

    @staticmethod
    def _build_range_filter(range_config: Dict[str, Any]) -> Dict[str, Any]:
        # Supports either {'field': 'age', 'gte': 18} or {'age': {'gte': 18}}
        if "field" in range_config:
            field_name = range_config["field"]
            bounds = {
                k: range_config[k]
                for k in ("gt", "gte", "lt", "lte")
                if k in range_config and range_config[k] is not None
            }
            return {"range": {field_name: bounds}}

        field_name, bounds = next(iter(range_config.items()))
        if not isinstance(bounds, dict):
            raise ValueError("'range' value must be a dictionary of bounds.")
        return {"range": {field_name: bounds}}
    
    def _build_filters(self, source_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        filters: List[Dict[str, Any]] = []
        
        if self.static_filter:
            for filter_item in self.static_filter:
                if filter_item.query is not None:
                    filters.extend(filter_item.query)
                elif filter_item.column is not None:
                    col_name = filter_item.column
                    filters.append({
                        'term': {
                            col_name: source_record.get(col_name)
                        }
                    })
                elif filter_item.term is not None:
                    filters.append({'term': filter_item.term})
                elif filter_item.range is not None:
                    filters.append(self._build_range_filter(filter_item.range))
                else:
                    raise ValueError(f"Invalid static_filter structure: {filter_item}")
        logger.debug(f"Constructed filters: {filters}")
        return filters   
    
    
    
    def build_bool_query(self, source_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Builds an Elasticsearch query body for a given record based on the phase rules.

        Args:
            source_record (Dict[str, Any]): A dictionary representing a single record.

        """

        bool_query_clauses = {
            "must": [],
            "should": [],
            "filter": self._build_filters(source_record),
            "must_not": []
        }

        for rule in self.phase_rules:
            source_col = rule.source_column
            target_col = rule.target_column
            clause_type = rule.es_clause_type
            query_type = rule.query_type

            value = source_record.get(source_col)

            if value is None or (isinstance(value, str) and not value.strip()):
                logger.debug(f"Source value for '{source_col}' is null or empty. Skipping rule for ES query.")
                continue

            clause_builder = self._query_strategies.get(query_type)
            
            if not clause_builder:
                logger.warning(f"Unsupported query type: '{query_type}' in rule: {rule}. Skipping this rule.")
                continue

            clause = clause_builder(target_col, value, rule)

            if clause_type in bool_query_clauses:
                bool_query_clauses[clause_type].append(clause)
            else:
                logger.debug(f"Unrecognized ES clause type '{clause_type}' in rule: {rule}. Rule ignored for bool query.")

        logger.debug(f"Constructed bool query clauses: {bool_query_clauses}")
        return bool_query_clauses

    def build_search_body_for_record(self, source_record: Dict[str, Any]) -> Dict[str, Any]:
        """Builds the full Elasticsearch search request body for one source record."""
        bool_query_clauses = self.build_bool_query(source_record)
        return {
            "query": {"bool": bool_query_clauses},
            "_source": self.target_fields,
            "size": self.candidate_limit,
        }