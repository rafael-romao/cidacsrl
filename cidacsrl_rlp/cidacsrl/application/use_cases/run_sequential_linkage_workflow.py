from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_reader_port import DataReaderPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.get_candidates_port import GetCandidatesPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.scoring_port import ScoringPort
from cidacsrl_rlp.cidacsrl.domain.models.workflow import SequentialBlockingWorkflow


class RunSequentialLinkageWorkflowUseCase:
    def __init__(self, data_reader_port: DataReaderPort, get_candidates_port: GetCandidatesPort, scoring_port: ScoringPort):        
        self.data_reader = data_reader_port
        self.get_candidates = get_candidates_port
        self.scoring = scoring_port
    
    def execute(self, config: SequentialBlockingWorkflow):
        df_source = self.data_reader.read_data()        
        for phase in config.blocking_phases:
            if phase.enabled:
                df_candidates = self.get_candidates.get_candidates(df_source, phase)
                df_scored_pairs = self.scoring.calculate_score(df_candidates, phase)

        return df_scored_pairs