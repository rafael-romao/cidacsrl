from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_reader_port import DataReaderPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.get_candidates_port import GetCandidatesPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.scoring_port import ScoringPort
from cidacsrl_rlp.cidacsrl.domain.models.workflow import SequentialBlockingWorkflow


class RunSequentialLinkageWorkflowUseCase:
    def __init__(self, 
                 data_reader_port: DataReaderPort, 
                 get_candidates_port: GetCandidatesPort, 
                 scoring_port: ScoringPort):        
        self.data_reader = data_reader_port
        self.get_candidates = get_candidates_port
        self.scoring = scoring_port
    
    def execute(self, config: SequentialBlockingWorkflow):
        df_source = self.data_reader.read_data()        
        for phase_context in config.build_blocking_phase_contexts():
            if phase_context.enabled:
                df_candidates = self.get_candidates.get_candidates(df_source, phase_context)
                df_scored_pairs = self.scoring.calculate_score(df_candidates, phase_context)

        return df_scored_pairs