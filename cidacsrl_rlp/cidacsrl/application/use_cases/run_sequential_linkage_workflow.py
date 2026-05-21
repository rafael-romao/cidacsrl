from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_reader_port import DataReaderPort
from cidacsrl_rlp.cidacsrl.domain.models.workflow import SequentialBlockingWorkflow


class RunSequentialLinkageWorkflowUseCase:
    def __init__(self, data_reader_port: DataReaderPort):        
        self.data_reader = data_reader_port 

    def execute(self, config: SequentialBlockingWorkflow):
        df_source = self.data_reader.read_data()        
        for phase in config.blocking_phases:
            if phase.enabled:
                self._run_phase(df_source, phase)
    
    def _run_phase(self, df_source, phase):
        pass