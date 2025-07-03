from .base import Pipeline
from .loaninfo.preprocess import idx_loaninfo_1, sampaling_loaninfo_1, encoding_loaninfo_1


class SamplingPipeline(Pipeline):
    def __call__(self, data):
        data = idx_loaninfo_1(data)
        data = encoding_loaninfo_1(data)
        train, val, test = sampaling_loaninfo_1(data, **self.params)
        return train, val, test