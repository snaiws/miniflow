import pandas as pd

from .base import Source



class SourceCSV(Source):
    '''
    파이프라인 덕타이핑(__call__)
    '''
    def __init__(self, params={}):
        self.params = params
        
    
    def __call__(self, data):
        '''
        전처리 조합
        '''
        data = pd.read_csv(data, **self.params)
        return data