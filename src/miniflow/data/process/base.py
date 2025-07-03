from abc import ABC, abstractmethod


class Pipeline(ABC):
    def __init__(self, params={}):
        self.params = params
        
    
    @abstractmethod
    def __call__(self, data):
        '''
        전처리 조합
        '''
        pass