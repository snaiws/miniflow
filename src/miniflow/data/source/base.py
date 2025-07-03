from abc import ABC, abstractmethod


class Source(ABC):
    '''
    파이프라인 덕타이핑(__call__)
    '''
    def __init__(self, params={}):
        self.params = params
        
    
    @abstractmethod
    def __call__(self, data):
        '''
        전처리 조합
        '''
        pass