from abc import ABC, abstractmethod



class BaseDataset(ABC):
    """DataNode를 사용하는 기본 Dataset 클래스"""
    
    def __init__(self, params):
        """
        Args:
            data_node: 입력 데이터를 제공할 DataNode 인스턴스
            label_node: 레이블 데이터를 제공할 DataNode 인스턴스 (선택적)
            transform: 변환 파이프라인
        """
        self.params = params
        self._data = None
        
    
    @property
    async def data(self):
        """데이터에 대한 지연 로딩 구현"""
        if self._data is None:
            self._data = await self.get_data(**self.params)
        return self._data
    
    @abstractmethod
    def __len__(self):
        """데이터셋의 크기 반환"""
        pass
    
    @abstractmethod
    def __getitem__(self, idx):
        """인덱스로 데이터 항목 접근"""
        pass
    
    @abstractmethod
    async def get_data(self):
        '''
        데이터노드를 통해 파이프라인 정의
        '''
        pass