import asyncio

from .base import BaseDataNode
    

class SubNode(BaseDataNode):
    async def _get_data(self):
        data = await self.prior_nodes[0].get_data(self.element) # 지연호출
        if self.process is not None:
            data = self.process(*data)
        return data


class PubNode(BaseDataNode):
    def __init__(self, *prior_nodes, element:list, process=None):
        self.element = element # id or path
        self.prior_nodes = prior_nodes # 지연호출
        self.process = process
        self._cache:dict = None
        self._cache_lock = asyncio.Lock()  # 락 추가
        self.pre_hooks = []   # 데이터 처리 전 실행할 훅(로깅, api훅 등)
        self.post_hooks = []  # 데이터 처리 후 실행할 훅(로깅, api훅 등)

        self.subnodes = [SubNode(self, element=element, process = None) for element in self.element]
        

    def publish(self):
        return self.subnodes


    async def get_data(self, element):
        if self._cache is None:
            async with self._cache_lock:
                if self._cache is None:  # 이중 체크
                    # 프리훅
                    await asyncio.gather(*[hook(self) for hook in self.pre_hooks])
                    # 데이터 처리
                    self._cache = await self._get_data()  # 여기서 캐시 저장
                    # 포스트훅
                    for hook in self.post_hooks:
                        asyncio.create_task(hook(self))
        return self._cache[element]


    async def _get_data(self):
        data = await asyncio.gather(*[node.get_data() for node in self.prior_nodes])
        if self.process is not None:
            data = self.process(*data)
        result = {element:datum for element,datum in zip(self.element, data)}
        return result
    
    