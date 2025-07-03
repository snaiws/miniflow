import asyncio

from .base import BaseDataNode



class CommonNode(BaseDataNode):
    async def _get_data(self):
        data = await asyncio.gather(*[node.get_data() for node in self.prior_nodes]) # 지연호출
        if self.process is not None:
            data = self.process(*data)
        return data