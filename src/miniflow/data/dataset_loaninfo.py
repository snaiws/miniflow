import asyncio

from .base import BaseDataset
from .node import CommonNode, PubNode, SubNode
from .source import DataSourceCSV
from .process import PreprocessPipeline, SamplingPipeline



async def myhook(node):
    print(node.element)


class DatasetLoaninfoRaw(BaseDataset):
    def __len__(self):
        """데이터셋의 크기 반환"""
        return len(self.data)
    
    def __getitem__(self, idx):
        """인덱스로 데이터 항목 접근"""
        pass

    async def get_data(self, csv_path, ratio, random_state):
        params1 = {
            "ratio":ratio,
            "random_state":random_state
        }

        pipeline1 = SamplingPipeline(params1)
        pipeline2 = PreprocessPipeline()

        node_0 = DataSourceCSV(csv_path)
        node_1 = PubNode(node_0, element= ['train', 'validate', 'test'], process = pipeline1)
        node_1.add_post_hook(myhook)
        node_2, node_3, node_4 = node_1.publish()

        
        node_5 = PubNode(node_2, node_3, node_4, element=['X_train', 'y_train','X_val','y_val','X_test','y_test'], process=pipeline2)
        node_5.add_post_hook(myhook)
        node_6, node_7, node_8, node_9, node_10, node_11 = node_5.publish()

        results = await asyncio.gather(node_6.get_data(), node_7.get_data(), node_8.get_data(), node_9.get_data(), node_10.get_data(), node_11.get_data())
        return results



if __name__ == "__main__":
    async def main():
        params = {
            "csv_path":"/workspace/Storage/template_structured/Data/raw/train.csv",
            "ratio": (0.6, 0.2, 0.2),
            "random_state":42
            }
        data = DatasetLoaninfoRaw(params)
        result = await data.data  # Add await here
        print(result)  # Print the result, not the coroutine
    
    import asyncio
    asyncio.run(main())