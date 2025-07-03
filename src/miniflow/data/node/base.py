# data/node/base.py
from abc import ABC, abstractmethod
import asyncio
from typing import Dict, Any, Optional
import logging

from .cache import CacheManager

logger = logging.getLogger()

class BaseDataNode(ABC):
    """
    프록시 패턴을 통해 데이터를 제어, 로깅, 지연호출
    글로벌 캐시 및 동시성 제어 포함
    """
    
    def __init__(self, *prior_nodes, element, process=None):
        self.element = element
        self.prior_nodes = prior_nodes
        self.process = process
        self.pre_hooks = []
        self.post_hooks = []
    
    async def get_data(self):
        """데이터 가져오기 (글로벌 캐시 사용)"""
        cache_key = self._generate_cache_key()
        
        async def compute_data():
            # 프리훅 실행
            for hook in self.pre_hooks:
                asyncio.create_task(hook(self))
            
            # 실제 데이터 계산
            result = await self._get_data()
            
            # 포스트훅 실행
            for hook in self.post_hooks:
                asyncio.create_task(hook(self))
            
            return result
        
        return await CacheManager.get_or_compute(cache_key, compute_data)
    
    def _generate_cache_key(self) -> str:
        """캐시 키 생성"""
        # prior_nodes의 캐시 키들 조합
        prior_keys = []
        for node in self.prior_nodes:
            if hasattr(node, '_generate_cache_key'):
                prior_keys.append(node._generate_cache_key())
            else:
                prior_keys.append(str(id(node)))
        
        # element, process, prior_keys를 조합
        process_str = str(type(self.process).__name__ if self.process else "None")
        prior_str = "|".join(prior_keys)
        
        cache_key = f"{type(self).__name__}:{self.element}:{process_str}:{hash(prior_str)}"
        return cache_key
    
    @abstractmethod
    async def _get_data(self):
        """실제 데이터 호출 (하위 클래스에서 구현)"""
        pass
    
    def add_pre_hook(self, hook):
        """데이터 처리 전에 실행할 훅 추가"""
        self.pre_hooks.append(hook)
        return self
    
    def add_post_hook(self, hook):
        """데이터 처리 후에 실행할 훅 추가"""
        self.post_hooks.append(hook)
        return self
    
    @classmethod
    def clear_cache(cls, pattern: Optional[str] = None):
        """캐시 수동 정리"""
        if pattern is None:
            CacheManager._cache.clear()
            CacheManager._cache_sizes.clear()
            CacheManager._access_times.clear()
            CacheManager._cache_locks.clear()
        else:
            # 패턴 매칭으로 선택적 삭제
            keys_to_remove = [k for k in CacheManager._cache.keys() if pattern in k]
            for key in keys_to_remove:
                CacheManager._cache.pop(key, None)
                CacheManager._cache_sizes.pop(key, None)
                CacheManager._access_times.pop(key, None)
                CacheManager._cache_locks.pop(key, None)
    
    @classmethod
    def get_cache_stats(cls) -> Dict[str, Any]:
        """캐시 통계 정보"""
        total_size = sum(CacheManager._cache_sizes.values())
        return {
            "total_items": len(CacheManager._cache),
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "max_size_mb": CacheManager.MAX_CACHE_SIZE / (1024 * 1024),
            "usage_percent": (total_size / CacheManager.MAX_CACHE_SIZE) * 100
        }