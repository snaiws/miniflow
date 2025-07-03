import asyncio
import sys
import gc
from typing import Dict, Any
import logging

logger = logging.getLogger()

class CacheManager:
    """글로벌 캐시 및 메모리 관리"""
    _cache: Dict[str, Any] = {}
    _cache_locks: Dict[str, asyncio.Lock] = {}
    _cache_sizes: Dict[str, int] = {}
    _access_times: Dict[str, float] = {}
    _main_lock = asyncio.Lock()
    
    # 설정값들
    MAX_CACHE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB 제한
    MAX_CACHE_ITEMS = 1000  # 최대 캐시 아이템 수
    CLEANUP_THRESHOLD = 0.8  # 80% 도달시 정리
    
    @classmethod
    async def get_or_compute(cls, cache_key: str, compute_func, estimated_size: int = 0):
        """캐시에서 가져오거나 계산 후 저장"""
        import time
        
        # 캐시 락 관리
        async with cls._main_lock:
            if cache_key not in cls._cache_locks:
                cls._cache_locks[cache_key] = asyncio.Lock()
        
        # 캐시 키별 락
        async with cls._cache_locks[cache_key]:
            # 캐시 히트
            if cache_key in cls._cache:
                cls._access_times[cache_key] = time.time()
                logger.debug(f"Cache hit: {cache_key}")
                return cls._cache[cache_key]
            
            # 메모리 정리 체크
            await cls._cleanup_if_needed()
            
            # 계산 실행
            logger.debug(f"Cache miss, computing: {cache_key}")
            result = await compute_func()
            
            # 결과 크기 추정
            if estimated_size == 0:
                estimated_size = cls._estimate_size(result)
            
            # 캐시 저장
            cls._cache[cache_key] = result
            cls._cache_sizes[cache_key] = estimated_size
            cls._access_times[cache_key] = time.time()
            
            logger.debug(f"Cached result: {cache_key} (size: {estimated_size} bytes)")
            return result
    
    @classmethod
    async def _cleanup_if_needed(cls):
        """메모리 사용량이 임계치를 넘으면 정리"""
        total_size = sum(cls._cache_sizes.values())
        total_items = len(cls._cache)
        
        if (total_size > cls.MAX_CACHE_SIZE * cls.CLEANUP_THRESHOLD or 
            total_items > cls.MAX_CACHE_ITEMS * cls.CLEANUP_THRESHOLD):
            
            logger.info(f"Starting cache cleanup. Size: {total_size}, Items: {total_items}")
            await cls._cleanup_cache()
    
    @classmethod
    async def _cleanup_cache(cls):
        """LRU 방식으로 캐시 정리"""
        import time
        
        # 접근 시간 기준으로 정렬 (오래된 것부터)
        sorted_items = sorted(
            cls._access_times.items(), 
            key=lambda x: x[1]
        )
        
        # 30% 정도 제거
        remove_count = max(1, len(sorted_items) // 3)
        
        for cache_key, _ in sorted_items[:remove_count]:
            if cache_key in cls._cache:
                del cls._cache[cache_key]
                del cls._cache_sizes[cache_key]
                del cls._access_times[cache_key]
                if cache_key in cls._cache_locks:
                    del cls._cache_locks[cache_key]
        
        # 가비지 컬렉션 실행
        gc.collect()
        
        logger.info(f"Cache cleanup completed. Removed {remove_count} items")
    
    @classmethod
    def _estimate_size(cls, obj) -> int:
        """객체 크기 추정"""
        try:
            return sys.getsizeof(obj)
        except:
            return 1024  # 기본값 1KB

