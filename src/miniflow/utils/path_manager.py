from queue import Queue, Empty
import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional
from enum import Enum
from pathlib import Path
from multiprocessing import Process, Queue as MPQueue, Pipe
import os
import time



class TaskType(Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"

@dataclass
class FileTask:
    task_id: str
    task_type: TaskType
    file_path: str
    data: Any = None
    binary: bool = False
    callback: Optional[Callable] = None
    result_queue: Optional[Queue] = None

class FileMessageBroker:
    def __init__(self, num_workers=4):
        self.task_queue = Queue()
        self.workers = []
        self.results = {}
        self.file_locks = {}
        self.lock = threading.Lock()
        
        # 워커 스레드들 시작
        for i in range(num_workers):
            worker = threading.Thread(target=self._worker, daemon=True)
            worker.start()
            self.workers.append(worker)
    
    def _worker(self):
        while True:
            try:
                task = self.task_queue.get(timeout=1)
                if task is None:  # 종료 신호
                    break
                self._execute_task(task)
                self.task_queue.task_done()
            except Empty:
                continue
    
    def _execute_task(self, task: FileTask):
        # 파일별 락 획득
        with self.lock:
            if task.file_path not in self.file_locks:
                self.file_locks[task.file_path] = threading.RLock()
        
        file_lock = self.file_locks[task.file_path]
        
        try:
            with file_lock:
                result = self._do_file_operation(task)
                
            # 결과 처리
            if task.result_queue:
                task.result_queue.put(result)
            elif task.callback:
                task.callback(result)
                
        except Exception as e:
            error_result = f"ERROR: {e}"
            if task.result_queue:
                task.result_queue.put(error_result)
            elif task.callback:
                task.callback(error_result)
    
    def _do_file_operation(self, task: FileTask):
        path = Path(task.file_path)
        
        if task.task_type == TaskType.READ:
            mode = "rb" if task.binary else "r"
            with open(path, mode, encoding=None if task.binary else 'utf-8') as f:
                return f.read()
                
        elif task.task_type == TaskType.WRITE:
            # 디렉토리 생성
            path.parent.mkdir(parents=True, exist_ok=True)
            mode = "wb" if task.binary else "w"
            with open(path, mode, encoding=None if task.binary else 'utf-8') as f:
                f.write(task.data)
            return "SUCCESS"
            
        elif task.task_type == TaskType.DELETE:
            if path.exists():
                path.unlink()
                return "DELETED"
            else:
                return "NOT_FOUND"
    
    def submit_task(self, task: FileTask):
        self.task_queue.put(task)
    
    def read_sync(self, file_path: str, binary=False):
        result_queue = Queue()
        task = FileTask(
            task_id=f"read_{id(result_queue)}",
            task_type=TaskType.READ,
            file_path=file_path,
            binary=binary,
            result_queue=result_queue
        )
        self.submit_task(task)
        result = result_queue.get()
        if isinstance(result, str) and result.startswith("ERROR:"):
            raise IOError(result[7:])
        return result
    
    def write_sync(self, file_path: str, data, binary=False):
        result_queue = Queue()
        task = FileTask(
            task_id=f"write_{id(data)}",
            task_type=TaskType.WRITE,
            file_path=file_path,
            data=data,
            binary=binary,
            result_queue=result_queue
        )
        self.submit_task(task)
        result = result_queue.get()
        if isinstance(result, str) and result.startswith("ERROR:"):
            raise IOError(result[7:])
        return result
    
    def delete_sync(self, file_path: str):
        result_queue = Queue()
        task = FileTask(
            task_id=f"delete_{id(file_path)}",
            task_type=TaskType.DELETE,
            file_path=file_path,
            result_queue=result_queue
        )
        self.submit_task(task)
        result = result_queue.get()
        if isinstance(result, str) and result.startswith("ERROR:"):
            raise IOError(result[7:])
        return result
    
    def shutdown(self):
        # 모든 작업 완료 대기
        self.task_queue.join()
        
        # 워커들에게 종료 신호
        for _ in self.workers:
            self.task_queue.put(None)

# ===== 중앙 파일 서버 =====

class CentralFileServer:
    def __init__(self, num_workers=4):
        self.broker = FileMessageBroker(num_workers)
        self.request_queue = MPQueue()
        self.running = False
        
    def start_server_process(self):
        """별도 프로세스에서 파일 서버 시작"""
        server_process = Process(target=self._run_server, daemon=True)
        server_process.start()
        return server_process
    
    def _run_server(self):
        """파일 서버 메인 루프"""
        self.running = True
        print(f"File server started (PID: {os.getpid()})")
        
        while self.running:
            try:
                response_conn, request = self.request_queue.get(timeout=1)
                self._handle_request(response_conn, request)
            except Exception as e:
                if self.running:  # 정상 종료가 아닌 경우만 출력
                    print(f"Server error: {e}")
                break
    
    def _handle_request(self, response_conn, request):
        """클라이언트 요청 처리"""
        try:
            # FileTask 생성
            task = FileTask(
                task_id=request['task_id'],
                task_type=TaskType(request['task_type']),
                file_path=request['file_path'],
                data=request.get('data'),
                binary=request.get('binary', False)
            )
            
            # 동기적으로 처리
            if task.task_type == TaskType.READ:
                result = self.broker.read_sync(task.file_path, task.binary)
            elif task.task_type == TaskType.WRITE:
                result = self.broker.write_sync(task.file_path, task.data, task.binary)
            elif task.task_type == TaskType.DELETE:
                result = self.broker.delete_sync(task.file_path)
            
            response_conn.send({"status": "success", "data": result})
            
        except Exception as e:
            response_conn.send({"status": "error", "message": str(e)})
        finally:
            response_conn.close()
    
    def stop(self):
        self.running = False

# 전역 파일 서버 인스턴스
_file_server = None
_server_process = None

def start_file_server(num_workers=4):
    """파일 서버 시작"""
    global _file_server, _server_process
    if _file_server is None:
        _file_server = CentralFileServer(num_workers)
        _server_process = _file_server.start_server_process()
        time.sleep(0.1)  # 서버 시작 대기
    return _file_server

def get_file_server():
    """파일 서버 인스턴스 반환"""
    if _file_server is None:
        return start_file_server()
    return _file_server



class ProxyPath:
    """중앙 파일 서버를 사용하는 Path 래퍼"""
    
    def __init__(self, path):
        self._path = Path(path)
        self._server = get_file_server()
    
    def __str__(self):
        return str(self._path)
    
    def __repr__(self):
        return f"ProxyPath({self._path!r})"
    
    def __fspath__(self):
        return str(self._path)
    
    def __truediv__(self, other):
        return ProxyPath(self._path / other)
    
    def __rtruediv__(self, other):
        return ProxyPath(other / self._path)
    
    @property
    def name(self):
        return self._path.name
    
    @property
    def suffix(self):
        return self._path.suffix
    
    @property
    def parent(self):
        return ProxyPath(self._path.parent)
    
    @property
    def stem(self):
        return self._path.stem
    
    def exists(self):
        return self._path.exists()
    
    def is_file(self):
        return self._path.is_file()
    
    def is_dir(self):
        return self._path.is_dir()
    
    def mkdir(self, parents=False, exist_ok=False):
        return self._path.mkdir(parents=parents, exist_ok=exist_ok)
    
    def _send_request(self, request):
        """서버에 요청 전송"""
        parent_conn, child_conn = Pipe()
        self._server.request_queue.put((child_conn, request))
        
        response = parent_conn.recv()
        parent_conn.close()
        
        if response["status"] == "error":
            raise IOError(response["message"])
        
        return response["data"]
    
    def read_text(self, encoding='utf-8'):
        request = {
            'task_id': f"read_text_{id(self)}_{time.time()}",
            'task_type': 'read',
            'file_path': str(self._path),
            'binary': False
        }
        return self._send_request(request)
    
    def write_text(self, data, encoding='utf-8'):
        request = {
            'task_id': f"write_text_{id(self)}_{time.time()}",
            'task_type': 'write',
            'file_path': str(self._path),
            'data': data,
            'binary': False
        }
        return self._send_request(request)
    
    def read_bytes(self):
        request = {
            'task_id': f"read_bytes_{id(self)}_{time.time()}",
            'task_type': 'read',
            'file_path': str(self._path),
            'binary': True
        }
        return self._send_request(request)
    
    def write_bytes(self, data):
        request = {
            'task_id': f"write_bytes_{id(self)}_{time.time()}",
            'task_type': 'write',
            'file_path': str(self._path),
            'data': data,
            'binary': True
        }
        return self._send_request(request)
    
    def unlink(self):
        request = {
            'task_id': f"delete_{id(self)}_{time.time()}",
            'task_type': 'delete',
            'file_path': str(self._path)
        }
        return self._send_request(request)

# ===== 사용 예제 =====
if __name__ == "__main__":
    # 파일 서버 시작
    start_file_server()
    
    # 사용법 (기존과 동일)
    path = ProxyPath("test_images/sample.jpg")
    
    # 텍스트 파일 테스트
    text_path = ProxyPath("test_data/sample.txt")
    text_path.write_text("Hello, World!")
    content = text_path.read_text()
    print(f"Read content: {content}")
    
    # 바이너리 파일 테스트
    binary_data = b"Binary content example"
    binary_path = ProxyPath("test_data/sample.bin")
    binary_path.write_bytes(binary_data)
    read_data = binary_path.read_bytes()
    print(f"Binary data match: {binary_data == read_data}")
    
    print("All tests completed!")