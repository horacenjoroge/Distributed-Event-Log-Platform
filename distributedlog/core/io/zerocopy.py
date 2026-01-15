"""
Zero-copy data transfers for performance optimization.

Uses sendfile() and memory-mapped files to avoid user-space copying.
"""

import mmap
import os
import platform
from typing import Optional

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class ZeroCopySupport:
    """
    Detect and manage zero-copy support.
    
    Different platforms support different zero-copy mechanisms:
    - Linux: sendfile() - best performance
    - macOS: sendfile() available but different API
    - Windows: TransmitFile() - not implemented here
    """
    
    @staticmethod
    def is_linux() -> bool:
        """Check if running on Linux."""
        return platform.system() == "Linux"
    
    @staticmethod
    def is_macos() -> bool:
        """Check if running on macOS."""
        return platform.system() == "Darwin"
    
    @staticmethod
    def is_windows() -> bool:
        """Check if running on Windows."""
        return platform.system() == "Windows"
    
    @staticmethod
    def supports_sendfile() -> bool:
        """
        Check if sendfile() is supported.
        
        Returns:
            True if sendfile() available
        """
        if ZeroCopySupport.is_linux():
            return True
        elif ZeroCopySupport.is_macos():
            return True  # macOS has sendfile but different API
        else:
            return False
    
    @staticmethod
    def get_platform_info() -> dict:
        """
        Get platform information.
        
        Returns:
            Platform info dict
        """
        return {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "sendfile_support": ZeroCopySupport.supports_sendfile(),
        }


class SendfileTransfer:
    """
    Zero-copy file transfer using sendfile().
    
    sendfile() copies data between file descriptors without
    passing through user space, using DMA (Direct Memory Access).
    
    Traditional copy:
    Disk → Kernel buffer → User buffer → Socket buffer → NIC
    (4 copies, 2 context switches)
    
    Zero-copy with sendfile():
    Disk → Kernel buffer → NIC
    (2 copies via DMA, 0 user-space copies)
    """
    
    def __init__(self):
        """Initialize sendfile transfer."""
        self.platform = platform.system()
        self.sendfile_available = False
        
        # Try to import sendfile
        if ZeroCopySupport.is_linux():
            try:
                # Linux sendfile
                import os
                # Check if os.sendfile exists (Python 3.3+)
                if hasattr(os, 'sendfile'):
                    self.sendfile_available = True
                    logger.info("Linux sendfile() available")
            except Exception as e:
                logger.warning(f"sendfile() not available: {e}")
        
        elif ZeroCopySupport.is_macos():
            # macOS has sendfile but different signature
            # For simplicity, we'll use fallback on macOS
            logger.info("macOS detected, using fallback (mmap)")
    
    def sendfile_to_socket(
        self,
        out_fd: int,
        in_fd: int,
        offset: int,
        count: int,
    ) -> int:
        """
        Transfer data from file to socket using sendfile().
        
        Args:
            out_fd: Output socket file descriptor
            in_fd: Input file descriptor
            offset: Offset in input file
            count: Number of bytes to transfer
        
        Returns:
            Number of bytes transferred
        """
        if not self.sendfile_available:
            raise NotImplementedError("sendfile() not available on this platform")
        
        try:
            # Linux sendfile: os.sendfile(out_fd, in_fd, offset, count)
            bytes_sent = os.sendfile(out_fd, in_fd, offset, count)
            
            logger.debug(
                "Zero-copy transfer completed",
                bytes=bytes_sent,
                offset=offset,
            )
            
            return bytes_sent
        
        except Exception as e:
            logger.error(
                "sendfile() failed",
                error=str(e),
                offset=offset,
                count=count,
            )
            raise
    
    def can_use_sendfile(self, use_tls: bool = False) -> bool:
        """
        Check if sendfile can be used.
        
        sendfile() is incompatible with TLS (encryption needs user-space).
        
        Args:
            use_tls: Whether TLS is enabled
        
        Returns:
            True if sendfile can be used
        """
        if use_tls:
            logger.debug("Cannot use sendfile with TLS (needs user-space encryption)")
            return False
        
        return self.sendfile_available


class MappedFileReader:
    """
    Memory-mapped file reader for zero-copy reads.
    
    Uses mmap() to map file directly into memory, avoiding read() syscalls.
    
    Benefits:
    - No read() syscall overhead
    - Kernel manages page cache
    - Lazy loading (pages loaded on access)
    - Shared memory between processes
    """
    
    def __init__(
        self,
        filepath: str,
        offset: int = 0,
        length: Optional[int] = None,
    ):
        """
        Initialize memory-mapped file reader.
        
        Args:
            filepath: Path to file
            offset: Offset in file
            length: Length to map (None = entire file)
        """
        self.filepath = filepath
        self.offset = offset
        self.length = length
        
        self.fd: Optional[int] = None
        self.mmap: Optional[mmap.mmap] = None
        
        logger.debug(
            "MappedFileReader initialized",
            filepath=filepath,
            offset=offset,
            length=length,
        )
    
    def open(self) -> None:
        """Open and map file into memory."""
        try:
            # Open file
            self.fd = os.open(self.filepath, os.O_RDONLY)
            
            # Get file size
            file_size = os.fstat(self.fd).st_size
            
            # Calculate map length
            if self.length is None:
                map_length = file_size - self.offset
            else:
                map_length = min(self.length, file_size - self.offset)
            
            if map_length <= 0:
                raise ValueError(f"Invalid map length: {map_length}")
            
            # Create memory map
            self.mmap = mmap.mmap(
                self.fd,
                length=map_length,
                offset=self.offset,
                access=mmap.ACCESS_READ,
            )
            
            logger.info(
                "File mapped into memory",
                filepath=self.filepath,
                size=map_length,
                offset=self.offset,
            )
        
        except Exception as e:
            logger.error(
                "Failed to map file",
                filepath=self.filepath,
                error=str(e),
            )
            self.close()
            raise
    
    def read(self, size: int = -1) -> bytes:
        """
        Read from mapped memory.
        
        Args:
            size: Number of bytes to read (-1 = all remaining)
        
        Returns:
            Data bytes
        """
        if not self.mmap:
            raise RuntimeError("File not mapped, call open() first")
        
        return self.mmap.read(size)
    
    def read_at(self, position: int, size: int) -> bytes:
        """
        Read from specific position in mapped memory.
        
        Args:
            position: Position in file
            size: Number of bytes to read
        
        Returns:
            Data bytes
        """
        if not self.mmap:
            raise RuntimeError("File not mapped, call open() first")
        
        # Seek to position
        self.mmap.seek(position)
        return self.mmap.read(size)
    
    def close(self) -> None:
        """Close memory map and file."""
        if self.mmap:
            self.mmap.close()
            self.mmap = None
        
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        
        logger.debug("Mapped file closed", filepath=self.filepath)
    
    def __enter__(self):
        """Context manager entry."""
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class DirectBufferTransfer:
    """
    Direct buffer transfers without intermediate copying.
    
    Uses memoryview and buffer protocol for zero-copy slicing.
    """
    
    @staticmethod
    def create_view(data: bytes, offset: int = 0, size: Optional[int] = None) -> memoryview:
        """
        Create memory view without copying.
        
        Args:
            data: Source data
            offset: Offset in data
            size: Size of view (None = remaining)
        
        Returns:
            Memory view (zero-copy slice)
        """
        mv = memoryview(data)
        
        if size is None:
            return mv[offset:]
        else:
            return mv[offset:offset + size]
    
    @staticmethod
    def transfer_to_buffer(source: bytes, dest: bytearray, offset: int = 0) -> int:
        """
        Transfer data to buffer without copying (using memoryview).
        
        Args:
            source: Source data
            dest: Destination buffer
            offset: Offset in destination
        
        Returns:
            Number of bytes transferred
        """
        source_view = memoryview(source)
        dest_view = memoryview(dest)
        
        # Copy using slice assignment (efficient)
        size = len(source_view)
        dest_view[offset:offset + size] = source_view
        
        return size


def get_optimal_transfer_method(
    file_size: int,
    use_tls: bool = False,
    platform_override: Optional[str] = None,
) -> str:
    """
    Determine optimal transfer method based on context.
    
    Args:
        file_size: Size of data to transfer
        use_tls: Whether TLS is enabled
        platform_override: Override platform detection
    
    Returns:
        Transfer method: "sendfile", "mmap", or "standard"
    """
    current_platform = platform_override or platform.system()
    
    # TLS requires user-space encryption
    if use_tls:
        return "standard"
    
    # sendfile() best for large files on Linux
    if current_platform == "Linux" and file_size > 64 * 1024:  # > 64KB
        if ZeroCopySupport.supports_sendfile():
            return "sendfile"
    
    # mmap good for medium-sized files
    if 4 * 1024 <= file_size <= 100 * 1024 * 1024:  # 4KB - 100MB
        return "mmap"
    
    # Standard read/write for small files
    return "standard"


logger.info(
    "Zero-copy module loaded",
    platform=platform.system(),
    sendfile_support=ZeroCopySupport.supports_sendfile(),
)
