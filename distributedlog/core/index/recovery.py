"""
Index recovery and rebuild utilities.

Handles index corruption detection and rebuilding by scanning log files.
"""

from pathlib import Path

from distributedlog.core.index.offset_index import OffsetIndex
from distributedlog.core.log.format import MessageSet
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class IndexRecovery:
    """Utilities for index recovery and rebuild operations."""
    
    @staticmethod
    def rebuild_index(
        log_path: Path,
        base_offset: int,
        index_interval_bytes: int = 4096,
    ) -> OffsetIndex:
        """
        Rebuild index by scanning the entire log file.
        
        This is used when:
        - Index file is missing
        - Index file is corrupted
        - Index file is out of sync with log
        
        Args:
            log_path: Path to the log file
            base_offset: Base offset of the segment
            index_interval_bytes: Bytes between index entries
        
        Returns:
            Rebuilt OffsetIndex
        """
        logger.info(
            "Rebuilding index from log",
            log_path=str(log_path),
            base_offset=base_offset,
        )
        
        index = OffsetIndex(
            base_offset=base_offset,
            directory=log_path.parent,
            interval_bytes=index_interval_bytes,
        )
        
        with open(log_path, "rb") as f:
            current_offset = base_offset
            position = 0
            entries_added = 0
            
            while True:
                length_bytes = f.read(4)
                
                if len(length_bytes) == 0:
                    break
                
                if len(length_bytes) < 4:
                    logger.warning(
                        "Partial length field at end of log",
                        position=position,
                    )
                    break
                
                length = int.from_bytes(length_bytes, byteorder="big")
                
                if length <= 0 or length > 100 * 1024 * 1024:
                    logger.error(
                        "Invalid message length during rebuild",
                        position=position,
                        length=length,
                    )
                    break
                
                remaining_bytes = f.read(length)
                
                if len(remaining_bytes) < length:
                    logger.warning(
                        "Incomplete message at end of log",
                        position=position,
                        expected=length,
                        got=len(remaining_bytes),
                    )
                    break
                
                full_message = length_bytes + remaining_bytes
                
                try:
                    MessageSet.deserialize(full_message, current_offset)
                    
                    if index.append(current_offset, position):
                        entries_added += 1
                    
                    current_offset += 1
                    position += len(full_message)
                    
                except ValueError as e:
                    logger.error(
                        "Failed to deserialize during rebuild",
                        position=position,
                        error=str(e),
                    )
                    break
        
        index.flush()
        
        logger.info(
            "Index rebuild complete",
            log_path=str(log_path),
            entries_added=entries_added,
            messages_scanned=current_offset - base_offset,
        )
        
        return index
    
    @staticmethod
    def validate_index(index: OffsetIndex, log_path: Path) -> bool:
        """
        Validate that index entries are consistent with log file.
        
        Args:
            index: Index to validate
            log_path: Path to log file
        
        Returns:
            True if index is valid, False if corrupted
        """
        logger.info("Validating index", log_path=str(log_path))
        
        try:
            with open(log_path, "rb") as f:
                for i in range(min(10, index.entries_count())):
                    entry = index._read_entry(i)
                    
                    f.seek(entry.position)
                    length_bytes = f.read(4)
                    
                    if len(length_bytes) != 4:
                        logger.error(
                            "Index points to invalid position",
                            entry_index=i,
                            position=entry.position,
                        )
                        return False
                    
                    length = int.from_bytes(length_bytes, byteorder="big")
                    remaining = f.read(length)
                    
                    if len(remaining) < length:
                        logger.error(
                            "Index points to incomplete message",
                            entry_index=i,
                            position=entry.position,
                        )
                        return False
                    
                    full_message = length_bytes + remaining
                    absolute_offset = entry.relative_offset + index.base_offset
                    
                    try:
                        message_set = MessageSet.deserialize(full_message, absolute_offset)
                        
                        if message_set.message.offset != absolute_offset:
                            logger.error(
                                "Offset mismatch in index",
                                expected=absolute_offset,
                                actual=message_set.message.offset,
                            )
                            return False
                    
                    except ValueError as e:
                        logger.error(
                            "Message at indexed position is corrupted",
                            entry_index=i,
                            error=str(e),
                        )
                        return False
            
            logger.info("Index validation passed", log_path=str(log_path))
            return True
        
        except Exception as e:
            logger.error("Index validation failed", error=str(e))
            return False
    
    @staticmethod
    def recover_or_rebuild(
        log_path: Path,
        base_offset: int,
        index_interval_bytes: int = 4096,
    ) -> OffsetIndex:
        """
        Recover existing index or rebuild if corrupted.
        
        Args:
            log_path: Path to log file
            base_offset: Base offset of segment
            index_interval_bytes: Bytes between index entries
        
        Returns:
            Valid OffsetIndex
        """
        index_path = Path(str(log_path).replace(".log", ".index"))
        
        if index_path.exists():
            try:
                index = OffsetIndex(
                    base_offset=base_offset,
                    directory=log_path.parent,
                    interval_bytes=index_interval_bytes,
                )
                
                if IndexRecovery.validate_index(index, log_path):
                    logger.info("Recovered existing index", log_path=str(log_path))
                    return index
                else:
                    logger.warning(
                        "Index validation failed, rebuilding",
                        log_path=str(log_path),
                    )
                    index.close()
                    index_path.unlink()
            
            except Exception as e:
                logger.warning(
                    "Failed to open existing index, rebuilding",
                    error=str(e),
                )
                if index_path.exists():
                    index_path.unlink()
        
        return IndexRecovery.rebuild_index(
            log_path=log_path,
            base_offset=base_offset,
            index_interval_bytes=index_interval_bytes,
        )
