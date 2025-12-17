#!/usr/bin/env python3
import io
import shutil
import time
import tarfile
from pathlib import Path
from datetime import datetime, timezone
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class DimmTarCaptureHandler(FileSystemEventHandler):
    def __init__(self, source_file, tar_path, duration_seconds):
        self.source = Path(source_file)
        self.tar_path = tar_path
        self.duration = duration_seconds
        self.start_time = None
        self.copy_count = 0
        self.stop_observer = False
        self.tar_file = tarfile.open(tar_path, 'w:gz')
        
    def on_closed(self, event):
        """Triggered on CLOSE_WRITE - when frame is complete"""
        if event.src_path != str(self.source):
            return
        
        if self.start_time is None:
            self.start_time = time.time()
            print("First frame detected - starting capture!\n")
        
        elapsed = time.time() - self.start_time
        
        if elapsed > self.duration:
            self.stop_observer = True
            return
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{self.source.stem}_{timestamp}.fits"
        
        try:
            # Read file content
            with open(self.source, 'rb') as f:
                file_data = f.read()
            
            # Create tarinfo
            tarinfo = tarfile.TarInfo(name=filename)
            tarinfo.size = len(file_data)
            tarinfo.mtime = time.time()
            
            # Add to tar.gz
            self.tar_file.addfile(tarinfo, io.BytesIO(file_data))
            
            self.copy_count += 1
            
            if self.copy_count % 50 == 0:
                rate = self.copy_count / elapsed if elapsed > 0 else 0
                print(f"Captured {self.copy_count} frames ({rate:.1f} Hz)")
        except Exception as e:
            print(f"Capture error: {e}")
    
    def close(self):
        """Close the tar file"""
        if self.tar_file:
            self.tar_file.close()


def capture_dimm_to_tar(source_file: str, duration_seconds: float = 5.0):
    """Capture DIMM frames directly to tar.gz archive."""
    source = Path(source_file)
    
    if not source.exists():
        print(f"ERROR: File not found: {source}")
        return None
    
    # Create tar.gz filename
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    tar_path = f"./dimm_data_{timestamp}.tar.gz"
    
    print(f"Monitoring: {source}")
    print(f"Output archive: {tar_path}")
    print(f"Duration: {duration_seconds}s")
    print(f"Capturing on: CLOSE_WRITE (complete frames)")
    print(f"Waiting for frames...\n")
    
    event_handler = DimmTarCaptureHandler(source, tar_path, duration_seconds)
    observer = Observer()
    observer.schedule(event_handler, str(source.parent), recursive=False)
    observer.start()
    
    try:
        while not event_handler.stop_observer:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        observer.stop()
        observer.join()
        event_handler.close()  # Close tar file
    
    if event_handler.start_time:
        elapsed = time.time() - event_handler.start_time
        rate = event_handler.copy_count / elapsed if elapsed > 0 else 0
        
        # Get final archive size
        archive_size_mb = Path(tar_path).stat().st_size / (1024 * 1024)
        uncompressed_mb = event_handler.copy_count * 26 / 1024
        compression_ratio = uncompressed_mb / archive_size_mb if archive_size_mb > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"Captured: {event_handler.copy_count} frames")
        print(f"Duration: {elapsed:.3f}s")
        print(f"Actual rate: {rate:.1f} Hz")
        print(f"Archive size: {archive_size_mb:.2f} MB")
        print(f"Uncompressed: {uncompressed_mb:.2f} MB")
        print(f"Compression: {compression_ratio:.1f}x")
        print(f"Saved to: {tar_path}")
        print(f"{'='*50}")
    else:
        print("\n⚠️  No frames captured")
        Path(tar_path).unlink(missing_ok=True)  # Remove empty archive
        return None
    
    return tar_path


if __name__ == "__main__":
    capture_dimm_to_tar(
        source_file="/dimm/dimm/image/dimm_tool/boxframe.fits",
        duration_seconds=30.0
    )
