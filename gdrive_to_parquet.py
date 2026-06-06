import os
import threading
import logging
import gdown
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

# Cấu hình logging theo rules.md
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thay vì hardcode, ưu tiên lấy từ biến môi trường (có giá trị mặc định)
GDRIVE_FILE_ID = os.getenv("GDRIVE_FILE_ID", "1N744AnNIz7GNkfBNqMAk5zru7svIWn12")
PARQUET_PATH = os.getenv("PARQUET_PATH", "data/raw/events_raw.parquet")


def stream_to_parquet() -> None:
    """
    Tải file CSV từ Google Drive thông qua stream (pipe) và ghi trực tiếp
    thành file Parquet để tối ưu bộ nhớ cho dữ liệu cực lớn.
    """
    
    # Tạo thư mục đích nếu chưa tồn tại
    os.makedirs(os.path.dirname(PARQUET_PATH) or '.', exist_ok=True)
    
    read_fd, write_fd = os.pipe()
    download_error = []

    def download():
        try:
            with os.fdopen(write_fd, "wb") as f:
                logger.info(f"Đang bắt đầu tải từ Google Drive (ID: {GDRIVE_FILE_ID})...")
                gdown.download(id=GDRIVE_FILE_ID, output=f, quiet=True, resume=False)
        except Exception as e:
            download_error.append(e)
            logger.error(f"Lỗi tải file: {str(e)}")

    t = threading.Thread(target=download, daemon=True)
    t.start()

    writer = None
    row_count = 0
    
    # Cấu hình PyArrow CSV: 
    # Ép kiểu mặc định thành string để tránh crash khi type inference sai ở giữa file
    read_options = pacsv.ReadOptions(block_size=1024 * 1024 * 5) # Đọc chunk 5MB
    convert_options = pacsv.ConvertOptions(
        strings_can_be_null=True
        # Bạn có thể ép kiểu cụ thể nếu biết trước: column_types={"price": pa.string()} 
    )

    try:
        with os.fdopen(read_fd, "rb") as f:
            reader = pacsv.open_csv(
                f, 
                read_options=read_options, 
                convert_options=convert_options
            )
            for batch in reader:
                table = pa.Table.from_batches([batch])
                if writer is None:
                    writer = pq.ParquetWriter(
                        PARQUET_PATH, 
                        table.schema, 
                        compression="zstd"
                    )
                writer.write_table(table)
                row_count += len(batch)
                
                # In đè dòng hiện tại trên terminal (dùng print an toàn hơn logger trong trường hợp in đè \r liên tục)
                print(f"\r[Ingestion Phase 1] Đã ghi {row_count:,} dòng...", end="", flush=True)
                
    except Exception as e:
        logger.error(f"\nLỗi khi đọc CSV hoặc ghi Parquet: {str(e)}")
        raise
    finally:
        if writer:
            writer.close()

    t.join()
    
    # Ký tự \n giúp ngắt dòng sau khi hàm print(\r...) chạy xong
    if download_error:
        print() 
        raise download_error[0]

    print()
    logger.info(f"Hoàn tất Ingestion! Tổng: {row_count:,} dòng -> {PARQUET_PATH}")


if __name__ == "__main__":
    stream_to_parquet()
