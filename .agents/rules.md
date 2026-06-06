# Quy tắc và Hướng dẫn Phát triển (Development Rules & Guidelines)

Dự án này là một **Kho dữ liệu Thương mại điện tử (eCommerce Data Warehouse)**, sử dụng các công nghệ thuộc Modern Data Stack (Python, SQL, DuckDB, dbt, Pandas, Spark). Tài liệu này thiết lập các tiêu chuẩn và nguyên tắc mà Agent (AI assistant) phải tuân thủ khi phát triển và bảo trì mã nguồn trong repository này.

---

## 1. Ngôn ngữ Giao tiếp (Communication)
- **Ngôn ngữ phản hồi chính**: Sử dụng tiếng Việt (Vietnamese) rõ ràng, tự nhiên và chuyên nghiệp để trả lời người dùng, trừ khi có yêu cầu khác.
- **Tài liệu & Code**: Viết code, ghi chú (comments), tài liệu API và mô tả commit bằng tiếng Anh (English) để đồng bộ với môi trường phát triển kỹ thuật.

---

## 2. Tiêu chuẩn Lập trình (Coding Standards)

### A. Python (Pandas, PySpark, Scripts)
- **Định dạng**: Tuân thủ tiêu chuẩn PEP 8. Sử dụng type hints đầy đủ cho hàm và biến.
- **Log**: Luôn sử dụng thư viện `logging` của Python thay vì sử dụng câu lệnh `print` thông thường.
- **Môi trường**: Luôn chạy code trong môi trường ảo đã được cấu hình (ví dụ: `.venv`). Sử dụng `pip` hoặc công cụ quản lý package tương ứng một cách cẩn thận.
- **Xử lý dữ liệu**:
  - Khi xử lý dữ liệu lớn, ưu tiên các giải pháp streaming hoặc tối ưu hóa bộ nhớ (ví dụ: chunking trong Pandas hoặc tận dụng PySpark DataFrame thay vì Pandas thường).
  - Tránh các vòng lặp hàng (`for index, row in df.iterrows()`), ưu tiên các hàm vector hóa (`vectorized operations`).

### B. SQL & dbt (Data Build Tool)
- **Định dạng**: Viết hoa các từ khóa SQL (ví dụ: `SELECT`, `FROM`, `WHERE`, `JOIN`).
- **Cấu trúc truy vấn**:
  - Ưu tiên sử dụng CTEs (Common Table Expressions) thay vì subqueries để tăng khả năng đọc hiểu và tái sử dụng.
  - Sử dụng ký tự alias rõ ràng (ví dụ: `orders AS o` thay vì đặt tên tùy tiện).
- **dbt**:
  - Các tệp model phải được phân chia rõ ràng theo các layer: `staging`, `intermediate`, và `marts`.
  - Luôn định nghĩa schema và mô tả chi tiết trong tệp `schema.yml`.
  - Viết các test cơ bản (`unique`, `not_null`, `relationships`) cho các cột khóa chính và khóa ngoại.

---

## 3. Chất lượng & Kiểm thử Dữ liệu (Data Quality & Testing)
- **Kiểm thử dữ liệu đầu vào**: Luôn thực hiện kiểm tra kiểu dữ liệu (`data types`), giá trị null, và trùng lặp trước khi load vào kho dữ liệu.
- **Xử lý dữ liệu nhạy cảm (PII)**: 
  - Đảm bảo các thông tin cá nhân của khách hàng (Họ tên, Số điện thoại, Email) phải được mã hóa (hashing) hoặc ẩn danh (masking) trước khi lưu trữ ở các layer tiếp theo.
- **Xử lý lỗi**: Tích hợp khối lệnh `try-except` đầy đủ trong các pipeline ETL/ELT và ghi log chi tiết khi xảy ra lỗi.

---

## 4. Cấu trúc Thư mục Dự án (Project Directory Structure)
Khi tạo mới thư mục hoặc tệp tin, hãy tuân theo sơ đồ tổ chức dưới đây:
- `data/`: Chứa dữ liệu thô (raw data) cục bộ để test hoặc dữ liệu mẫu dạng csv/parquet (được đưa vào `.gitignore`).
- `src/`: Mã nguồn Python cho việc ingestion, ETL, utility functions.
- `dbt_project/` hoặc `models/`: Chứa mã nguồn dbt để chuyển đổi dữ liệu.
- `notebooks/`: Chứa các tệp Jupyter Notebook (`.ipynb`) phục vụ phân tích dữ liệu khám phá (EDA).
- `tests/`: Chứa mã nguồn kiểm thử chất lượng code (unit tests).

---

## 5. Tương tác với Hệ thống & Lệnh chạy
- Khi đề xuất chạy một command, phải đảm bảo command đó an toàn và được chạy trong thư mục làm việc chính xác.
- Chỉ đề xuất cài đặt thư viện mới khi thực sự cần thiết và phải tương thích với các phiên bản hiện tại trong dự án.

---

## 6. Quy tắc Đặt tên File (File Naming Conventions)

Tên file phải rõ ràng, mô tả đúng chức năng, và tuân thủ chuẩn Python module naming.

### A. Quy tắc chung
- **Chỉ dùng chữ thường** (`lowercase`) và dấu gạch dưới (`snake_case`). Ví dụ: `ingest_bronze.py`, KHÔNG dùng `IngestBronze.py` hay `ingest-bronze.py`.
- **KHÔNG bắt đầu tên file bằng số**. Ví dụ: `ingest_bronze.py`, KHÔNG dùng `00_ingest.py`. Lý do: Python không cho phép import trực tiếp module có tên bắt đầu bằng số, buộc phải dùng `importlib` — đây là anti-pattern.
- **Tên file phải tự giải thích** (`self-descriptive`). Người đọc phải hiểu được chức năng chính của file chỉ từ tên, không cần mở code.

### B. Quy ước đặt tên theo vai trò (Naming Patterns)

| Vai trò | Mẫu đặt tên | Ví dụ |
|---------|-------------|-------|
| Script ETL chính | `<hành_động>_<layer/entity>.py` | `ingest_bronze.py`, `clean_silver.py` |
| Xây dựng Dimension | `build_dim_<tên_bảng>.py` | `build_dim_users.py`, `build_dim_categories.py` |
| Xây dựng Fact Table | `build_fact_<tên_bảng>.py` | `build_fact_events.py` |
| Điều phối Pipeline | `run_pipeline.py` | `run_pipeline.py` |
| Module tiện ích dùng chung | `<chức_năng>.py` trong `src/common/` | `data_quality.py`, `etl_metadata.py` |
| Cấu hình | `<tên_hệ_thống>_config.py` trong `config/` | `spark_config.py` |
| File test | `test_<chức_năng_được_test>.py` | `test_scd2_logic.py`, `test_3nf_integrity.py` |

### C. Quy ước đặt tên thư mục
- Tên thư mục luôn dùng `snake_case` và viết thường.
- Thư mục chứa dữ liệu theo tầng Medallion: `data/raw/`, `data/bronze/`, `data/silver/`, `data/gold/`.
- Thư mục module phải có file `__init__.py` để Python nhận diện là package.

### D. Quy ước đặt tên file Parquet Output
- Tên file Parquet output phải khớp tên bảng logic: `events_raw.parquet`, `events_clean.parquet`, `dim_users.parquet`, `fact_events.parquet`.
- KHÔNG dùng tên chung chung như `output.parquet` hay `data.parquet`.
