# Tìm hiểu các Tools và Framework của Big Data

## 1. Giới thiệu
Big Data là tập hợp các dữ liệu có **khối lượng lớn (Volume)**, **tốc độ sinh dữ liệu cao (Velocity)** và **đa dạng định dạng (Variety)**, khiến việc xử lý bằng các công cụ truyền thống trở nên khó khăn.  
Để giải quyết vấn đề này, nhiều **framework và công cụ Big Data** đã được phát triển nhằm hỗ trợ việc **thu thập, lưu trữ, xử lý, phân tích và trực quan hóa dữ liệu** hiệu quả hơn.

---

## 2. Kiến trúc tổng quan của hệ thống Big Data
Một hệ thống Big Data thường được chia thành 5 tầng chính:

| Tầng | Chức năng | Ví dụ công cụ |
|------|------------|----------------|
| **Thu thập dữ liệu (Data Ingestion)** | Lấy dữ liệu từ nhiều nguồn khác nhau như API, cảm biến, log... | Kafka, Flume, Sqoop |
| **Lưu trữ dữ liệu (Storage)** | Lưu trữ dữ liệu lớn, có thể phân tán | HDFS, Amazon S3, HBase, Cassandra |
| **Xử lý dữ liệu (Processing)** | Biến dữ liệu thô thành dữ liệu có giá trị | Hadoop MapReduce, Apache Spark, Apache Flink |
| **Phân tích & Truy vấn (Analytics)** | Cho phép truy vấn dữ liệu bằng SQL hoặc script | Hive, Impala, Presto, Pig |
| **Trực quan hóa (Visualization)** | Hiển thị kết quả phân tích dưới dạng biểu đồ | Power BI, Tableau, Superset, Streamlit |

---

## 3. Các công cụ và framework phổ biến

### 3.1. Lưu trữ dữ liệu
- **HDFS (Hadoop Distributed File System)**: hệ thống file phân tán của Hadoop.
- **Apache HBase**: cơ sở dữ liệu NoSQL chạy trên HDFS, tối ưu cho dữ liệu dạng cột.
- **Cassandra / MongoDB**: NoSQL database linh hoạt, phù hợp dữ liệu phi cấu trúc.
- **Amazon S3**: kho lưu trữ đám mây phổ biến của AWS.

### 3.2. Xử lý dữ liệu
- **Apache Hadoop**: mô hình xử lý batch theo cơ chế MapReduce.
- **Apache Spark**: xử lý nhanh gấp nhiều lần Hadoop nhờ dùng bộ nhớ RAM, hỗ trợ batch và streaming.
- **Apache Flink**: chuyên cho xử lý dữ liệu streaming thời gian thực.

### 3.3. Truyền dữ liệu thời gian thực (Streaming)
- **Apache Kafka**: hệ thống message queue giúp truyền dữ liệu liên tục giữa các dịch vụ.
- **Apache Storm / Flink**: xử lý real-time streams.

### 3.4. Quản lý luồng công việc
- **Apache Airflow**: công cụ điều phối (orchestrator) giúp tự động hóa và lập lịch các pipeline dữ liệu.
- **Oozie**: điều phối các job trong hệ sinh thái Hadoop.

### 3.5. Machine Learning và AI
- **MLlib (trong Spark)**: thư viện học máy chạy song song trên dữ liệu lớn.
- **TensorFlow on Spark**: kết hợp deep learning với xử lý dữ liệu phân tán.

### 3.6. Trực quan hóa
- **Power BI / Tableau**: công cụ thương mại mạnh mẽ.
- **Apache Superset / Streamlit**: giải pháp mã nguồn mở cho dashboard dữ liệu.

---

## 4. Ví dụ về một pipeline Big Data điển hình
Một quy trình Big Data thực tế có thể bao gồm:

Nguồn dữ liệu (API / WebSocket)
↓
Kafka → Spark → Iceberg → Streamlit
↑
Airflow (lên lịch xử lý)



- **Kafka**: thu thập dữ liệu thời gian thực.  
- **Spark**: xử lý dữ liệu batch hoặc streaming.  
- **Airflow**: tự động điều phối pipeline.  
- **Iceberg**: lưu dữ liệu sạch, truy vấn nhanh.  
- **Streamlit**: trực quan hóa dữ liệu cho người dùng cuối.

---

## 5. Kết luận
Hệ sinh thái Big Data bao gồm nhiều công cụ hoạt động ở các tầng khác nhau, từ thu thập đến hiển thị dữ liệu.  
Sự kết hợp giữa các công cụ như **Kafka, Spark, Airflow, Iceberg và Streamlit** giúp xây dựng một hệ thống **xử lý dữ liệu lớn, nhanh và hiệu quả**, đáp ứng nhu cầu phân tích và ra quyết định trong thời gian thực.
