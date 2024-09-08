# Project 3 - Processing Using Airflow and Spark

## Deskripsi Proyek

Proyek ini berfokus pada pemrosesan data menggunakan Apache Airflow dan Apache Spark. Tujuan dari proyek ini adalah untuk mengautomasi dan memproses data dari berbagai sumber menggunakan alur kerja yang ditetapkan dalam Apache Airflow, serta melakukan analisis data menggunakan Apache Spark.

## Struktur Proyek

- **airflow-docker/**: Folder ini berisi konfigurasi dan file yang diperlukan untuk menjalankan Apache Airflow di dalam Docker. Ini termasuk file DAG (Directed Acyclic Graph) yang mendefinisikan alur kerja pemrosesan data.
  - **dags/d_1_batch_processing_spark.py**: Skrip Python yang mendefinisikan DAG Airflow untuk pemrosesan data menggunakan Spark.

## Fitur Utama

- **Data Ingestion**: Mengambil data dari sumber PostgreSQL dan memprosesnya menggunakan Spark.
- **Data Transformation**: Menggunakan Spark untuk mentransformasikan data menjadi format yang diinginkan.
- **Data Loading**: Memuat hasil pemrosesan ke dalam database TiDB.

## Teknologi yang Digunakan

- **Apache Airflow**: Alat orkestrasi alur kerja untuk mengautomasi dan menjadwalkan tugas.
- **Apache Spark**: Platform pemrosesan data terdistribusi untuk analisis data besar.
- **PostgreSQL**: Sistem manajemen basis data relasional yang digunakan sebagai sumber data.
- **TiDB**: Sistem manajemen basis data SQL terdistribusi yang digunakan untuk menyimpan hasil pemrosesan.

## Prerequisites

- Docker
- Apache Airflow
- Apache Spark
- PostgreSQL
- TiDB

## Cara Menjalankan Proyek

1. **Clone Repository**
   ```sh
   git clone https://github.com/Gustiimam/Project-3---Processing-using-airflow-and-spark.git
2. **Directory**
   cd Project-3---Processing-using-airflow-and-spark/airflow-docker
3. **Jalankan Airflow**
   jalankan docker compose up
4. **UI Aiflow**
   Akses UI Airflow: http://localhost:8080
5. **Start DAGS**
   jalankan DAGs d_1_branch_proseccing

##Penjelasan DAG
d_1_batch_processing_spark.py: DAG ini terdiri dari beberapa tugas untuk mengambil data dari PostgreSQL, memproses data menggunakan Spark, dan memuat hasilnya ke dalam TiDB.

Tugas 1: top_countries_get_data - Mengambil data dari tabel country dan city, lalu memprosesnya untuk menghitung jumlah negara.
Tugas 2: top_countries_load_data - Memuat hasil dari top_countries_get_data ke dalam TiDB.
Tugas 3: total_film_get_data - Mengambil data dari tabel film, film_category, dan category, lalu memprosesnya untuk menghitung total film berdasarkan kategori.
Tugas 4: total_film_load_data - Memuat hasil dari total_film_get_data ke dalam TiDB.


Sesuaikan informasi ini sesuai dengan proyek dan struktur file spesifik yang kamu miliki. Jika ada bagian tambahan yang ingin ditambahkan, seperti panduan penggunaan, troubleshooting, atau dokumentasi lebih lanjut, kamu bisa menambahkannya sesuai kebutuhan.
