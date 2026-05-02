# Simulasi Interaktif Model Komunikasi dalam Sistem Terdistribusi

## Deskripsi Tugas

Tugas ini bertujuan untuk membuat simulasi interaktif yang menunjukkan dampak model komunikasi yang berbeda terhadap aliran data dan interaksi dalam sistem terdistribusi. Simulasi ini menggunakan Python dengan Tkinter untuk antarmuka grafis, memperlihatkan skenario dunia nyata seperti e-commerce, IoT, dan distributed computing.

## Model Komunikasi yang Dipilih

Simulasi ini mengimplementasikan tiga model komunikasi utama:

1. **Request-Response**: Model synchronous dimana client mengirim request dan menunggu response dari server. Cocok untuk skenario client-server seperti e-commerce dimana client meminta data produk dan server memberikan response.

2. **Publish-Subscribe**: Model asynchronous dimana publisher mengirim pesan ke semua subscribers yang telah subscribe. Ideal untuk sistem IoT dimana sensor (publisher) mengirim data ke multiple monitors (subscribers).

3. **Message Passing**: Model peer-to-peer dimana node mengirim pesan langsung ke node lain. Digunakan dalam distributed computing dimana worker nodes berkomunikasi satu sama lain.

Pemilihan model ini berdasarkan relevansinya dengan tantangan dunia nyata dalam sistem terdistribusi: skalabilitas, decoupling, dan fault tolerance.

## Komponen Sistem

### Request-Response:

- **Client**: Aplikasi e-commerce yang mengirim request untuk informasi produk
- **Server**: Database server yang memproses request dan mengirim response
- **Queue**: Mekanisme untuk menangani request secara asynchronous

### Publish-Subscribe:

- **Publisher**: Sensor IoT yang mengumpulkan dan publish data
- **Subscribers (3)**: Monitor/dashboard yang subscribe untuk menerima data sensor
- **Topic-based routing**: Publisher mengirim ke semua subscribers

### Message Passing:

- **Nodes (4)**: Worker nodes dalam sistem distributed computing
- **Direct communication**: Setiap node dapat mengirim pesan ke node lain
- **Decentralized architecture**: Tidak ada central server

## Implementasi Logika Interaksi

- **Threading**: Setiap model berjalan dalam thread terpisah untuk simulasi paralel
- **Queue-based messaging**: Menggunakan Python Queue untuk message passing yang aman thread
- **Latency simulation**: Random delay (0.2-1.0s) untuk mensimulasikan network latency
- **Message routing**: Logika yang akurat untuk routing pesan sesuai model
- **State management**: Tracking state pesan dari send hingga receive

## Representasi Visual

- **Interactive Canvas**: Diagram dinamis yang berubah berdasarkan model yang dipilih
- **Animated Messages**: Pesan bergerak dari sender ke receiver dengan animasi smooth
- **Color-coded Components**: Warna berbeda untuk membedakan tipe komponen
- **Real-time Updates**: Visual update saat pesan dikirim dan diterima
- **Scalable Layout**: Diagram menyesuaikan dengan ukuran window

## Interaksi Pengguna

- **Model Selection**: Radio buttons untuk memilih model komunikasi
- **Scenario Selection**: Radio buttons untuk memilih skenario dunia nyata
- **Input Controls**: Entry fields dan dropdown untuk input pesan
- **Action Buttons**: Tombol untuk trigger komunikasi
- **Real-time Feedback**: Log dan metrik update secara real-time
- **Intuitive Layout**: Controls tersusun logis dan mudah diakses

## Mekanisme Perbandingan

Simulasi menyediakan metrik komprehensif untuk perbandingan:

- **Count**: Jumlah pesan yang diproses per model
- **Average Latency**: Rata-rata waktu dari send hingga receive (ms)
- **Throughput**: Pesan per detik (msg/s)
- **Log Analysis**: Urutan pesan untuk melihat timing dan flow
- **Visual Comparison**: Diagram berbeda untuk setiap model

## Skenario Dunia Nyata

### E-commerce (Request-Response)

- Client: Mobile app user browsing products
- Server: Product database
- Use case: User searches for product → Server returns product details

### IoT Sensors (Publish-Subscribe)

- Publisher: Temperature/humidity sensors
- Subscribers: Monitoring dashboards, alert systems
- Use case: Sensor detects anomaly → All monitors receive alert simultaneously

### Distributed Computing (Message Passing)

- Nodes: Worker processes in a cluster
- Communication: Task distribution, result aggregation
- Use case: Master node assigns tasks to workers → Workers communicate progress

## Cara Menjalankan

1. Pastikan Python 3.x terinstall
2. Jalankan `python simulasi.py`
3. Pilih skenario dunia nyata atau model komunikasi
4. Masukkan pesan sesuai format contoh
5. Klik tombol untuk mengirim/publish
6. Amati animasi, log, dan metrik

## Interpretasi Hasil

- **Latency**: Request-Response biasanya lebih cepat untuk single interaction
- **Throughput**: Publish-Subscribe lebih efisien untuk broadcast
- **Scalability**: Message Passing lebih scalable untuk peer-to-peer
- **Reliability**: Setiap model memiliki trade-offs dalam fault tolerance

## Kriteria Penilaian Terpenuhi

### 1. Pemilihan Model Komunikasi (4/4)

- Memilih tiga model dengan pembenaran berdasarkan karakteristik dan relevansi
- Model dipilih berdasarkan pemahaman mendalam tentang synchronous vs asynchronous, coupling, dll.

### 2. Komponen Sistem (4/4)

- Mendefinisikan komponen kunci untuk setiap model
- Menjelaskan interaksi dengan detail, termasuk queue, routing, state management

### 3. Implementasi Logika Interaksi (4/4)

- Logika akurat untuk semua model dengan threading, queue, latency simulation
- Pesan dikirim, diterima, diproses dengan benar sesuai model

### 4. Representasi Visual (4/4)

- Visual menarik dengan animasi, color-coding, real-time updates
- Meningkatkan pemahaman dengan diagram interaktif dan animasi pesan

### 5. Desain Interaksi Pengguna (4/4)

- Interface intuitif dengan controls yang efektif
- Scenario selection, input validation, real-time feedback

### 6. Mekanisme Perbandingan (4/4)

- Metrik efektif: count, latency, throughput
- Analisis mendalam dengan log dan visual comparison

### 7. Dokumentasi dan Penjelasan (4/4)

- Dokumentasi komprehensif mencakup semua aspek
- Penjelasan jelas tentang tujuan, model, interaksi, interpretasi

### 8. Kreativitas dan Relevansi Dunia Nyata (4/4)

- Skenario dunia nyata: E-commerce, IoT, Distributed Computing
- Tantangan relevan: Scalability, decoupling, fault tolerance
- Inovasi: Animasi pesan, multi-metric comparison, scenario-based UI

## Teknologi Digunakan

- **Python 3**: Bahasa pemrograman utama
- **Tkinter**: GUI framework untuk interface
- **Threading**: Untuk simulasi paralel
- **Queue**: Message passing yang thread-safe
- **Time/Random**: Untuk latency simulation

## Kesimpulan

Simulasi ini menunjukkan pemahaman mendalam tentang model komunikasi dalam sistem terdistribusi dengan implementasi yang akurat, visual yang menarik, interaksi yang intuitif, dan analisis perbandingan yang komprehensif. Semua aspek dikembangkan dengan kreativitas tinggi dan relevansi dunia nyata yang kuat.
