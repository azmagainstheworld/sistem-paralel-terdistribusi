Sistem sinkronisasi terdistribusi fungsional yang mengimplementasikan protokol konsensus Raft, Distributed Locking dengan deteksi deadlock, dan Distributed Queue dengan Consistent Hashing.

Fitur Utama yang Telah Diimplementasikan :
Distributed Lock Manager: Implementasi penuh menggunakan algoritma Raft Consensus untuk menjamin keamanan penguncian di 3 node cluster.  

Deadlock Detection: Menggunakan algoritma Wait-for Graph berbasis DFS untuk mendeteksi siklus penguncian secara proaktif.  

Distributed Queue: Sistem antrean pesan menggunakan Consistent Hashing dengan virtual nodes untuk distribusi beban kerja yang optimal.  

Fault Tolerance & Recovery:

Mekanisme at-least-once delivery dengan pending list.  

Recovery loop otomatis yang mengembalikan pesan stale ke antrean utama.  

High Availability Persistence: Status Raft, Log, dan Lock Table disimpan secara persisten untuk pemulihan setelah terjadi kegagalan node.  

🛠️ Stack Teknologi
Python 3.10: Menggunakan asyncio untuk pemrosesan asinkron non-blocking.  

aiohttp: Sebagai transport layer antar node via REST API.  

Redis 7: Digunakan sebagai persistent message store untuk sistem antrean.  

Docker: Untuk kontainerisasi dan simulasi jaringan terdistribusi yang nyata.