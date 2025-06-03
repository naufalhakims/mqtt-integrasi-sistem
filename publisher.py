# mqtt_publisher_full.py

import paho.mqtt.client as mqtt
import paho.mqtt.properties as mqtt_props
import paho.mqtt.packettypes as mqtt_packettypes
import time
import ssl
import uuid
import json
import threading
import signal # Pastikan signal diimport

# --- Konfigurasi Umum (UNTUK HIVEMQ CLOUD) ---
BROKER_ADDRESS = "2a4b87c12f234df1adead04ea3a95c23.s1.eu.hivemq.cloud"  # GANTI DENGAN URL CLUSTER ANDA
PORT_MQTTS = 8883
USERNAME = "naufalhakim"  # GANTI DENGAN USERNAME ANDA
PASSWORD = "Insis123"  # GANTI DENGAN PASSWORD ANDA

# --- Global variables untuk graceful shutdown ---
running = True  # Variabel global untuk mengontrol loop utama publisher
publisher_client_id_global = f"python-publisher-{uuid.uuid4()}"
publisher_ping_counter = 0

# --- State untuk Request/Response dari Publisher ---
publisher_pending_responses = {}
publisher_response_events = {}
publisher_client_id_global = f"python-publisher-{uuid.uuid4()}" # Global agar bisa diakses untuk response topic

# Signal handler untuk publisher
def publisher_signal_handler(sig, frame):
    global running
    # Dapatkan nama sinyal untuk logging yang lebih informatif
    signal_name = signal.Signals(sig).name if isinstance(sig, signal.Signals) else str(sig)
    print(f'\n🚨 Publisher ({publisher_client_id_global}) menerima sinyal shutdown ({signal_name})...')
    running = False

# Daftarkan signal handlers untuk publisher
signal.signal(signal.SIGINT, publisher_signal_handler)
signal.signal(signal.SIGTERM, publisher_signal_handler)

# --- Callback Functions ---
def on_subscribe_publisher(client, userdata, mid, reason_codes_list, properties=None):
    print(f"PUBLISHER LOG: on_subscribe callback dipicu untuk MID: {mid}")
    for i, rc in enumerate(reason_codes_list):
        if isinstance(rc, mqtt.ReasonCodes):
            if rc.is_failure:
                print(f"   ⚠️ Publisher: Langganan item-{i} di MID {mid} GAGAL. Alasan: {str(rc)}")
            else:
                print(f"   ✅ Publisher: Langganan item-{i} di MID {mid} BERHASIL. Kode/QoS: {str(rc)}")
        else:
            print(f"   ℹ️ Publisher: Langganan item-{i} di MID {mid} memiliki kode int: {rc}")


def on_connect(client, userdata, flags, reason_code, properties=None):
    rc_object = reason_code
    if isinstance(reason_code, int):
        rc_object = mqtt.ReasonCodes(mqtt.CONNACK, reason_code)

    if rc_object.is_failure:
        print(f"❌ Publisher Gagal terhubung: {str(rc_object)}")
    else:
        print(f"✅ Publisher Terhubung ke Broker MQTT! (RC: {str(rc_object)})")
        print(f"   CONNECT Flags: {flags}")
        if properties:
            print(f"   CONNECT Properties: {properties}")
        
        response_topic_for_publisher = f"client/{publisher_client_id_global}/response"
        subscriptions = [
            ("pong/sub_to_pub", 1),             # <<< GANTI NAMA TOPIK: Menerima PONG dari subscriber (balasan PING publisher)
            (response_topic_for_publisher, 1),  # Untuk menerima response dari subscriber
            ("actions/system/get_info", 1),     # Topik dimana subscriber bisa mengirim request ke publisher
            ("ping/sub_to_pub", 1)              # <<< TAMBAHKAN: Menerima PING dari subscriber
        ]
        overall_result, mid = client.subscribe(subscriptions)
        if overall_result == mqtt.MQTT_ERR_SUCCESS:
            print(f"   ✉️ Publisher: Paket SUBSCRIBE (MID: {mid}) berhasil dikirim untuk {len(subscriptions)} topik.")
        else:
            print(f"   ⚠️ Publisher: Gagal mengirim paket SUBSCRIBE. Error: {mqtt.error_string(overall_result)}")


def on_disconnect(client, userdata, reason_code, properties=None):
    rc_object = reason_code # Di v2, reason_code adalah objek ReasonCode atau None
    if rc_object is None: # Disconnect normal
        print("ℹ️ Publisher Terputus secara normal.")
    elif isinstance(reason_code, int): # Kompatibilitas
        rc_object = mqtt.ReasonCodes(mqtt.DISCONNECT, reason_code)
        print(f"⚠️ Publisher Terputus dari Broker MQTT! (RC: {str(rc_object)})")
    else: # Objek ReasonCode
         print(f"⚠️ Publisher Terputus dari Broker MQTT! (RC: {str(rc_object)})")

    if properties:
        print(f"   DISCONNECT Properties: {properties}")


def on_publish(client, userdata, mid, reason_codes=None, properties=None):
    # Untuk Paho MQTT v2.0.0+, reason_codes adalah objek ReasonCode tunggal atau None
    # Untuk versi lama, bisa jadi integer atau tidak ada.
    
    print(f"DEBUG_ON_PUBLISH: MID {mid}, Reason_codes type: {type(reason_codes)}, Value: {reason_codes}") # TAMBAHKAN DEBUG INI

    if isinstance(reason_codes, mqtt.ReasonCodes): # Ini adalah path untuk MQTTv5 ACK dari broker (Paho v2+)
        if not reason_codes.is_failure:
            print(f"✅ Publisher: Pesan MID {mid} TERKONFIRMASI oleh Broker (ACK: {str(reason_codes)}).")
        else:
            print(f"⚠️ Publisher: Pesan MID {mid} GAGAL dikonfirmasi Broker. Alasan: {str(reason_codes)}")
    elif isinstance(reason_codes, int) and reason_codes == 0: # Sukses sederhana (mungkin QoS 0 di Paho lama)
         print(f"✅ Publisher: Pesan MID {mid} terkirim (RC: {reason_codes}, kemungkinan QoS 0 atau Paho < 2.0).")
    elif reason_codes is None: # Umumnya berarti QoS 0, pesan diserahkan ke OS (Paho v2+)
         print(f"✅ Publisher: Pesan MID {mid} (QoS 0) diserahkan ke network stack.")
    else: # Kasus lain atau error sebagai integer
        print(f"⚠️ Publisher: Info/Gagal publish untuk MID {mid}, Raw Reason_codes/RC: {reason_codes}")

def on_message_publisher(client, userdata, msg):
    print(f"DEBUG_ON_MESSAGE_PUBLISHER: Pesan diterima di Paho callback untuk topik: {msg.topic}")
    print(f"\n🔔 PUBLISHER MENERIMA PESAN:")
    payload_str = ""
    try:
        payload_str = msg.payload.decode('utf-8')
    except UnicodeDecodeError:
        payload_str = f"[Binary data, {len(msg.payload)} bytes]"

    print(f"   Topic   : {msg.topic}")
    print(f"   Payload : {payload_str}")
    print(f"   QoS     : {msg.qos}")

    correlation_id = None
    if msg.properties:
        props = msg.properties
        if hasattr(props, 'CorrelationData') and props.CorrelationData:
            correlation_id = props.CorrelationData.decode('utf-8')
            print(f"   Correlation ID: {correlation_id}")

    # 1. Handle PONG dari Subscriber (balasan PING dari Publisher)
    if msg.topic == "pong/sub_to_pub": # <<< GANTI NAMA TOPIK
        print(f"   🏓 PONG diterima dari subscriber (balasan PING kita): {payload_str}")

    # 2. Handle Response dari Subscriber (untuk request yang dikirim publisher)
    elif msg.topic == f"client/{publisher_client_id_global}/response" and correlation_id:
        # ... (logika handle response tetap sama) ...
        print(f"   📬 RESPONSE diterima untuk Correlation ID '{correlation_id}': {payload_str}")
        if correlation_id in publisher_pending_responses:
            publisher_pending_responses[correlation_id] = payload_str
            if correlation_id in publisher_response_events:
                publisher_response_events[correlation_id].set()
        else:
            print("   ⚠️ Diterima response untuk correlation ID yang tidak diketahui atau sudah timeout.")
    
    # 3. Handle Request dari Subscriber (Publisher sebagai Responder)
    elif msg.topic == "actions/system/get_info":
        # ... (logika handle request dari subscriber tetap sama) ...
        print(f"   ❓ REQUEST diterima dari subscriber: {payload_str}")
        response_topic_from_sub = None
        if msg.properties and hasattr(msg.properties, 'ResponseTopic') and msg.properties.ResponseTopic:
            response_topic_from_sub = msg.properties.ResponseTopic
        
        if response_topic_from_sub and correlation_id:
            handle_system_info_request(client, payload_str, response_topic_from_sub, correlation_id)
        else:
            print("   ⚠️ Request dari subscriber tidak memiliki ResponseTopic atau CorrelationData.")

    # 4. Handle PING dari Subscriber (Publisher merespons dengan PONG)
    elif msg.topic == "ping/sub_to_pub": # <<< TAMBAHKAN LOGIKA INI
        print(f"   🏓 PING diterima dari subscriber: {payload_str}")
        pong_payload = "PONG_FROM_PUBLISHER"
        # Kirim PONG ke topik yang didengarkan subscriber untuk PONG
        client.publish("pong/pub_to_sub", payload=pong_payload, qos=1) # <<< GANTI NAMA TOPIK TUJUAN PONG
        print(f"   📤 PONG (balasan PING subscriber) dikirim ke 'pong/pub_to_sub'")


def handle_system_info_request(client, request_payload, response_topic, correlation_id):
    """Publisher memproses request 'get_system_info' dari subscriber"""
    print(f"   ⚙️ Memproses 'get_system_info' request dari subscriber...")
    try:
        request_data = json.loads(request_payload)
    except json.JSONDecodeError:
        request_data = {}

    response_data = {
        "status": "OK",
        "publisher_id": publisher_client_id_global,
        "timestamp": time.time(),
        "system_load": "low", # Contoh data
        "original_request": request_data
    }
    response_payload = json.dumps(response_data)

    resp_props = mqtt_props.Properties(mqtt_packettypes.PacketTypes.PUBLISH)
    resp_props.CorrelationData = correlation_id.encode('utf-8')

    client.publish(response_topic, payload=response_payload, qos=1, properties=resp_props)
    print(f"   📤 RESPONSE (system_info) dikirim ke '{response_topic}'")


def on_log(client, userdata, level, buf):
    print(f"PUBLISHER LOG: {buf}")


def send_request_from_publisher(client, request_payload_dict, request_topic, qos=1, timeout=10):
    """Mengirim request dari publisher dan menunggu response."""
    response_topic = f"client/{publisher_client_id_global}/response" # Publisher punya response topic sendiri
    correlation_id = str(uuid.uuid4())

    publisher_pending_responses[correlation_id] = None
    publisher_response_events[correlation_id] = threading.Event()

    req_props = mqtt_props.Properties(mqtt_packettypes.PacketTypes.PUBLISH)
    req_props.ResponseTopic = response_topic
    req_props.CorrelationData = correlation_id.encode('utf-8')
    # req_props.MessageExpiryInterval = 60 # Jika request perlu expiry

    request_payload_str = json.dumps(request_payload_dict)
    result = client.publish(request_topic, payload=request_payload_str, qos=qos, properties=req_props)

    print(f"\n🚀 REQUEST dikirim ke '{request_topic}' dari publisher.")
    print(f"   Correlation ID: {correlation_id}")
    print(f"   Response diharapkan di: {response_topic}")

    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        if publisher_response_events[correlation_id].wait(timeout=timeout):
            response = publisher_pending_responses.get(correlation_id)
            print(f"   📬 RESPONSE diterima untuk request publisher: {response}")
        else:
            print(f"   ⌛️ TIMEOUT menunggu response untuk Correlation ID: {correlation_id}")
    else:
        print(f"   ❌ Gagal mengirim request (RC: {result.rc})")

    # Cleanup
    if correlation_id in publisher_pending_responses: del publisher_pending_responses[correlation_id]
    if correlation_id in publisher_response_events: del publisher_response_events[correlation_id]


def run_publisher():
    global running  # Pastikan 'running' diakses sebagai global
    global publisher_client_id_global, publisher_ping_counter # 'running' sudah global di level modul

    publisher = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=publisher_client_id_global,
        protocol=mqtt.MQTTv5
    )

    # ... (setup username_pw_set, callbacks, LWT, tls_set_context tetap sama) ...
    publisher.username_pw_set(USERNAME, PASSWORD)
    publisher.on_connect = on_connect
    publisher.on_subscribe = on_subscribe_publisher 
    publisher.on_disconnect = on_disconnect
    publisher.on_publish = on_publish 
    publisher.on_message = on_message_publisher
    publisher.on_log = on_log

    lwt_topic = "status/publisher/lastwill"
    lwt_payload = json.dumps({"id": publisher_client_id_global, "status": "offline_unexpectedly", "time": time.time()})
    lwt_props = mqtt_props.Properties(mqtt_packettypes.PacketTypes.WILLMESSAGE)
    lwt_props.MessageExpiryInterval = 3600
    lwt_props.WillDelayInterval = 5
    lwt_props.UserProperty = [("source", "LWT-Publisher-Full"), ("priority", "high")]
    publisher.will_set(
        lwt_topic, payload=lwt_payload, qos=1, retain=False, properties=lwt_props
    )
    print(f"📜 Last Will and Testament diatur ke topik: {lwt_topic}")

    try:
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        publisher.tls_set_context(context)
        print("🔒 TLS setup berhasil untuk HiveMQ Cloud")
    except Exception as e:
        print(f"💥 Publisher Error TLS setup: {e}")
        return

    try:
        print(f"🔗 Menghubungkan Publisher ke {BROKER_ADDRESS}:{PORT_MQTTS}...")
        publisher.connect(BROKER_ADDRESS, PORT_MQTTS, keepalive=60)
    except Exception as e:
        print(f"💥 Publisher Gagal terhubung ke broker: {e}")
        return

    publisher.loop_start()

    print("⏳ Menunggu koneksi publisher stabil...")
    time.sleep(3) 

    if not publisher.is_connected() and running: # Tambahkan cek 'running'
        print("❌ Publisher tidak dapat terhubung. Keluar.")
        publisher.loop_stop()
        return
    elif not running: # Jika sinyal shutdown diterima saat menunggu koneksi
        print("Publisher dihentikan saat menunggu koneksi.")
        if publisher.is_connected(): publisher.disconnect()
        publisher.loop_stop()
        return
    
    print("\n" + "="*40)
    print("🚀 PUBLISHER SIAP MELAKUKAN SEMUA AKSI 🚀")
    print("="*40 + "\n")

    last_ping_time = 0 # Inisialisasi agar PING pertama langsung dikirim
    ping_interval = 5 

    try:
        # --- Aksi Awal Publisher (dikirim sekali) ---
        if running: # Hanya lakukan aksi awal jika masih 'running'
            print("\n=== 1. Testing QoS 0, 1, dan 2 ===")
            # ... (kode publish QoS seperti sebelumnya) ...
            topic_qos = "sensor/data_qos"
            publisher.publish(topic_qos, payload=json.dumps({"msg": "QoS 0 test"}), qos=0)
            time.sleep(0.5)
            if not running: raise KeyboardInterrupt # Cek flag setelah sleep
            publisher.publish(topic_qos, payload=json.dumps({"msg": "QoS 1 test"}), qos=1)
            time.sleep(0.5)
            if not running: raise KeyboardInterrupt
            publisher.publish(topic_qos, payload=json.dumps({"msg": "QoS 2 test"}), qos=2)
            print("   Pesan QoS 0, 1, 2 telah dikirim.")
            time.sleep(1)
            if not running: raise KeyboardInterrupt

            print("\n=== 2. Testing Retained Message ===")
            # ... (kode publish Retained seperti sebelumnya) ...
            topic_retained = "config/device/static_info"
            retained_payload = json.dumps({"fw_version": "1.2.3", "sn": "SN_PUB_001"})
            publisher.publish(topic_retained, payload=retained_payload, qos=1, retain=True)
            print(f"   Retained message dikirim ke '{topic_retained}'.")
            time.sleep(1)
            if not running: raise KeyboardInterrupt

            print("\n=== 3. Testing Message Expiry ===")
            # ... (kode publish Expiry seperti sebelumnya) ...
            topic_expiry = "notifications/temporary_alert"
            expiry_props = mqtt_props.Properties(mqtt_packettypes.PacketTypes.PUBLISH)
            expiry_props.MessageExpiryInterval = 15
            publisher.publish(
                topic_expiry,
                payload=json.dumps({"alert_msg": "Temporary critical alert!"}),
                qos=1,
                properties=expiry_props
            )
            print(f"   Pesan dengan expiry 15 detik dikirim ke '{topic_expiry}'.")
            time.sleep(1)
            if not running: raise KeyboardInterrupt

            print("\n=== 4. Testing Request/Response (Publisher -> Subscriber) ===")
            # ... (kode panggil send_request_from_publisher seperti sebelumnya) ...
            request_payload_pub = {"cmd": "get_status", "id": "SENSOR_001"}
            send_request_from_publisher(publisher, request_payload_pub, "actions/device/get_status", qos=1, timeout=10)
            time.sleep(1) # Jeda sebelum loop periodik PING
            if not running: raise KeyboardInterrupt


        print("\n=== AKSI AWAL PUBLISHER TELAH DILAKUKAN (jika berjalan) ===")
        print("Publisher akan mulai mengirim PING periodik dan tetap berjalan...")
        print("Tekan Ctrl+C untuk menghentikan.")
        
        # Loop utama publisher untuk PING periodik
        # Variabel 'running' di sini akan merujuk ke 'running' global di level modul
        while running: 
            current_time = time.time()
            if publisher.is_connected() and (current_time - last_ping_time > ping_interval):
                publisher_ping_counter += 1
                ping_payload = f"PING_DARI_PUBLISHER_KE_{publisher_ping_counter}"

                print(f"\n📡 Publisher mengirim PING Periodik #{publisher_ping_counter}: {ping_payload}")
                ping_result = publisher.publish("ping/pub_to_sub", payload=ping_payload, qos=1)
                if ping_result.rc != mqtt.MQTT_ERR_SUCCESS:
                    print(f"   ⚠️ Gagal mengirim PING periodik (RC: {ping_result.rc})")
                last_ping_time = current_time
            
            # Loop ini harus memiliki sleep agar tidak memakan CPU 100%
            # Dan juga agar loop network Paho mendapatkan kesempatan berjalan
            for _ in range(10): # Cek 'running' lebih sering
                if not running:
                    break
                time.sleep(0.1)
            if not running:
                break
        
        if not running:
            print("Loop utama publisher dihentikan karena flag 'running' False.")

    # except KeyboardInterrupt:
    #     # Ini kemungkinan besar tidak akan tercapai jika signal_handler bekerja dengan baik
    #     # dan mengatur 'running' ke False, menyebabkan loop while berhenti secara alami.
    #     print("\nℹ️ Publisher dihentikan oleh pengguna (KeyboardInterrupt terdeteksi di try-except).")
    #     # 'running' sudah diatur False oleh signal_handler
    except Exception as e:
        print(f"💥 Error di loop utama publisher: {e}")
        # Tidak perlu 'global running' di sini, signal_handler yang utama
        # Cukup pastikan loop berhenti jika ada error tak terduga
        # Untuk memastikan assignment di bawah ini menarget global
        running = False # Hentikan loop jika ada error lain
    finally:
        print("\n🔌 Membersihkan publisher...")
        if publisher.is_connected():
            publisher.disconnect()
        publisher.loop_stop() 
        print("✅ Publisher berhenti.")

# if __name__ == "__main__": tetap sama
if __name__ == "__main__":
    run_publisher()
