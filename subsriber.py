# mqtt_subscriber_full.py

import paho.mqtt.client as mqtt
import paho.mqtt.properties as mqtt_props
import paho.mqtt.packettypes as mqtt_packettypes
import time
import ssl
import uuid
import threading
import json
from collections import deque
import signal
import sys

# --- Konfigurasi Umum (UNTUK HIVEMQ CLOUD) ---
BROKER_ADDRESS = "2a4b87c12f234df1adead04ea3a95c23.s1.eu.hivemq.cloud"  # GANTI DENGAN URL CLUSTER ANDA
PORT_MQTTS = 8883
USERNAME = "naufalhakim"  # GANTI DENGAN USERNAME ANDA
PASSWORD = "Insis123"  # GANTI DENGAN PASSWORD ANDA

# --- State untuk Request/Response dari Subscriber ---
subscriber_pending_responses = {}
subscriber_response_events = {}
subscriber_client_id_global = f"python-subscriber-{uuid.uuid4()}"

# --- Flow Control ---
MAX_CONCURRENT_PROCESSING = 3 # Batasi jumlah pesan yang diproses secara bersamaan
active_processing_count = 0
message_queue = deque() # Antrian untuk pesan yang belum diproses
processing_lock = threading.Lock() # Lock untuk sinkronisasi akses ke counter dan queue

# --- Global variables untuk graceful shutdown ---
running = True
subscriber_client_global = None # Untuk diakses oleh signal_handler

def signal_handler(sig, frame):
    global running, subscriber_client_global
    print('\n🚨 Menerima sinyal shutdown...')
    running = False
    if subscriber_client_global and subscriber_client_global.is_connected():
        print("🔌 Disconnecting subscriber...")
        # Kirim properti disconnect jika perlu (MQTTv5)
        # disconnect_props = mqtt_props.Properties(mqtt_packettypes.PacketTypes.DISCONNECT)
        # disconnect_props.ReasonString = "Subscriber shutting down by signal"
        # disconnect_props.UserProperty = [("shutdown_initiator", "signal_handler")]
        # subscriber_client_global.disconnect(properties=disconnect_props)
        subscriber_client_global.disconnect() # Disconnect standar
    # loop_stop() akan dipanggil di finally block run_subscriber
    # sys.exit(0) # Biarkan finally block yang membersihkan


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# --- Callback Functions ---

# TAMBAHKAN CALLBACK INI
def on_subscribe_subscriber(client, userdata, mid, reason_codes_list, properties=None):
    # reason_codes_list adalah list dari ReasonCode objek, satu untuk setiap topik
    # dalam paket SUBSCRIBE dengan MID yang sesuai.
    print(f"SUBSCRIBER LOG: on_subscribe callback dipicu untuk MID: {mid}")
    for i, rc in enumerate(reason_codes_list):
        # Untuk mengetahui topik mana yang terkait dengan rc ini, Anda perlu mencocokkan MID
        # atau berasumsi urutannya sama jika Anda tahu langganan mana yang dikirim dengan MID ini.
        # Untuk kesederhanaan, kita hanya log statusnya.
        if isinstance(rc, mqtt.ReasonCodes): # Seharusnya objek ReasonCode
            if rc.is_failure:
                print(f"   ⚠️ Subscriber: Langganan item-{i} di MID {mid} GAGAL. Alasan: {str(rc)}")
            else:
                # rc juga bisa berupa QoS yang diberikan (0, 1, atau 2) untuk langganan sukses
                print(f"   ✅ Subscriber: Langganan item-{i} di MID {mid} BERHASIL. Kode/QoS: {str(rc)}")
        else: # Fallback jika ternyata integer (kurang umum untuk Paho v2 on_subscribe)
            print(f"   ℹ️ Subscriber: Langganan item-{i} di MID {mid} memiliki kode int: {rc}")

def on_connect_subscriber(client, userdata, flags, reason_code, properties=None):
    global running # <-- DECLARE GLOBAL AT THE START OF THE FUNCTION
    
    rc_object = reason_code
    if isinstance(reason_code, int): # Untuk kompatibilitas jika reason_code adalah int
        rc_object = mqtt.ReasonCodes(mqtt.CONNACK, reason_code)

    if rc_object.is_failure:
        print(f"❌ Subscriber Gagal terhubung: {str(rc_object)}")
        running = False # Hentikan jika koneksi gagal
    else:
        print(f"✅ Subscriber Terhubung ke Broker MQTT! (RC: {str(rc_object)})")
        print(f"   CONNECT Flags: {flags}")
        if properties: print(f"   CONNECT Properties: {properties}")

        response_topic_for_subscriber = f"client/{subscriber_client_id_global}/response"
        subscriptions = [
            ("sensor/data_qos", 2),
            ("config/device/static_info", 1),
            ("notifications/temporary_alert", 1),
            ("status/publisher/lastwill", 1),
            ("actions/device/get_status", 1),
            ("ping/pub_to_sub", 1),                # <<< GANTI NAMA TOPIK: Menerima PING dari publisher
            (response_topic_for_subscriber, 1),
            ("pong/pub_to_sub", 1)                 # <<< GANTI NAMA TOPIK: Menerima PONG dari publisher (balasan PING subscriber)
        ]
        
        overall_result, mid = client.subscribe(subscriptions)
        
        if overall_result == mqtt.MQTT_ERR_SUCCESS:
            print(f"   ✉️ Subscriber: Paket SUBSCRIBE (MID: {mid}) berhasil dikirim untuk {len(subscriptions)} topik.")
        else:
            print(f"   ⚠️ Subscriber: Gagal mengirim paket SUBSCRIBE. Error: {mqtt.error_string(overall_result)}")
            running = False # Hentikan jika pengiriman paket subscribe gagal

def on_disconnect_subscriber(client, userdata, reason_code, properties=None):
    rc_object = reason_code 
    if rc_object is None:
        print("ℹ️ Subscriber Terputus secara normal.")
    elif isinstance(reason_code, int):
        rc_object = mqtt.ReasonCodes(mqtt.DISCONNECT, reason_code)
        print(f"⚠️ Subscriber Terputus dari Broker MQTT! (RC: {str(rc_object)})")
    else: # Objek ReasonCode
         print(f"⚠️ Subscriber Terputus dari Broker MQTT! (RC: {str(rc_object)})")
    if properties: print(f"   DISCONNECT Properties: {properties}")
    # Anda bisa implementasi reconnect logic di sini jika diperlukan


def process_message_thread(client, msg):
    """Fungsi untuk memproses pesan dalam thread terpisah (untuk flow control)."""
    global active_processing_count
    
    try:
        print(f"\n📬 SUBSCRIBER MENERIMA PESAN (Thread: {threading.get_ident()}):")
        payload_str = ""
        try:
            payload_str = msg.payload.decode('utf-8')
        except UnicodeDecodeError:
            payload_str = f"[Binary data, {len(msg.payload)} bytes]"

        print(f"   Topic   : {msg.topic}")
        print(f"   Payload : {payload_str}")
        print(f"   QoS     : {msg.qos}")
        print(f"   Retain  : {msg.retain}")

        correlation_id = None
        response_topic_from_requester = None # Topic untuk mengirim response
        message_expiry_interval = None

        if msg.properties:
            props = msg.properties
            print(f"   Properties:")
            if hasattr(props, 'MessageExpiryInterval') and props.MessageExpiryInterval is not None:
                message_expiry_interval = props.MessageExpiryInterval
                print(f"     - Message Expiry Interval: {message_expiry_interval} detik")
            if hasattr(props, 'CorrelationData') and props.CorrelationData:
                correlation_id = props.CorrelationData.decode('utf-8')
                print(f"     - Correlation ID: {correlation_id}")
            if hasattr(props, 'ResponseTopic') and props.ResponseTopic:
                response_topic_from_requester = props.ResponseTopic
                print(f"     - Response Topic: {response_topic_from_requester}")
            if hasattr(props, 'UserProperty') and props.UserProperty:
                print(f"     - User Properties: {props.UserProperty}")
        
        # --- Handler untuk berbagai jenis pesan ---
        
        # 1. Retained Message (ditandai dengan msg.retain == True)
        if msg.retain:
            print(f"   ➡️ Ini adalah RETAINED MESSAGE dari topik '{msg.topic}'.")
            # Lakukan sesuatu dengan retained message, misal update konfigurasi awal

        # 2. Last Will Testament dari Publisher
        if msg.topic == "status/publisher/lastwill":
            print(f"   💔 LAST WILL TESTAMENT diterima: Publisher terputus tidak normal. Payload: {payload_str}")

        # 3. Message dengan Expiry
        elif msg.topic == "notifications/temporary_alert":
            print(f"   ⏳ TEMPORARY ALERT diterima (akan expired): {payload_str}")
            if message_expiry_interval:
                print(f"      Alert ini memiliki expiry: {message_expiry_interval}s")
        
        # 4. Request dari Publisher (Subscriber sebagai Responder)
        elif msg.topic == "actions/device/get_status" and response_topic_from_requester and correlation_id:
            print(f"   ❓ REQUEST 'get_device_status' diterima dari publisher.")
            handle_device_status_request(client, payload_str, response_topic_from_requester, correlation_id)
        
        # 5. PING dari Publisher
        elif msg.topic == "ping/pub_to_sub": # <<< GANTI NAMA TOPIK
            print(f"   🏓 PING diterima dari publisher: {payload_str}")
            handle_ping_from_publisher(client, payload_str, msg.topic)

        # 6. Response dari Publisher (untuk request yang dikirim subscriber)
        elif msg.topic == f"client/{subscriber_client_id_global}/response" and correlation_id:
            print(f"   📬 RESPONSE diterima untuk Correlation ID '{correlation_id}': {payload_str}")
            if correlation_id in subscriber_pending_responses:
                subscriber_pending_responses[correlation_id] = payload_str
                if correlation_id in subscriber_response_events:
                    subscriber_response_events[correlation_id].set()
            else:
                print("   ⚠️ Diterima response untuk correlation ID yang tidak diketahui atau sudah timeout.")

        # 7. PONG dari Publisher (balasan PING dari Subscriber)
        elif msg.topic == "pong/pub_to_sub": # <<< GANTI NAMA TOPIK / TAMBAHKAN LOGIKA INI
            print(f"   🏓 PONG diterima dari publisher (balasan PING kita): {payload_str}")

    except Exception as e:
        print(f"💥 Error saat memproses pesan di thread: {e}")
    finally:
        with processing_lock:
            active_processing_count -= 1
            # print(f"   Flow Control: Selesai proses. Aktif: {active_processing_count}, Antrian: {len(message_queue)}")


def on_message_subscriber(client, userdata, msg):
    """Callback utama ketika pesan diterima, mengatur flow control."""
    print(f"DEBUG_ON_MESSAGE_SUBSCRIBER: Pesan diterima di Paho callback untuk topik: {msg.topic}") # <--- TAMBAHKAN INI
    global active_processing_count, message_queue
    
    with processing_lock:
        if active_processing_count >= MAX_CONCURRENT_PROCESSING:
            message_queue.append(msg)
            print(f"🚦 Flow Control: Kapasitas penuh ({active_processing_count}/{MAX_CONCURRENT_PROCESSING}). Pesan dari '{msg.topic}' ditambah ke antrian ({len(message_queue)}).")
            return
        
        active_processing_count += 1
        # print(f"   Flow Control: Memulai proses. Aktif: {active_processing_count}, Antrian: {len(message_queue)}")

    # Proses pesan di thread terpisah agar tidak memblokir network loop Paho
    thread = threading.Thread(target=process_message_thread, args=(client, msg))
    thread.daemon = True # Thread akan berhenti jika program utama berhenti
    thread.start()


def check_message_queue(client):
    """Memeriksa dan memproses pesan dari antrian jika ada kapasitas."""
    global active_processing_count, message_queue
    
    if not message_queue: # Jika antrian kosong, tidak ada yang dilakukan
        return

    with processing_lock:
        if active_processing_count < MAX_CONCURRENT_PROCESSING and message_queue:
            msg_from_queue = message_queue.popleft()
            active_processing_count += 1
            print(f"🚦 Flow Control: Mengambil pesan dari antrian untuk '{msg_from_queue.topic}'. Aktif: {active_processing_count}, Sisa Antrian: {len(message_queue)}")
            
            thread = threading.Thread(target=process_message_thread, args=(client, msg_from_queue))
            thread.daemon = True
            thread.start()


# --- Handler Spesifik ---
def handle_device_status_request(client, request_payload_str, response_topic, correlation_id):
    """Memproses request 'get_device_status' dan mengirim response."""
    print(f"   ⚙️ Memproses 'get_device_status' request...")
    try:
        request_data = json.loads(request_payload_str)
    except json.JSONDecodeError:
        request_data = {"error": "Invalid JSON in request"}

    # Simulasi mendapatkan status perangkat
    response_data = {
        "status": "OK",
        "device_id": request_data.get("device_id", "UNKNOWN_DEVICE"),
        "temperature": 28.5,
        "humidity": 55.2,
        "timestamp": time.time(),
        "responder_id": subscriber_client_id_global
    }
    response_payload = json.dumps(response_data)
    
    resp_props = mqtt_props.Properties(mqtt_packettypes.PacketTypes.PUBLISH)
    resp_props.CorrelationData = correlation_id.encode('utf-8')
    resp_props.UserProperty = [("response_type", "device_status_reply")]

    result = client.publish(response_topic, payload=response_payload, qos=1, properties=resp_props)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"   📤 RESPONSE (device_status) berhasil dikirim ke '{response_topic}'")
    else:
        print(f"   ❌ Gagal mengirim RESPONSE (device_status): {result.rc}")


def handle_ping_from_publisher(client, ping_payload, original_ping_topic):
    """Merespon PING dari publisher dengan PONG."""
    # Kirim PONG ke topik yang didengarkan publisher untuk PONG
    pong_topic_target = "pong/sub_to_pub" # <<< GANTI NAMA TOPIK
    pong_payload = "PONG_FROM_SUBSCRIBER"

    result = client.publish(pong_topic_target, payload=pong_payload, qos=1)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"   📤 PONG (balasan PING publisher) dikirim ke '{pong_topic_target}'")
    else:
        print(f"   ❌ Gagal mengirim PONG: {result.rc}")

def send_request_from_subscriber(client, request_payload_dict, request_topic, qos=1, timeout=10):
    """Mengirim request dari subscriber dan menunggu response."""
    response_topic = f"client/{subscriber_client_id_global}/response"
    correlation_id = str(uuid.uuid4())

    subscriber_pending_responses[correlation_id] = None
    subscriber_response_events[correlation_id] = threading.Event()

    req_props = mqtt_props.Properties(mqtt_packettypes.PacketTypes.PUBLISH)
    req_props.ResponseTopic = response_topic
    req_props.CorrelationData = correlation_id.encode('utf-8')
    # req_props.MessageExpiryInterval = 30

    request_payload_str = json.dumps(request_payload_dict)
    result = client.publish(request_topic, payload=request_payload_str, qos=qos, properties=req_props)

    print(f"\n🚀 REQUEST dikirim ke '{request_topic}' dari subscriber.")
    print(f"   Correlation ID: {correlation_id}")
    print(f"   Response diharapkan di: {response_topic}")

    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        if subscriber_response_events[correlation_id].wait(timeout=timeout):
            response = subscriber_pending_responses.get(correlation_id)
            print(f"   📬 RESPONSE diterima untuk request subscriber: {response}")
        else:
            print(f"   ⌛️ TIMEOUT menunggu response untuk Correlation ID: {correlation_id}")
    else:
        print(f"   ❌ Gagal mengirim request (RC: {result.rc})")
    
    # Cleanup
    if correlation_id in subscriber_pending_responses: del subscriber_pending_responses[correlation_id]
    if correlation_id in subscriber_response_events: del subscriber_response_events[correlation_id]


def on_log_subscriber(client, userdata, level, buf):
    print(f"SUBSCRIBER LOG: {buf}")


def run_subscriber():
    global subscriber_client_global, running, subscriber_client_id_global
    
    subscriber_client_global = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=subscriber_client_id_global,
        protocol=mqtt.MQTTv5
    )

    subscriber_client_global.username_pw_set(USERNAME, PASSWORD)
    subscriber_client_global.on_connect = on_connect_subscriber
    subscriber_client_global.on_subscribe = on_subscribe_subscriber # DAFTARKAN CALLBACK BARU
    subscriber_client_global.on_disconnect = on_disconnect_subscriber
    subscriber_client_global.on_message = on_message_subscriber
    subscriber_client_global.on_log = on_log_subscriber
    # ... sisa fungsi run_subscriber tetap sama ...

    try:
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        subscriber_client_global.tls_set_context(context)
        print("🔒 TLS setup berhasil untuk HiveMQ Cloud")
    except Exception as e:
        print(f"💥 Subscriber Error TLS setup: {e}")
        return

    try:
        print(f"🔗 Menghubungkan Subscriber ke {BROKER_ADDRESS}:{PORT_MQTTS}...")
        subscriber_client_global.connect(BROKER_ADDRESS, PORT_MQTTS, keepalive=60)
    except Exception as e:
        print(f"💥 Subscriber Gagal terhubung ke broker: {e}")
        return

    subscriber_client_global.loop_start() # Jalankan loop di background thread

    print("⏳ Menunggu koneksi subscriber stabil...")
    time.sleep(3) # Beri waktu untuk callback on_connect dan subscribe selesai

    if not subscriber_client_global.is_connected() and running:
        print("❌ Subscriber tidak dapat terhubung. Keluar.")
        subscriber_client_global.loop_stop()
        return
    elif not running: # Jika ada sinyal shutdown saat menunggu
         print("Proses dihentikan saat menunggu koneksi.")
         if subscriber_client_global: subscriber_client_global.loop_stop()
         return

    print("\n" + "="*40)
    print("🚀 SUBSCRIBER SIAP MENERIMA SEMUA PESAN 🚀")
    print("="*40 + "\n")

    # Contoh: Subscriber mengirim request ke publisher setelah beberapa saat
    # Ini akan dijalankan dalam thread agar tidak memblokir loop utama check_message_queue
    # def delayed_subscriber_actions():
    #     time.sleep(10) 
    #     if running and subscriber_client_global.is_connected():
    #         print("\n=== Subscriber akan mengirimkan request ke Publisher ===")
    #         req_payload = {"component": "all", "requester_id": subscriber_client_id_global}
    #         send_request_from_subscriber(subscriber_client_global, req_payload, "actions/system/get_info")

    #         time.sleep(5) # Jeda sebelum PING
    #         print("\n=== Subscriber akan mengirimkan PING ke Publisher ===")
    #         ping_payload_sub = "PING_FROM_SUBSCRIBER"
    #         # Kirim PING ke topik yang didengarkan Publisher untuk PING dari Subscriber
    #         subscriber_client_global.publish("ping/sub_to_pub", payload=ping_payload_sub, qos=1) # <<< GANTI NAMA TOPIK
    #         print(f"   PING dari subscriber dikirim: {ping_payload_sub} ke 'ping/sub_to_pub'. Menunggu PONG...")
    #         # PONG akan diterima di on_message -> process_message_thread

    # action_thread = threading.Thread(target=delayed_subscriber_actions)
    # action_thread.daemon = True
    # action_thread.start()

    try:
        while running:
            check_message_queue(subscriber_client_global) # Proses pesan dari antrian jika ada
            time.sleep(0.1) # Loop utama tidak perlu terlalu cepat
            
    except KeyboardInterrupt: # Seharusnya sudah ditangani oleh signal_handler
        print("\nℹ️ Subscriber dihentikan oleh pengguna (KeyboardInterrupt).")
    finally:
        running = False # Pastikan flag running diset false
        print("\n🔌 Membersihkan subscriber...")
        # Signal handler sudah mencoba disconnect, ini sebagai fallback
        if subscriber_client_global and subscriber_client_global.is_connected():
            subscriber_client_global.disconnect()
        if subscriber_client_global: # Pastikan loop dihentikan meskipun tidak connected
            subscriber_client_global.loop_stop()
        print("✅ Subscriber berhenti.")

if __name__ == "__main__":
    run_subscriber()
