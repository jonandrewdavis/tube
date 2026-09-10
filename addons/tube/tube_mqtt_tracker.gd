class_name TubeMqttTracker extends TubeTracker

const MqttClient := preload("./mqtt/mqtt.gd")
const MAX_PACKET_SIZE := 262144
const PING_TIMEOUT := 10.0

var _mqtt := MqttClient.new()
var _info_hash: String
var _peer_id_hash: String
var _url := ""
var _subscription_id := 0
var _ping_pending := false
var _ping_elapsed := 0.0
var _close_elapsed := 0.0
var _send_error: Error = OK


func _init(info_hash: String, peer_id_hash: String) -> void:
	_info_hash = info_hash
	_peer_id_hash = peer_id_hash
	# Trackers are manually polled RefCounteds, so this Node stays outside the tree.
	_mqtt.verbose_level = 0
	_mqtt.binarymessages = true
	_mqtt.max_packet_size = MAX_PACKET_SIZE
	_mqtt.client_id = "tube" + Crypto.new().generate_random_bytes(8).hex_encode()
	_mqtt._ready()
	_mqtt.broker_connected.connect(_on_broker_connected)
	_mqtt.subscription_acknowledged.connect(_on_subscribed)
	_mqtt.broker_connection_failed.connect(_on_failed.bind("MQTT broker connection failed"))
	_mqtt.broker_disconnected.connect(_on_failed.bind("MQTT broker connection closed"))
	_mqtt.received_message_details.connect(_on_message)
	_mqtt.ping_sent.connect(_on_ping_sent)
	_mqtt.ping_received.connect(_on_ping_received)
	_mqtt.send_failed.connect(_on_send_failed)


func _notification(what: int) -> void:
	if what == NOTIFICATION_PREDELETE and is_instance_valid(_mqtt):
		_mqtt.free()


func connect_to_url(p_url: String) -> Error:
	_url = p_url
	state = WebSocketPeer.STATE_CONNECTING
	if not (p_url.begins_with("ws://") or p_url.begins_with("wss://")):
		_on_failed("MQTT requires a ws:// or wss:// broker URL")
		return ERR_INVALID_PARAMETER
	if not _mqtt.connect_to_broker(p_url):
		_on_failed("MQTT connection failed")
		return ERR_CANT_CONNECT
	socket = _mqtt.websocket
	return OK


func is_open() -> bool:
	return state == WebSocketPeer.STATE_OPEN


func is_close() -> bool:
	return state == WebSocketPeer.STATE_CLOSED


func close(_p_info_hash: String, _p_peer_id_hash: String) -> void:
	if state >= WebSocketPeer.STATE_CLOSING:
		return
	# Defer socket cleanup until upstream has finished delivering this packet.
	state = WebSocketPeer.STATE_CLOSING
	_close_elapsed = 0.0
	state_changed.emit()


func send_announce(_p_info_hash: String, _p_peer_id_hash: String) -> Error:
	return OK


func send_stop(_p_info_hash: String, _p_peer_id_hash: String) -> Error:
	return OK


func send_data(data: Dictionary) -> Error:
	if not is_open():
		return ERR_UNCONFIGURED
	if not data.get("to_peer_id") is String:
		return ERR_INVALID_DATA
	var topic := _topic(data.to_peer_id)
	var payload := JSON.stringify(data).to_utf8_buffer()
	if 2 + topic.to_utf8_buffer().size() + payload.size() > MAX_PACKET_SIZE:
		return ERR_OUT_OF_MEMORY
	_send_error = OK
	_mqtt.publish(topic, payload, false, 0)
	var error := _send_error
	if error:
		raise_warning("Cannot send MQTT signaling: " + error_string(error))
	else:
		data_sent.emit(data)
	return error


func _topic(id: String) -> String:
	return "tube/v1/%s/peer/%s" % [_info_hash.to_utf8_buffer().hex_encode(), id]


func _on_broker_connected() -> void:
	if state != WebSocketPeer.STATE_CONNECTING:
		return
	if socket.get_selected_protocol() != "mqtt":
		_on_failed("Broker did not select the mqtt WebSocket protocol")
		return
	_subscription_id = _mqtt.subscribe(_topic(_peer_id_hash), 0)


func _on_subscribed(id: int, result: int) -> void:
	if state != WebSocketPeer.STATE_CONNECTING:
		return
	if id != _subscription_id or result != 0:
		_on_failed("MQTT broker rejected subscription")
		return
	state = WebSocketPeer.STATE_OPEN
	state_changed.emit()
	connected.emit()


func _on_failed(message: String) -> void:
	if state >= WebSocketPeer.STATE_CLOSING:
		return
	error_message = message
	close("", "")
	failed.emit()


func _on_send_failed(error: int) -> void:
	_send_error = error


func _on_ping_sent() -> void:
	if not _ping_pending:
		_ping_pending = true
		_ping_elapsed = 0.0


func _on_ping_received() -> void:
	_ping_pending = false
	_ping_elapsed = 0.0


func _on_message(topic: String, payload: PackedByteArray, retained: bool) -> void:
	if state >= WebSocketPeer.STATE_CLOSING or retained or topic != _topic(_peer_id_hash):
		return
	var data = JSON.parse_string(payload.get_string_from_utf8())
	if not _is_valid_answer(data):
		raise_warning("Received invalid MQTT signaling")
		return
	received_data.emit(data)
	received_answer.emit(data)


func _is_valid_answer(data: Variant) -> bool:
	if not data is Dictionary:
		return false
	if data.get("info_hash") != _info_hash or data.get("to_peer_id") != _peer_id_hash:
		return false
	var sender = data.get("peer_id")
	if not sender is String or not sender.is_valid_int():
		return false
	var id := int(sender)
	if id < 1 or id > 2147483647 or sender != str(id).pad_zeros(20) or sender == _peer_id_hash:
		return false
	var is_host := int(_peer_id_hash) == 1
	if (is_host and id == 1) or (not is_host and id != 1):
		return false
	var answer = data.get("answer")
	if not answer is Dictionary:
		return false
	if answer.get("type") != ("offer" if is_host else "answer"):
		return false
	if not answer.get("sdp") is String or answer.sdp.is_empty():
		return false
	if not answer.get("ice_candidates") is Array:
		return false
	for candidate in answer.ice_candidates:
		if not TubeTracker.is_ice_candidate_data_valid(candidate):
			return false
		if candidate.index < 0 or candidate.index > 65535 or candidate.index != int(candidate.index):
			return false
	return true


func _process(delta: float) -> void:
	if is_close():
		return
	if state == WebSocketPeer.STATE_CLOSING:
		if _mqtt.brokerconnectmode != MqttClient.BCM_NOCONNECTION:
			_mqtt.disconnect_from_server()
		socket.poll()
		_close_elapsed += delta
		if socket.get_ready_state() == WebSocketPeer.STATE_CLOSED or _close_elapsed >= 1.0:
			state = WebSocketPeer.STATE_CLOSED
			state_changed.emit()
			disconnected.emit()
		return
	_mqtt._process(delta)
	if state == WebSocketPeer.STATE_CONNECTING:
		connecting_time += delta
		if connecting_time >= connect_timeout:
			_on_failed("MQTT connection or subscription timed out")
	elif is_open():
		up_time += delta
	if _ping_pending and state < WebSocketPeer.STATE_CLOSING:
		_ping_elapsed += delta
		if _ping_elapsed >= PING_TIMEOUT:
			_on_failed("MQTT broker did not answer ping")
