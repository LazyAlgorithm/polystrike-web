// Tessera's multiplayer in a browser: WebRTC data channels between the host's browser and each player's, so no
// server of yours carries the game and nobody forwards a port.
//
// Finding each other: a browser cannot accept connections, so the two sides swap connection details through public
// WebTorrent trackers, the way browser BitTorrent clients do. A room code names a "swarm" on those trackers; players
// put WebRTC offers into it, the host answers them, and from then on the trackers are not involved. Several trackers
// are used at once, so one being down does not matter.
//
// Reaching each other: STUN servers tell each browser its public address, which is enough on most home and phone
// networks. Strict networks (some offices, campuses and mobile carriers) also need a TURN relay; add one to
// TesseraNet.iceServers (for example from Cloudflare's TURN service) to reach those players too.
//
// The engine side is native/tessera_web.c (tn_rtc_*) and Engine/src/Net/WebRtcTransport.bf.
(() => {
	const TRACKERS = [
		"wss://tracker.webtorrent.dev",
		"wss://tracker.openwebtorrent.com",
		"wss://tracker.btorrent.xyz",
		"wss://tracker.files.fm:7073/announce",
	];
	const CODE_LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no 0/O, 1/I: codes are read aloud and typed on phones
	const CODE_LENGTH = 6;
	const OFFERS_PER_ANNOUNCE = 4;
	const CLIENT_ANNOUNCE_MS = 4000;
	const HOST_ANNOUNCE_MS = 20000;
	const JOIN_TIMEOUT_MS = 30000;
	const ICE_GATHER_MS = 2500;
	const CHUNK = 16 * 1024; // the size every browser's data channels accept in one message
	const encoder = new TextEncoder();

	const random = (length, letters) => {
		const bytes = crypto.getRandomValues(new Uint8Array(length));
		let text = "";
		for (const b of bytes) text += letters[b % letters.length];
		return text;
	};
	const sha1Binary = async (text) => {
		const digest = new Uint8Array(await crypto.subtle.digest("SHA-1", encoder.encode(text)));
		return String.fromCharCode(...digest);
	};
	// Waits until the browser has found its addresses (or a little while has passed), so one offer or answer holds
	// everything the other side needs and nothing has to be sent through the trackers afterwards.
	const gathered = (pc) => new Promise((resolve) => {
		if (pc.iceGatheringState === "complete") return resolve();
		const done = () => { clearTimeout(timer); pc.removeEventListener("icegatheringstatechange", check); resolve(); };
		const check = () => { if (pc.iceGatheringState === "complete") done(); };
		const timer = setTimeout(done, ICE_GATHER_MS);
		pc.addEventListener("icegatheringstatechange", check);
	});

	const net = {
		iceServers: [
			{ urls: ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"] },
			{ urls: "stun:stun.cloudflare.com:3478" },
		],
		trackers: TRACKERS.slice(),
		role: null,        // "host" or "client"
		code: "",
		status: "",        // what is happening, for the game to show
		events: [],        // [kind, peer, Uint8Array] for the engine: 0 connected, 1 disconnected, 2 data
		peers: new Map(),  // engine peer id -> peer
		nextPeer: 1,
	};
	let infoHash = "", peerId = "", sockets = [], timers = [], pending = new Map(), joinTimer = 0;

	net.makeCode = () => random(CODE_LENGTH, CODE_LETTERS);
	// What a player types: case, spaces and dashes do not matter.
	net.cleanCode = (text) => String(text).toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 12);

	function emit(kind, peer, data) { net.events.push([kind, peer, data || null]); }

	function makePeer(id, pc) {
		const peer = { id, pc, reliable: null, unreliable: null, open: false, closed: false, parts: [], remoteId: "" };
		peer.reliable = pc.createDataChannel("r", { negotiated: true, id: 0, ordered: true });
		peer.unreliable = pc.createDataChannel("u", { negotiated: true, id: 1, ordered: false, maxRetransmits: 0 });
		for (const channel of [peer.reliable, peer.unreliable]) {
			channel.binaryType = "arraybuffer";
			channel.onopen = () => opened(peer);
			channel.onclose = () => closed(peer);
			channel.onmessage = (event) => received(peer, new Uint8Array(event.data));
		}
		pc.onconnectionstatechange = () => {
			if (pc.connectionState === "failed" || pc.connectionState === "closed") closed(peer);
		};
		return peer;
	}

	function opened(peer) {
		if (peer.open || peer.closed) return;
		if (peer.reliable.readyState !== "open" || peer.unreliable.readyState !== "open") return;
		peer.open = true;
		net.peers.set(peer.id, peer);
		if (net.role === "client") {
			clearTimeout(joinTimer);
			net.status = "Connected";
			stopSignaling(); // joined: the trackers are not needed any more
			for (const other of pending.values()) if (other !== peer) other.pc.close();
			pending.clear();
		}
		emit(0, peer.id);
	}

	function closed(peer) {
		if (peer.closed) return;
		peer.closed = true;
		try { peer.pc.close(); } catch (e) {}
		if (peer.open) {
			net.peers.delete(peer.id);
			emit(1, peer.id);
		}
	}

	// Messages: one header byte, 0 for a whole message, 1 for a piece with more to come, 2 for the last piece.
	function received(peer, bytes) {
		if (!bytes.length) return;
		const kind = bytes[0];
		if (kind === 0) { emit(2, peer.id, bytes.subarray(1)); return; }
		peer.parts.push(bytes.subarray(1));
		if (kind === 2) {
			let total = 0;
			for (const part of peer.parts) total += part.length;
			const whole = new Uint8Array(total);
			let at = 0;
			for (const part of peer.parts) { whole.set(part, at); at += part.length; }
			peer.parts = [];
			emit(2, peer.id, whole);
		}
	}

	net.send = (id, bytes, reliable) => {
		const peer = net.peers.get(id);
		if (!peer || !peer.open) return;
		// Anything too big for one message goes reliably, in pieces, so it arrives whole.
		const channel = reliable || bytes.length + 1 > CHUNK ? peer.reliable : peer.unreliable;
		if (channel.readyState !== "open") return;
		if (!reliable && channel.bufferedAmount > 256 * 1024) return; // a stale snapshot is worth less than none
		try {
			if (bytes.length + 1 <= CHUNK) {
				const message = new Uint8Array(bytes.length + 1);
				message.set(bytes, 1);
				channel.send(message);
				return;
			}
			for (let at = 0; at < bytes.length; at += CHUNK - 1) {
				const piece = bytes.subarray(at, Math.min(at + CHUNK - 1, bytes.length));
				const message = new Uint8Array(piece.length + 1);
				message[0] = at + piece.length >= bytes.length ? 2 : 1;
				message.set(piece, 1);
				channel.send(message);
			}
		} catch (e) {
			closed(peer);
		}
	};

	net.disconnect = (id) => {
		const peer = net.peers.get(id);
		if (!peer) return;
		peer.open = false; // the engine asked: no event back
		net.peers.delete(id);
		closed(peer);
	};

	function stopSignaling() {
		for (const t of timers) clearInterval(t);
		timers = [];
		for (const socket of sockets) { try { socket.close(); } catch (e) {} }
		sockets = [];
	}

	net.close = () => {
		stopSignaling();
		clearTimeout(joinTimer);
		for (const peer of net.peers.values()) { peer.open = false; closed(peer); }
		for (const peer of pending.values()) closed(peer);
		net.peers.clear();
		pending.clear();
		net.events = [];
		net.role = null;
		net.code = "";
		net.status = "";
	};

	function announce(socket, extra) {
		if (socket.readyState !== WebSocket.OPEN) return;
		socket.send(JSON.stringify(Object.assign({
			action: "announce", info_hash: infoHash, peer_id: peerId,
			numwant: net.role === "client" ? OFFERS_PER_ANNOUNCE : 0,
			uploaded: 0, downloaded: 0, left: 1,
		}, extra)));
	}

	// A player: fresh offers into the swarm every few seconds until the host answers one.
	async function clientAnnounce(socket) {
		if (net.role !== "client" || net.peers.size) return;
		const offers = [];
		for (let i = 0; i < OFFERS_PER_ANNOUNCE; i++) {
			const pc = new RTCPeerConnection({ iceServers: net.iceServers });
			const offerId = random(20, "abcdefghijklmnopqrstuvwxyz0123456789");
			const peer = makePeer(1, pc);
			peer.created = Date.now();
			pending.set(offerId, peer);
			await pc.setLocalDescription(await pc.createOffer());
			await gathered(pc);
			offers.push({ offer_id: offerId, offer: { type: "offer", sdp: pc.localDescription.sdp } });
		}
		// Offers nobody answered in time are dropped.
		for (const [offerId, peer] of pending) {
			if (!peer.open && Date.now() - peer.created > 3 * CLIENT_ANNOUNCE_MS + ICE_GATHER_MS * OFFERS_PER_ANNOUNCE) {
				pending.delete(offerId);
				closed(peer);
			}
		}
		if (net.role === "client" && !net.peers.size) announce(socket, { offers, event: "started" });
	}

	async function onTrackerMessage(socket, message) {
		let data;
		try { data = JSON.parse(message.data); } catch (e) { return; }
		if (data.info_hash !== infoHash || data.peer_id === peerId) return;
		if (data["failure reason"]) return;
		if (net.role === "host" && data.offer && data.offer_id && typeof data.peer_id === "string" && data.peer_id[0] === "C") {
			// A player found us. One connection per player: a newer offer from them replaces one still being set up.
			for (const peer of pending.values()) if (peer.remoteId === data.peer_id && !peer.open) closed(peer);
			for (const peer of net.peers.values()) if (peer.remoteId === data.peer_id) return;
			const pc = new RTCPeerConnection({ iceServers: net.iceServers });
			const peer = makePeer(net.nextPeer++, pc);
			peer.remoteId = data.peer_id;
			pending.set(data.offer_id, peer);
			try {
				await pc.setRemoteDescription(data.offer);
				await pc.setLocalDescription(await pc.createAnswer());
				await gathered(pc);
				announce(socket, { to_peer_id: data.peer_id, offer_id: data.offer_id, answer: { type: "answer", sdp: pc.localDescription.sdp } });
			} catch (e) {
				closed(peer);
			}
			setTimeout(() => pending.delete(data.offer_id), JOIN_TIMEOUT_MS);
		}
		else if (net.role === "client" && data.answer && data.offer_id && typeof data.peer_id === "string" && data.peer_id[0] === "H") {
			const peer = pending.get(data.offer_id);
			if (!peer || peer.pc.signalingState !== "have-local-offer") return;
			peer.remoteId = data.peer_id;
			try { await peer.pc.setRemoteDescription(data.answer); } catch (e) { closed(peer); }
		}
	}

	async function startSignaling(app) {
		infoHash = await sha1Binary("tessera:" + app + ":" + net.code);
		peerId = (net.role === "host" ? "H" : "C") + random(19, "abcdefghijklmnopqrstuvwxyz0123456789");
		let reached = 0, failed = 0;
		for (const url of net.trackers) {
			let socket;
			try { socket = new WebSocket(url); } catch (e) { failed++; continue; }
			sockets.push(socket);
			socket.onmessage = (message) => onTrackerMessage(socket, message);
			socket.onerror = () => {
				if (++failed === net.trackers.length && !reached) net.status = "Could not reach the internet to find other players";
			};
			socket.onopen = () => {
				reached++;
				if (net.role === "host") {
					net.status = "Waiting for players";
					announce(socket, { offers: [], event: "started" });
					timers.push(setInterval(() => announce(socket, { offers: [] }), HOST_ANNOUNCE_MS));
				} else {
					net.status = "Looking for the host";
					clientAnnounce(socket);
					timers.push(setInterval(() => clientAnnounce(socket), CLIENT_ANNOUNCE_MS));
				}
			};
		}
	}

	// app: names the game, so two games' rooms with the same code never meet.
	net.host = (app, code) => {
		net.close();
		net.role = "host";
		net.code = net.cleanCode(code || "") || net.makeCode();
		net.status = "Opening the room";
		startSignaling(app);
		return net.code;
	};

	net.connect = (app, code) => {
		net.close();
		net.role = "client";
		net.code = net.cleanCode(code);
		if (!net.code) return false;
		net.status = "Looking for the host";
		startSignaling(app);
		joinTimer = setTimeout(() => {
			if (net.peers.size) return;
			net.status = "Nobody is hosting " + net.code + ". Check the code, or ask the host for a new link.";
			stopSignaling();
			for (const peer of pending.values()) closed(peer);
			pending.clear();
			emit(1, 1); // the engine hears that the server went away
		}, JOIN_TIMEOUT_MS);
		return true;
	};

	addEventListener("pagehide", () => net.close());
	window.TesseraNet = net;
})();
