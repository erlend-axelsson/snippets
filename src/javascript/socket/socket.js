const __SECOND__ = 1000;
const __MINUTE__ = 60 * 1000;
const __DEFAULT_RETRY__ = {
    attempts: 0,
    maxAttempts: 5,
    baseDelay: 2 * __SECOND__,
    jitter: 2 * __SECOND__,
    maxDelay: __MINUTE__
};
const __SOCKET_NORMAL_CLOSURE__ = 1000;
const __SOCKET_GOING_AWAY__ = 1001;
const __SOCKET_PROTOCOL_ERROR__ = 1002;
const __SOCKET_UNSUPPORTED_DATA__ = 1003;
const __SOCKET_POLICY_VIOLATION__ = 1008;
const __NOT_TO_RETRY__ = [
    __SOCKET_NORMAL_CLOSURE__,
    __SOCKET_GOING_AWAY__,
    __SOCKET_PROTOCOL_ERROR__,
    __SOCKET_UNSUPPORTED_DATA__,
    __SOCKET_POLICY_VIOLATION__
];
export const newSocket = (uri, initialState) => {
    return new SocketWrapper(uri, initialState);
};

class SocketWrapper {
    uri;
    state;
    protocols = [];
    binaryType = "blob";
    errorEvent = false;
    buffer = [];
    retry = {...__DEFAULT_RETRY__};
    #ws = null;
    #handler = {
        onopen: () => {
        },
        onclose: () => {
        },
        onmessage: () => {
        },
        onerror: () => {
        }
    };

    constructor(uri, initialState) {
        this.uri = uri;
        this.state = initialState;
    }

    withProtocols(protocols) {
        this.protocols = protocols;
        return this;
    }

    withBinaryType(binaryType) {
        this.binaryType = binaryType;
        if (!!this.#ws) {
            this.#ws.binaryType = binaryType;
        }
        return this;
    }

    withBuffer(buffer) {
        this.buffer = buffer;
        return this;
    }

    withState(newState) {
        this.state = newState;
        return this;
    }

    withRetry(options) {
        this.retry = this.#retryOptions(options);
        return this;
    }

    onOpen(handler) {
        this.#handler.onopen = handler;
        return this;
    }

    onClose(handler) {
        this.#handler.onclose = handler;
        return this;
    }

    onMessage(handler) {
        this.#handler.onmessage = handler;
        return this;
    }

    onError(handler) {
        this.#handler.onerror = handler;
        return this;
    }

    get status() {
        switch (this.statusCode) {
            case WebSocket.CONNECTING:
                return "CONNECTING";
            case WebSocket.OPEN:
                return "OPEN";
            case WebSocket.CLOSING:
                return "CLOSING";
            case WebSocket.CLOSED:
                return "CLOSED";
        }
    }

    get statusCode() {
        return !!this.#ws ? this.#ws.readyState : WebSocket.CLOSED;
    }

    connect = () => {
        if (!!this.#ws && this.#ws.readyState <= WebSocket.OPEN) {
            return;
        }
        const newSocket = new WebSocket(this.uri, this.protocols);
        newSocket.binaryType = this.binaryType;
        newSocket.onopen = this.#onopen;
        newSocket.onclose = this.#onclose;
        newSocket.onmessage = this.#onmessage;
        newSocket.onerror = this.#onerror;
        this.#ws = newSocket;
    };
    disconnect = (code, reason) => {
        if (this.statusCode < WebSocket.CLOSING) {
            console.log(this.#ws);
            this.#ws?.close(code, reason);
        }
    };
    send = (message) => {
        if (!this.#ws || this.statusCode !== WebSocket.OPEN) {
            this.buffer.push(message);
            return;
        }
        this.#ws.send(message);
    };
    #onopen = (event) => {
        if (!!this.retry) {
            this.retry.attempts = 0;
        }
        this.errorEvent = false;
        this.#handler.onopen(event, this);
        this.#flush();
    };
    #onclose = (event) => {
        this.#handler.onclose(event, this);
        if (this.#shouldRetry(event.code)) {
            this.retry.attempts++;
            setTimeout(() => this.connect(), this.#getDelay());
        }
        this.#ws = null;
    };
    #onmessage = (event) => this.#handler.onmessage(event, this);
    #onerror = (event) => {
        this.errorEvent = true;
        this.#handler.onerror(event, this);
    };

    #flush() {
        while (this.buffer.length > 0 && this.#ws?.readyState === WebSocket.OPEN) {
            const message = this.buffer.at(0);
            if (!!message) {
                this.#ws.send(message);
                this.buffer.shift();
            }
        }
    }

    #shouldRetry(code) {
        if (!this.errorEvent) {
            return false;
        }
        if (__NOT_TO_RETRY__.includes(code)) {
            return false;
        }
        return this.retry.attempts < this.retry.maxAttempts;
    }

    #retryOptions(options) {
        if (options === true) {
            return {...__DEFAULT_RETRY__};
        } else if (options === false) {
            return {...__DEFAULT_RETRY__, maxAttempts: 0};
        }
        return {...__DEFAULT_RETRY__, ...options};
    }

    #getDelay() {
        const delay = this.retry.baseDelay * (2 ** this.retry.attempts);
        const jitter = Math.floor(Math.random() * this.retry.jitter);
        return Math.min(this.retry.maxDelay, delay + jitter);
    }
}
