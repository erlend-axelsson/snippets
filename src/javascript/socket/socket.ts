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

type ConnectFunction = () => void;
type DisconnectFunction = (code?: number, reason?: string) => void;
type SendFunction = (message: MessageData) => void;
type RetryOptions = {
  attempts: number;
  maxAttempts: number;
  maxDelay: number;
  baseDelay: number;
  jitter: number;
};
type WithRetry<T> = (options: boolean | Partial<RetryOptions>) => Socket<T>;
type WithProtocols<T> = (options: string | string[]) => Socket<T>;
type WithBinaryType<T> = (options: BinaryType) => Socket<T>;
type WithBuffer<T> = (options: MessageBuffer | any[]) => Socket<T>;
type WithState = <U>(state: U extends any ? U : never) => Socket<U>;
type Handlers<T> = {
  onopen: EventHandler<T>;
  onclose: CloseHandler<T>;
  onmessage: FrameHandler<T>;
  onerror: EventHandler<T>;
}

export type FrameHandler<T> = (event: MessageEvent<FrameData>, socket: Socket<T>) => any;
export type EventHandler<T> = (event: Event, socket: Socket<T>) => any;
export type CloseHandler<T> = (event: CloseEvent, socket: Socket<T>) => any;
export type FrameData = string | Blob | ArrayBuffer;
export type MessageData = string | Blob | BufferSource;
export type BinaryType = "blob" | "arraybuffer"
export type MessageBuffer = {
  push: (...messages: MessageData[]) => any;
  shift: () => MessageData | undefined;
  length: number;
  at: (index: number) => MessageData | undefined;
}

export interface Socket<T> {
  uri: string;
  state: T
  protocols: string | string[];
  retry: RetryOptions;
  buffer: MessageBuffer,
  binaryType: BinaryType;
  errorEvent: boolean
  readonly withRetry: WithRetry<T>;
  readonly withProtocols: WithProtocols<T>;
  readonly withBinaryType: WithBinaryType<T>;
  readonly withBuffer: WithBuffer<T>;
  readonly withState: WithState;
  readonly connect: ConnectFunction;
  readonly disconnect: DisconnectFunction;
  readonly send: SendFunction;
  readonly status: "CONNECTING" | "OPEN" | "CLOSING" | "CLOSED";
  readonly statusCode: 0 | 1 | 2 | 3;
  readonly onOpen: (handler: EventHandler<T>) => Socket<T>;
  readonly onClose: (handler: CloseHandler<T>) => Socket<T>;
  readonly onMessage: (handler: FrameHandler<T>) => Socket<T>;
  readonly onError: (handler: EventHandler<T>) => Socket<T>;
}

export const newSocket = <T>(uri: string, initialState?: T): Socket<typeof initialState> => {
  return new SocketWrapper(uri, initialState);
}

class SocketWrapper<T> implements Socket<T> {
  uri: string;
  state: T;
  protocols: string | string[] = [];
  binaryType: BinaryType = "blob"
  errorEvent: boolean = false;
  buffer: MessageBuffer = ([] as MessageData[]);
  retry: RetryOptions = {...__DEFAULT_RETRY__};
  #ws: WebSocket | null = null;
  #handler: Handlers<T> = {
    onopen: () => {
    },
    onclose: () => {
    },
    onmessage: () => {
    },
    onerror: () => {
    }
  }

  constructor(uri: string, initialState: T) {
    this.uri = uri;
    this.state = initialState;
  }

  withProtocols(protocols: string | string[]): Socket<T> {
    this.protocols = protocols;
    return this;
  }

  withBinaryType(binaryType: BinaryType): Socket<T> {
    this.binaryType = binaryType;
    if (!!this.#ws) {
      this.#ws.binaryType = binaryType;
    }
    return this;
  }

  withBuffer(buffer: MessageBuffer): Socket<T> {
    this.buffer = buffer;
    return this;
  }

  withState<U>(newState: U): Socket<U> {
    this.state = newState as any as T;
    return this as any as Socket<U>;
  }

  withRetry(options: Partial<RetryOptions> | boolean) {
    this.retry = this.#retryOptions(options);
    return this;
  }

  onOpen(handler: EventHandler<T>): Socket<T> {
    this.#handler.onopen = handler;
    return this;
  }

  onClose(handler: CloseHandler<T>): Socket<T> {
    this.#handler.onclose = handler;
    return this;
  }

  onMessage(handler: FrameHandler<T>): Socket<T> {
    this.#handler.onmessage = handler;
    return this;
  }

  onError(handler: EventHandler<T>): Socket<T> {
    this.#handler.onerror = handler;
    return this;
  }

  get status(): "CONNECTING" | "OPEN" | "CLOSING" | "CLOSED" {
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
  };

  get statusCode(): 0 | 1 | 2 | 3 {
    return !!this.#ws ? this.#ws.readyState : WebSocket.CLOSED;
  };

  readonly connect: ConnectFunction = () => {
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
  readonly disconnect: DisconnectFunction = (code?: number, reason?: string) => {
    if (this.statusCode < WebSocket.CLOSING) {
      console.log(this.#ws);
      this.#ws?.close(code, reason)
    }
  };
  readonly send: SendFunction = (message: MessageData) => {
    if (!this.#ws || this.statusCode !== WebSocket.OPEN) {
      this.buffer.push(message);

      return;
    }
    this.#ws.send(message);
  };

  #onopen = (event: Event) => {
    if (!!this.retry) {
      this.retry.attempts = 0;
    }
    this.errorEvent = false;
    this.#handler.onopen(event, this);
    this.#flush()
  }
  #onclose = (event: CloseEvent) => {
    this.#handler.onclose(event, this);
    if (this.#shouldRetry(event.code)) {
      this.retry.attempts++;
      setTimeout(() => this.connect(), this.#getDelay())
    }
    this.#ws = null;
  };
  #onmessage = (event: MessageEvent<FrameData>) =>
    this.#handler.onmessage(event, this);
  #onerror = (event: Event) => {
    this.errorEvent = true;
    this.#handler.onerror(event, this);
  }

  #flush() {
    while (this.buffer.length > 0 && this.#ws?.readyState === WebSocket.OPEN) {
      const message = this.buffer.at(0);
      if (!!message) {
        this.#ws.send(message);
        this.buffer.shift();
      }
    }
  }

  #shouldRetry(code: number): boolean {
    if (!this.errorEvent) {
      return false;
    }
    if (__NOT_TO_RETRY__.includes(code)) {
      return false;
    }
    return this.retry.attempts < this.retry.maxAttempts;
  }

  #retryOptions(options: Partial<RetryOptions> | boolean): RetryOptions {
    if (options === true) {
      return {...__DEFAULT_RETRY__};
    } else if (options === false) {
      return {...__DEFAULT_RETRY__, maxAttempts: 0}
    }
    return {...__DEFAULT_RETRY__, ...options}
  }

  #getDelay() {
    const delay = this.retry.baseDelay * (2 ** this.retry.attempts);
    const jitter = Math.floor(Math.random() * this.retry.jitter)
    return Math.min(this.retry.maxDelay, delay + jitter);
  }
}
