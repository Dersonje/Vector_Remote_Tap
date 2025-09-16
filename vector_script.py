import json
import threading
from datetime import datetime
import websocket  # websocket-client library
from collections import deque, defaultdict
import argparse

QUERY = """
query GetAllConfigInfo {
  sources {
    nodes {
      componentId
      componentType
      outputTypes
      outputs {
        outputId
        sentEventsTotal {
          timestamp
          sentEventsTotal
        }
      }
      transforms {
        componentId
        componentType
      }
      sinks {
        componentId
        componentType
      }
      metrics {
        receivedBytesTotal {
          timestamp
          receivedBytesTotal
        }
        receivedEventsTotal {
          timestamp
          receivedEventsTotal
        }
        sentEventsTotal {
          timestamp
          sentEventsTotal
        }
        ... on FileSourceMetrics {
          files {
            nodes {
              name
              receivedBytesTotal {
                timestamp
                receivedBytesTotal
              }
              receivedEventsTotal {
                timestamp
                receivedEventsTotal
              }
              sentEventsTotal {
                timestamp
                sentEventsTotal
              }
            }
            pageInfo {
              hasNextPage
              hasPreviousPage
              startCursor
              endCursor
            }
            totalCount
          }
        }
        ... on GenericSourceMetrics {
          receivedBytesTotal {
            timestamp
            receivedBytesTotal
          }
          receivedEventsTotal {
            timestamp
            receivedEventsTotal
          }
          sentEventsTotal {
            timestamp
            sentEventsTotal
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
    totalCount
  }
  transforms {
    nodes {
      componentId
      componentType
      outputs {
        outputId
        sentEventsTotal {
          timestamp
          sentEventsTotal
        }
      }
      sources {
        componentId
        componentType
      }
      transforms {
        componentId
        componentType
      }
      sinks {
        componentId
        componentType
      }
      metrics {
        receivedEventsTotal {
          timestamp
          receivedEventsTotal
        }
        sentEventsTotal {
          timestamp
          sentEventsTotal
        }
        ... on GenericTransformMetrics {
          receivedEventsTotal {
            timestamp
            receivedEventsTotal
          }
          sentEventsTotal {
            timestamp
            sentEventsTotal
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
    totalCount
  }
  sinks {
    nodes {
      componentId
      componentType
      sources {
        componentId
        componentType
      }
      transforms {
        componentId
        componentType
      }
      metrics {
        receivedEventsTotal {
          timestamp
          receivedEventsTotal
        }
        sentBytesTotal {
          timestamp
          sentBytesTotal
        }
        sentEventsTotal {
          timestamp
          sentEventsTotal
        }
        ... on GenericSinkMetrics {
          receivedEventsTotal {
            timestamp
            receivedEventsTotal
          }
          sentBytesTotal {
            timestamp
            sentBytesTotal
          }
          sentEventsTotal {
            timestamp
            sentEventsTotal
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
    totalCount
  }
  hostMetrics {
    memory {
      totalBytes
      freeBytes
      availableBytes
      activeBytes
      buffersBytes
      cachedBytes
      sharedBytes
      usedBytes
      inactiveBytes
      wiredBytes
    }
    swap {
      freeBytes
      totalBytes
      usedBytes
      swappedInBytesTotal
      swappedOutBytesTotal
    }
    cpu {
      cpuSecondsTotal
    }
    loadAverage {
      load1
      load5
      load15
    }
    network {
      receiveBytesTotal
      receiveErrsTotal
      receivePacketsTotal
      transmitBytesTotal
      transmitErrsTotal
      transmitPacketsDropTotal
      transmitPacketsTotal
    }
    filesystem {
      freeBytes
      totalBytes
      usedBytes
    }
    disk {
      readBytesTotal
      readsCompletedTotal
      writtenBytesTotal
      writesCompletedTotal
    }
    tcp {
      tcpConnsTotal
      tcpTxQueuedBytesTotal
      tcpRxQueuedBytesTotal
    }
  }
  meta {
    versionString
    hostname
  }
}
"""

class VectorClient:
    def __init__(self, ws_url):
        self.ws_url = ws_url

    def execute_query(self, query, variables=None):
        self.result = None
        self.error = None
        self.event = threading.Event()

        def on_message(ws, message):
            try:
                data = json.loads(message)
                msg_type = data.get('type')
                if msg_type == 'connection_ack':
                    return
                elif msg_type == 'data':
                    payload = data.get('payload', {})
                    if 'errors' in payload:
                        self.error = payload['errors']
                    else:
                        self.result = payload.get('data')
                elif msg_type == 'complete':
                    self.event.set()
                elif msg_type == 'ka':
                    pass
                else:
                    print(f"Unhandled message type: {msg_type}")
            except json.JSONDecodeError:
                self.error = f"Invalid JSON message: {message}"
            except Exception as e:
                self.error = f"Error processing message: {e}"

        def on_error(ws, error):
            self.error = f"WebSocket error: {error}"
            self.event.set()

        def on_close(ws, close_status_code, close_msg):
            pass  # Event will be set on complete or error

        def on_open(ws):
            init_payload = json.dumps({
                "type": "connection_init",
                "payload": {}
            })
            ws.send(init_payload)

            def send_query():
                sub_payload = json.dumps({
                    "id": "1",
                    "type": "start",
                    "payload": {
                        "query": query,
                        "variables": variables or {}
                    }
                })
                ws.send(sub_payload)

            threading.Timer(0.5, send_query).start()

        ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
            subprotocols=["graphql-ws"]
        )

        def run_ws():
            ws_app.run_forever()

        thread = threading.Thread(target=run_ws)
        thread.start()
        self.event.wait()
        ws_app.close()
        thread.join()

        if self.error:
            raise Exception(self.error)
        return self.result


class VectorEventSubscriber:
    def __init__(self, ws_url, patterns, limit):
        self.ws_url = ws_url
        self.patterns = patterns
        self.limit = limit
        self.event_count = 0
        self.lock = threading.Lock()
        self.subscription_id = None

    def on_message(self, ws, message):
        """Callback for incoming WebSocket messages."""
        try:
            data = json.loads(message)
            # print(f"Received raw message: {json.dumps(data, indent=2)}")  # Uncomment for debug

            msg_type = data.get('type')
            if msg_type == 'connection_ack':
                print("Connection acknowledged by server.")
                return
            elif msg_type == 'data':
                payload = data.get('payload', {})
                if 'errors' in payload:
                    print(f"GraphQL Error: {payload['errors']}")
                    return
                if 'data' in payload and 'outputEventsByComponentIdPatterns' in payload['data']:
                    events = payload['data']['outputEventsByComponentIdPatterns']
                    if isinstance(events, list):
                        for event in events:
                            print("{ \"event\":", json.dumps(event, indent=2), "}")  # Pretty-print the event
                            with self.lock:
                                self.event_count += 1
                                if self.event_count >= self.limit:
                                    print(f"\nReached limit of {self.limit} events. Closing connection.")
                                    self.unsubscribe(ws)
                                    ws.close()
                                    return
                    else:
                        print(f"Unexpected events format: {events}")
            elif msg_type == 'ka':
                pass
            elif msg_type == 'complete':
                print("Subscription complete.")
            else:
                print(f"Unhandled message type: {msg_type}")
        except json.JSONDecodeError:
            print(f"Invalid JSON message: {message}")
        except Exception as e:
            print(f"Error processing message: {e}")

    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"WebSocket connection closed (code: {close_status_code}, msg: {close_msg}).")

    def on_open(self, ws):
        init_payload = json.dumps({
            "type": "connection_init",
            "payload": {}
        })
        ws.send(init_payload)
        print(f"Sent connection_init at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")

        def send_subscription():
            subscription_query = """
            subscription OutputEventsByComponentIdPatterns($outputsPatterns: [String!]!) {
  outputEventsByComponentIdPatterns(outputsPatterns: $outputsPatterns) {
    ... on Log {
      componentId
      componentType
      componentKind
      message
      timestamp
      string(encoding: JSON)
      json(field: "message")
    }
    ... on Metric {
      componentId
      componentType
      componentKind
      timestamp
      name
      namespace
      kind
      valueType
      value
      tags {
        key
        value
      }
      string(encoding: JSON)
    }
    ... on EventNotification {
      message
    }
    ... on Trace {
      componentId
      componentType
      componentKind
      string(encoding: JSON)
      json(field: "trace")
    }
    __typename
  }
}
            """
            variables = {"outputsPatterns": self.patterns}
            sub_payload = json.dumps({
                "id": "1",
                "type": "start",
                "payload": {
                    "query": subscription_query,
                    "variables": variables
                }
            })
            ws.send(sub_payload)
            self.subscription_id = "1"
            print(f"Subscription sent for components matching patterns: {', '.join(self.patterns)}...")

        threading.Timer(0.5, send_subscription).start()

    def unsubscribe(self, ws):
        if self.subscription_id and ws:
            ws.send(json.dumps({
                "id": self.subscription_id,
                "type": "stop"
            }))

    def subscribe(self):
        ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
            subprotocols=["graphql-ws"]
        )
        ws_app.run_forever()

def get_connected(all_by_id, outgoing, start):
    reverse = defaultdict(list)
    for fr, tos in outgoing.items():
        for to in tos:
            reverse[to].append(fr)

    def bfs(start, graph):
        q = deque([start])
        vis = set()
        while q:
            cur = q.popleft()
            if cur in vis:
                continue
            vis.add(cur)
            for neigh in graph[cur]:
                q.append(neigh)
        return vis

    upstream = bfs(start, reverse)
    downstream = bfs(start, outgoing)
    connected = upstream.union(downstream)
    return connected

if __name__ == "__main__":
    # Configuration
    VECTOR_WS_URL = "ws://127.0.0.1:8686/graphql"

    parser = argparse.ArgumentParser(description="Vector Event Subscriber and Config Retriever")
    subparsers = parser.add_subparsers(dest='command', required=True)

    subscribe_parser = subparsers.add_parser('subscribe', help='Subscribe to events from components')
    subscribe_parser.add_argument('--patterns', nargs='+', default=["my_http_source", "replace_via", "my_console_sink"], help='Component patterns/IDs')
    subscribe_parser.add_argument('--limit', type=int, default=10, help='Event limit')

    info_parser = subparsers.add_parser('get-info', help='Get info for a specific component')
    info_parser.add_argument('name', help='Component name/ID')

    chain_parser = subparsers.add_parser('get-chain', help='Get chain info for a component and its connected inputs/outputs')
    chain_parser.add_argument('name', help='Component name/ID')

    args = parser.parse_args()

    if args.command == 'subscribe':
        subscriber = VectorEventSubscriber(VECTOR_WS_URL, args.patterns, args.limit)
        try:
            subscriber.subscribe()
        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            subscriber.unsubscribe(None)
        except Exception as e:
            print(f"Connection failed: {e}")
    else:
        try:
            client = VectorClient(VECTOR_WS_URL)
            data = client.execute_query(QUERY)

            sources_nodes = data['sources']['nodes']
            transforms_nodes = data['transforms']['nodes']
            sinks_nodes = data['sinks']['nodes']

            sources_by_id = {n['componentId']: n for n in sources_nodes}
            transforms_by_id = {n['componentId']: n for n in transforms_nodes}
            sinks_by_id = {n['componentId']: n for n in sinks_nodes}
            all_by_id = {**sources_by_id, **transforms_by_id, **sinks_by_id}

            # Build outgoing graph (upstream -> downstream)
            outgoing = defaultdict(list)
            for nodes in [sources_nodes, transforms_nodes]:
                for node in nodes:
                    for out_key in ['transforms', 'sinks']:
                        if out_key in node:
                            for out_comp in node[out_key]:
                                if out_comp['componentId'] not in outgoing[node['componentId']]:
                                    outgoing[node['componentId']].append(out_comp['componentId'])

            name = args.name
            if name not in all_by_id:
                print(f"Component '{name}' not found.")
                exit(1)

            # Build reverse for inputs
            reverse = defaultdict(list)
            for fr, tos in outgoing.items():
                for to in tos:
                    if fr not in reverse[to]:
                        reverse[to].append(fr)

            if args.command == 'get-info':
                info = all_by_id[name].copy()
                info['inputs'] = sorted(reverse.get(name, []))
                info['outputs'] = sorted(outgoing.get(name, []))
                if name in transforms_by_id:
                    input_transform_ids = [id_ for id_ in info['inputs'] if id_ in transforms_by_id]
                    input_transforms = [
                        {
                            "componentId": id_,
                            "componentType": all_by_id[id_]['componentType']
                        } for id_ in input_transform_ids
                    ]
                    output_transforms = info.get('transforms', [])
                    all_transforms = input_transforms + output_transforms
                    all_transforms = sorted(set(tuple(sorted(d.items())) for d in all_transforms))
                    all_transforms = [dict(t) for t in all_transforms]
                    info['transforms'] = all_transforms
                print(json.dumps(info, indent=2))
            else:  # get-chain
                connected_ids = get_connected(all_by_id, outgoing, name)
                chain_info = {}
                for id_ in sorted(connected_ids):
                    info = all_by_id[id_].copy()
                    info['inputs'] = sorted(reverse.get(id_, []))
                    info['outputs'] = sorted(outgoing.get(id_, []))
                    if id_ in transforms_by_id:
                        input_transform_ids = [id__ for id__ in info['inputs'] if id__ in transforms_by_id]
                        input_transforms = [
                            {
                                "componentId": id__,
                                "componentType": all_by_id[id__]['componentType']
                            } for id__ in input_transform_ids
                        ]
                        output_transforms = info.get('transforms', [])
                        all_transforms = input_transforms + output_transforms
                        all_transforms = sorted(set(tuple(sorted(d.items())) for d in all_transforms))
                        all_transforms = [dict(t) for t in all_transforms]
                        info['transforms'] = all_transforms
                    chain_info[id_] = info
                print(json.dumps(chain_info, indent=2))
        except Exception as e:
            print(f"Error: {e}")
