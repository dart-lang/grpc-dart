// Copyright (c) 2020, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import 'dart:async';

import 'package:grpc/grpc.dart';
import 'package:grpc/src/client/http2_connection.dart';
import 'package:http2/transport.dart';
import 'package:test/test.dart';

import '../src/client_utils.dart';
import '../src/utils.dart';

void main() {
  const dummyValue = 0;

  late ClientTransportConnectorHarness harness;

  setUp(() {
    harness = ClientTransportConnectorHarness()..setUp();
  });

  tearDown(() {
    harness.tearDown();
  });

  test('Unary calls work on the client', () async {
    const requestValue = 17;
    const responseValue = 19;

    void handleRequest(StreamMessage message) {
      final data = validateDataMessage(message);
      expect(mockDecode(data.data), requestValue);

      harness
        ..sendResponseHeader()
        ..sendResponseValue(responseValue)
        ..sendResponseTrailer();
    }

    await harness.runTest(
      clientCall: harness.client.unary(requestValue),
      expectedResult: responseValue,
      expectedPath: '/Test/Unary',
      serverHandlers: [handleRequest],
    );
  });

  test('Client-streaming calls work on the client', () async {
    const requests = [17, 3];
    const response = 12;

    var index = 0;

    void handleRequest(StreamMessage message) {
      final data = validateDataMessage(message);
      expect(mockDecode(data.data), requests[index++]);
    }

    void handleDone() {
      harness
        ..sendResponseHeader()
        ..sendResponseValue(response)
        ..sendResponseTrailer();
    }

    await harness.runTest(
      clientCall: harness.client.clientStreaming(Stream.fromIterable(requests)),
      expectedResult: response,
      expectedPath: '/Test/ClientStreaming',
      serverHandlers: [handleRequest, handleRequest],
      doneHandler: handleDone,
    );
  });

  test('Server-streaming calls work on the client', () async {
    const request = 4;
    const responses = [3, 17, 9];

    void handleRequest(StreamMessage message) {
      final data = validateDataMessage(message);
      expect(mockDecode(data.data), request);

      harness.sendResponseHeader();
      responses.forEach(harness.sendResponseValue);
      harness.sendResponseTrailer();
    }

    await harness.runTest(
      clientCall: harness.client.serverStreaming(request).toList(),
      expectedResult: responses,
      expectedPath: '/Test/ServerStreaming',
      serverHandlers: [handleRequest],
    );
  });

  test('Bidirectional calls work on the client', () async {
    const requests = [1, 15, 7];
    const responses = [3, 17, 9];

    var index = 0;

    void handleRequest(StreamMessage message) {
      final data = validateDataMessage(message);
      expect(mockDecode(data.data), requests[index]);

      if (index == 0) {
        harness.sendResponseHeader();
      }
      harness.sendResponseValue(responses[index]);
      index++;
    }

    void handleDone() {
      harness.sendResponseTrailer();
    }

    await harness.runTest(
      clientCall:
          harness.client.bidirectional(Stream.fromIterable(requests)).toList(),
      expectedResult: responses,
      expectedPath: '/Test/Bidirectional',
      serverHandlers: [handleRequest, handleRequest, handleRequest],
      doneHandler: handleDone,
    );
  });

  test('Unary call with no response throws error', () async {
    void handleRequest(_) {
      harness.sendResponseTrailer();
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException: GrpcError.unimplemented('No responses received'),
      serverHandlers: [handleRequest],
    );
  });

  test('Unary call with more than one response throws error', () async {
    void handleRequest(_) {
      harness
        ..sendResponseHeader()
        ..sendResponseValue(dummyValue)
        ..sendResponseValue(dummyValue)
        ..sendResponseTrailer();
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException:
          GrpcError.unimplemented('More than one response received'),
      serverHandlers: [handleRequest],
    );
  });

  test('Call throws if nothing is received', () async {
    void handleRequest(_) {
      harness.toClient.close();
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException: GrpcError.unavailable('Did not receive anything'),
      serverHandlers: [handleRequest],
    );
  });

  test('Call throws if trailers are missing', () async {
    void handleRequest(_) {
      harness
        ..sendResponseHeader()
        ..sendResponseValue(dummyValue)
        ..toClient.close();
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException: GrpcError.unavailable('Missing trailers'),
      serverHandlers: [handleRequest],
    );
  });

  test('Call throws if data is received before headers', () async {
    void handleRequest(_) {
      harness.sendResponseValue(dummyValue);
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException:
          GrpcError.unimplemented('Received data before headers'),
      serverHandlers: [handleRequest],
    );
  });

  test('Call throws if data is received after trailers', () async {
    void handleRequest(_) {
      harness
        ..sendResponseHeader()
        ..sendResponseTrailer(closeStream: false)
        ..sendResponseValue(dummyValue);
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException:
          GrpcError.unimplemented('Received data after trailers'),
      serverHandlers: [handleRequest],
    );
  });

  test('Call throws if multiple trailers are received', () async {
    void handleRequest(_) {
      harness
        ..sendResponseHeader()
        ..sendResponseTrailer(closeStream: false)
        ..sendResponseTrailer();
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException: GrpcError.unimplemented('Received multiple trailers'),
      serverHandlers: [handleRequest],
    );
  });

  test('Call throws if non-zero status is received', () async {
    const customStatusCode = 17;
    const customStatusMessage = 'Custom message';

    void handleRequest(_) {
      harness.toClient.add(HeadersStreamMessage([
        Header.ascii(':status', '200'),
        Header.ascii('content-type', 'application/grpc'),
        Header.ascii('grpc-status', '$customStatusCode'),
        Header.ascii('grpc-message', customStatusMessage)
      ], endStream: true));
      harness.toClient.close();
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException:
          GrpcError.custom(customStatusCode, message: customStatusMessage),
      serverHandlers: [handleRequest],
    );
  });

  test('Call throws on response stream errors', () async {
    void handleRequest(_) {
      harness.toClient.addError('Test error');
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException: GrpcError.unknown('Test error'),
      serverHandlers: [handleRequest],
    );
  });

  test('Call throws if unable to decode response', () async {
    const responseValue = 19;

    void handleRequest(StreamMessage message) {
      harness
        ..sendResponseHeader()
        ..sendResponseValue(responseValue)
        ..sendResponseTrailer();
    }

    harness.client = TestClient(harness.channel, decode: (bytes) {
      throw 'error decoding';
    });

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException: GrpcError.dataLoss('Error parsing response'),
      serverHandlers: [handleRequest],
    );
  });

  test('Call forwards known response stream errors', () async {
    final expectedException = GrpcError.dataLoss('Oops!');

    void handleRequest(_) {
      harness.toClient.addError(expectedException);
    }

    await harness.runFailureTest(
      clientCall: harness.client.unary(dummyValue),
      expectedException: expectedException,
      serverHandlers: [handleRequest],
    );
  });

  test('Known request errors are reported', () async {
    final expectedException = GrpcError.deadlineExceeded('Too late!');

    Stream<int> requests() async* {
      throw expectedException;
    }

    await harness.runFailureTest(
      clientCall: harness.client.clientStreaming(requests()),
      expectedException: expectedException,
      expectDone: false,
    );
  });

  test('Custom request errors are reported', () async {
    Stream<int> requests() async* {
      throw 'Error';
    }

    final expectedException = GrpcError.unknown('Error');
    await harness.runFailureTest(
      clientCall: harness.client.clientStreaming(requests()),
      expectedException: expectedException,
      expectDone: false,
    );
  });

  Future<void> makeUnaryCall() async {
    void handleRequest(StreamMessage message) {
      harness
        ..sendResponseHeader()
        ..sendResponseValue(1)
        ..sendResponseTrailer();
    }

    await harness.runTest(
      clientCall: harness.client.unary(1),
      expectedResult: 1,
      expectedPath: '/Test/Unary',
      serverHandlers: [handleRequest],
    );
  }

  test('Connection errors are reported', () async {
    final connectionStates = <ConnectionState>[];
    harness.connection!.connectionError = 'Connection error';
    harness.connection!.onStateChanged = (connection) {
      final state = connection.state;
      connectionStates.add(state);
    };

    final expectedException =
        GrpcError.unavailable('Error connecting: Connection error');

    await harness.expectThrows(
        harness.client.unary(dummyValue), expectedException);

    expect(
        connectionStates, [ConnectionState.connecting, ConnectionState.idle]);
  });

  test('Connections time out if idle', () async {
    final done = Completer();
    final connectionStates = <ConnectionState>[];
    harness.connection!.onStateChanged = (connection) {
      final state = connection.state;
      connectionStates.add(state);
      if (state == ConnectionState.idle) done.complete();
    };

    harness.channelOptions.idleTimeout = const Duration(microseconds: 10);

    await makeUnaryCall();
    harness.signalIdle();
    expect(
        connectionStates, [ConnectionState.connecting, ConnectionState.ready]);
    await done.future;
    expect(connectionStates, [
      ConnectionState.connecting,
      ConnectionState.ready,
      ConnectionState.idle
    ]);
  });

  test('Default reconnect backoff backs off', () {
    var lastBackoff = defaultBackoffStrategy(null);
    expect(lastBackoff, const Duration(seconds: 1));
    for (var i = 0; i < 12; i++) {
      final minNext = lastBackoff * (1.6 - 0.2);
      final maxNext = lastBackoff * (1.6 + 0.2);
      lastBackoff = defaultBackoffStrategy(lastBackoff);
      if (lastBackoff != const Duration(minutes: 2)) {
        expect(lastBackoff, greaterThanOrEqualTo(minNext));
        expect(lastBackoff, lessThanOrEqualTo(maxNext));
      }
    }
    expect(lastBackoff, const Duration(minutes: 2));
    expect(defaultBackoffStrategy(lastBackoff), const Duration(minutes: 2));
  });

  test('authority is computed correctly', () {
    final emptyOptions = ChannelOptions();
    expect(Http2ClientConnection('localhost', 8080, emptyOptions).authority,
        'localhost:8080');
    expect(Http2ClientConnection('localhost', 443, emptyOptions).authority,
        'localhost');
    final channelOptions = ChannelOptions(
        credentials: ChannelCredentials.insecure(authority: 'myauthority.com'));
    expect(Http2ClientConnection('localhost', 8080, channelOptions).authority,
        'myauthority.com');
    expect(Http2ClientConnection('localhost', 443, channelOptions).authority,
        'myauthority.com');
  });
}
