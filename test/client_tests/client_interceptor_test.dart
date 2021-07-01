import 'dart:async';

import 'package:grpc/grpc.dart';
import 'package:grpc/src/client/interceptor.dart';
import 'package:http2/transport.dart';
import 'package:test/test.dart';

import '../src/client_utils.dart';
import '../src/utils.dart';

class InterceptorInvocation {
  final int id;
  final int unary;
  final int streaming;

  InterceptorInvocation(this.id, this.unary, this.streaming);

  @override
  String toString() {
    return '{id: ${id}, unary: ${unary}, streaming: ${streaming}}';
  }
}

class FakeInterceptor implements ClientInterceptor {
  final int _id;
  int _unary = 0;
  int _streaming = 0;

  static final _invocations = <InterceptorInvocation>[];

  FakeInterceptor(this._id);

  @override
  ResponseFuture<R> interceptUnary<Q, R>(ClientMethod<Q, R> method, Q request,
      CallOptions options, ClientUnaryInvoker<Q, R> invoker) {
    _invocations.add(InterceptorInvocation(_id, ++_unary, _streaming));
    return ResponseFuture.wrap(
            Future.delayed(Duration(seconds: 1), () => 'dummy'))
        .then((_) => invoker(method, request, _inject(options)))
        .then((foo) async => foo)
        .whenComplete(() => 'complete')
        .then((bar) => bar)
        .catchError((e, s) => print('$e at $s'))
        .timeout(Duration(seconds: 5));
  }

  @override
  ResponseStream<R> interceptStreaming<Q, R>(
      ClientMethod<Q, R> method,
      Stream<Q> requests,
      CallOptions options,
      ClientStreamingInvoker<Q, R> invoker) {
    _invocations.add(InterceptorInvocation(_id, _unary, ++_streaming));

    final requestStream = _id > 10
        ? requests.cast<int>().map((req) => req * _id).cast<Q>()
        : requests;

    return invoker(method, requestStream, _inject(options));
  }

  CallOptions _inject(CallOptions options) {
    return options.mergedWith(CallOptions(metadata: {
      'x-interceptor': _invocations.map((i) => i.toString()).join(', '),
    }));
  }

  static void tearDown() {
    _invocations.clear();
  }
}

void main() {
  test('single unary interceptor', () async {
    final harness = ClientHarness()
      ..interceptors = [FakeInterceptor(1)]
      ..setUp();

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
      expectedCustomHeaders: {
        'x-interceptor': '{id: 1, unary: 1, streaming: 0}'
      },
      serverHandlers: [handleRequest],
    );

    harness.tearDown();
    FakeInterceptor.tearDown();
  });

  test('multiple unary interceptors', () async {
    final harness = ClientHarness()
      ..interceptors = [FakeInterceptor(1), FakeInterceptor(2)]
      ..setUp();

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
      expectedCustomHeaders: {
        'x-interceptor':
            '{id: 1, unary: 1, streaming: 0}, {id: 2, unary: 1, streaming: 0}'
      },
      serverHandlers: [handleRequest],
    );

    harness.tearDown();
    FakeInterceptor.tearDown();
  });

  test('single streaming interceptor', () async {
    final harness = ClientHarness()
      ..interceptors = [FakeInterceptor(1)]
      ..setUp();

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
      expectedCustomHeaders: {
        'x-interceptor': '{id: 1, unary: 0, streaming: 1}'
      },
      serverHandlers: [handleRequest, handleRequest, handleRequest],
      doneHandler: handleDone,
    );

    harness.tearDown();
    FakeInterceptor.tearDown();
  });

  test('multiple streaming interceptors', () async {
    final harness = ClientHarness()
      ..interceptors = [FakeInterceptor(1), FakeInterceptor(2)]
      ..setUp();

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
      expectedCustomHeaders: {
        'x-interceptor':
            '{id: 1, unary: 0, streaming: 1}, {id: 2, unary: 0, streaming: 1}'
      },
      serverHandlers: [handleRequest, handleRequest, handleRequest],
      doneHandler: handleDone,
    );

    harness.tearDown();
    FakeInterceptor.tearDown();
  });

  test('streaming interceptors with request stream operation', () async {
    const ids = [21, 73, 37];

    final harness = ClientHarness()
      ..interceptors = ids.map((e) => FakeInterceptor(e))
      ..setUp();

    const requests = [1, 15, 7];
    const multiplier = 21 * 73 * 37;
    const responses = [1 * multiplier, 15 * multiplier, 7 * multiplier];

    var index = 0;

    void handleRequest(StreamMessage message) {
      final data = validateDataMessage(message);
      final request = mockDecode(data.data);
      if (index++ == 0) {
        harness.sendResponseHeader();
      }
      harness.sendResponseValue(request);
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

    harness.tearDown();
    FakeInterceptor.tearDown();
  });
}
