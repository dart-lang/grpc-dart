import 'package:grpc/grpc.dart';
import 'package:grpc/src/client/common.dart';
import 'package:mockito/mockito.dart';
import 'package:test/test.dart';

import '../src/client_utils.dart';

class FakeClientCall<T> extends Fake implements ClientCall<dynamic, T> {
  final T? _response;
  final GrpcError? _error;

  FakeClientCall(this._response, this._error);

  factory FakeClientCall.value(T response) => FakeClientCall(response, null);

  factory FakeClientCall.error(GrpcError error) => FakeClientCall(null, error);

  @override
  Stream<T> get response =>
      _error != null ? Stream.error(_error!) : Stream.value(_response!);
}

void main() {
  test(
      'Returns a new [ResponseFuture] which is completed with the result of the call to `onValue`',
      () async {
    final response = 1;
    final responseFuture = ResponseFuture(FakeClientCall.value(response));

    expect(await responseFuture, response);
    expect(await responseFuture.responseThen((p1) => p1), response);
    expect(responseFuture.responseThen((p1) => p1), isA<ResponseFuture>());

    expect(await responseFuture.responseThen((p1) => p1 * 2), response * 2);
    expect(
        await responseFuture
            .responseThen((p1) => p1 * 2)
            .responseThen((p2) => p2 * 2),
        response * 2 * 2);

    expect(responseFuture.responseThen((p1) => p1), isA<ResponseFuture>());
    expect(responseFuture.then((p1) => '2'), isA<Future>());

    expect(await responseFuture.responseThen((p1) => p1), response);
    expect(await responseFuture.then((p1) => '2'), '2');
  });

  test('Support Future after ResponseFuture', () async {
    final response = 1;
    final responseFuture = ResponseFuture<int>(FakeClientCall.value(response));

    final future = Future<String>.value('1');
    expect(responseFuture.then((value) => future), isA<Future<String>>());
    expect(await responseFuture.then((value) => future), '1');
  });

  test('Support ResponseFuture after ResponseFuture', () async {
    final response1 = 1;
    final response2 = 2;

    final responseFuture1 =
        ResponseFuture<int>(FakeClientCall.value(response1));
    final responseFuture2 =
        ResponseFuture<int>(FakeClientCall.value(response2));

    expect(responseFuture1.responseThen((value) => responseFuture2),
        isA<ResponseFuture<int>>());
    expect(await responseFuture1, response1);
    expect(await responseFuture1.responseThen((value) => responseFuture2),
        response2);
  });

  test(
      'Returns a new [ResponseFuture] that will be completed with the result of calling the `onError` callback.',
      () async {
    final response = 1;
    final responseFuture =
        ResponseFuture<int>(FakeClientCall.error(GrpcError.unknown()));

    final responseFutureCatchError =
        responseFuture.responseCatchError((error) => response * 2);

    expect(responseFutureCatchError, isA<ResponseFuture<int>>());
    expect(await responseFutureCatchError, response * 2);
  });
}
