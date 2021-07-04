// Copyright (c) 2018, the gRPC project authors. Please see the AUTHORS file
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
import 'dart:developer';

import '../shared/codec.dart';
import '../shared/message.dart';
import '../shared/profiler.dart';
import '../shared/status.dart';
import 'common.dart';
import 'connection.dart';
import 'method.dart';
import 'transport/transport.dart';

const _reservedHeaders = [
  'content-type',
  'te',
  'grpc-timeout',
  'grpc-accept-encoding',
  'grpc-encoding',
  'user-agent',
];

/// Provides per-RPC metadata.
///
/// Metadata providers will be invoked for every RPC, and can add their own
/// metadata to the RPC. If the function returns a [Future], the RPC will await
/// completion of the returned [Future] before transmitting the request.
///
/// The metadata provider is given the current [metadata] map (possibly modified
/// by previous metadata providers) and the [uri] that is being called, and is
/// expected to modify the map before returning or before completing the
/// returned [Future].
typedef MetadataProvider = FutureOr<void> Function(
    Map<String, String> metadata, String uri);

/// Runtime options for an RPC.
class CallOptions {
  final Map<String, String> metadata;
  final Duration? timeout;
  final List<MetadataProvider> metadataProviders;
  final Codec? compression;

  CallOptions._(
    this.metadata,
    this.timeout,
    this.metadataProviders,
    this.compression,
  );

  /// Creates a [CallOptions] object.
  ///
  /// [CallOptions] can specify static [metadata], set the [timeout], and
  /// configure per-RPC metadata [providers]. The metadata [providers] are
  /// invoked in order for every RPC, and can modify the outgoing metadata
  /// (including metadata provided by previous providers).
  factory CallOptions({
    Map<String, String>? metadata,
    Duration? timeout,
    List<MetadataProvider>? providers,
    Codec? compression,
  }) {
    return CallOptions._(
      Map.unmodifiable(metadata ?? {}),
      timeout,
      List.unmodifiable(providers ?? []),
      compression,
    );
  }

  factory CallOptions.from(Iterable<CallOptions> options) =>
      options.fold(CallOptions(), (p, o) => p.mergedWith(o));

  CallOptions mergedWith(CallOptions? other) {
    if (other == null) return this;
    final mergedMetadata = Map.from(metadata)..addAll(other.metadata);
    final mergedTimeout = other.timeout ?? timeout;
    final mergedProviders = List.from(metadataProviders)
      ..addAll(other.metadataProviders);
    final mergedCompression = other.compression ?? compression;
    return CallOptions._(
      Map.unmodifiable(mergedMetadata),
      mergedTimeout,
      List.unmodifiable(mergedProviders),
      mergedCompression,
    );
  }
}

/// Runtime options for gRPC-web.
class WebCallOptions extends CallOptions {
  /// Whether to eliminate the CORS preflight request.
  ///
  /// If set to [true], all HTTP headers will be packed into an '$httpHeaders'
  /// query parameter, which should downgrade complex CORS requests into
  /// simple ones. This eliminates an extra roundtrip.
  ///
  /// For this to work correctly, a proxy server must be set up that
  /// understands the query parameter and can unpack/send the original
  /// list of headers to the server endpoint.
  final bool? bypassCorsPreflight;

  /// Whether to send credentials along with the XHR.
  ///
  /// This may be required for proxying or wherever the server
  /// needs to otherwise inspect client cookies for that domain.
  final bool? withCredentials;
  // TODO(mightyvoice): add a list of extra QueryParameter for gRPC.

  WebCallOptions._(Map<String, String> metadata, Duration? timeout,
      List<MetadataProvider> metadataProviders,
      {this.bypassCorsPreflight, this.withCredentials})
      : super._(metadata, timeout, metadataProviders, null);

  /// Creates a [WebCallOptions] object.
  ///
  /// [WebCallOptions] can specify static [metadata], [timeout],
  /// metadata [providers] of [CallOptions], [bypassCorsPreflight] and
  /// [withCredentials] for CORS request.
  factory WebCallOptions(
      {Map<String, String>? metadata,
      Duration? timeout,
      List<MetadataProvider>? providers,
      bool? bypassCorsPreflight,
      bool? withCredentials}) {
    return WebCallOptions._(Map.unmodifiable(metadata ?? {}), timeout,
        List.unmodifiable(providers ?? []),
        bypassCorsPreflight: bypassCorsPreflight ?? false,
        withCredentials: withCredentials ?? false);
  }

  @override
  CallOptions mergedWith(CallOptions? other) {
    if (other == null) return this;

    final mergedMetadata = Map.from(metadata)..addAll(other.metadata);
    final mergedTimeout = other.timeout ?? timeout;
    final mergedProviders = List.from(metadataProviders)
      ..addAll(other.metadataProviders);

    if (other is! WebCallOptions) {
      return WebCallOptions._(Map.unmodifiable(mergedMetadata), mergedTimeout,
          List.unmodifiable(mergedProviders),
          bypassCorsPreflight: bypassCorsPreflight,
          withCredentials: withCredentials);
    }

    final mergedBypassCorsPreflight =
        other.bypassCorsPreflight ?? bypassCorsPreflight;
    final mergedWithCredentials = other.withCredentials ?? withCredentials;
    return WebCallOptions._(Map.unmodifiable(mergedMetadata), mergedTimeout,
        List.unmodifiable(mergedProviders),
        bypassCorsPreflight: mergedBypassCorsPreflight,
        withCredentials: mergedWithCredentials);
  }
}

/// An active call to a gRPC endpoint.
class ClientCall<Q, R> implements Response {
  final ClientMethod<Q, R> _method;
  final Stream<Q> _requests;
  final CallOptions options;

  final _headers = Completer<Map<String, String>>();
  final _trailers = Completer<Map<String, String>>();
  bool _hasReceivedResponses = false;

  late Map<String, String> _headerMetadata;

  GrpcTransportStream? _stream;
  final _responses = StreamController<R>();
  StreamSubscription<List<int>>? _requestSubscription;
  StreamSubscription<GrpcMessage>? _responseSubscription;

  bool isCancelled = false;
  Timer? _timeoutTimer;

  final TimelineTask? _requestTimeline;
  TimelineTask? _responseTimeline;

  ClientCall(this._method, this._requests, this.options,
      [this._requestTimeline]) {
    _requestTimeline?.start('gRPC Request: ${_method.path}', arguments: {
      'method': _method.path,
      'timeout': options.timeout?.toString(),
    });
    _responses.onListen = _onResponseListen;
    if (options.timeout != null) {
      _timeoutTimer = Timer(options.timeout!, _onTimedOut);
    }
  }

  void onConnectionError(error) {
    _terminateWithError(GrpcError.unavailable('Error connecting: $error'));
  }

  void _terminateWithError(GrpcError error) {
    _finishTimelineWithError(error, _requestTimeline);
    if (!_responses.isClosed) {
      _responses.addError(error);
    }
    _safeTerminate();
  }

  static Map<String, String> _sanitizeMetadata(Map<String, String> metadata) {
    final sanitizedMetadata = <String, String>{};
    metadata.forEach((String key, String value) {
      final lowerCaseKey = key.trim().toLowerCase();
      if (!lowerCaseKey.startsWith(':') &&
          !_reservedHeaders.contains(lowerCaseKey)) {
        sanitizedMetadata[lowerCaseKey] = value.trim();
      }
    });
    return sanitizedMetadata;
  }

// TODO(sigurdm): Find out why we do this.
  static String audiencePath(ClientMethod method) {
    final lastSlashPos = method.path.lastIndexOf('/');
    return lastSlashPos == -1
        ? method.path
        : method.path.substring(0, lastSlashPos);
  }

  void onConnectionReady(ClientConnection connection) {
    if (isCancelled) return;

    if (options.metadataProviders.isEmpty) {
      _sendRequest(connection, _sanitizeMetadata(options.metadata));
    } else {
      final metadata = Map<String, String>.from(options.metadata);
      Future.forEach(
              options.metadataProviders,
              (MetadataProvider provider) => provider(metadata,
                  '${connection.scheme}://${connection.authority}${audiencePath(_method)}'))
          .then((_) => _sendRequest(connection, _sanitizeMetadata(metadata)))
          .catchError(_onMetadataProviderError);
    }
  }

  void _onMetadataProviderError(error) {
    _terminateWithError(GrpcError.internal('Error making call: $error'));
  }

  void _sendRequest(ClientConnection connection, Map<String, String> metadata) {
    late final GrpcTransportStream stream;
    try {
      stream = connection.makeRequest(
        _method.path,
        options.timeout,
        metadata,
        _onRequestError,
        callOptions: options,
      );
    } catch (e) {
      _terminateWithError(GrpcError.unavailable('Error making call: $e'));
      return;
    }
    _requestTimeline?.instant('Request sent', arguments: {
      'metadata': metadata,
    });
    _requestSubscription = _requests
        .map((data) {
          _requestTimeline?.instant('Data sent', arguments: {
            'data': data.toString(),
          });
          _requestTimeline?.finish();
          return _method.requestSerializer(data);
        })
        .handleError(_onRequestError)
        .listen(stream.outgoingMessages.add,
            onError: stream.outgoingMessages.addError,
            onDone: stream.outgoingMessages.close,
            cancelOnError: true);
    _stream = stream;
    // The response stream might have been listened to before _stream was ready,
    // so try setting up the subscription here as well.
    _onResponseListen();
  }

  void _finishTimelineWithError(GrpcError error, TimelineTask? timeline) {
    timeline?.finish(arguments: {
      'error': error.toString(),
    });
  }

  void _onTimedOut() {
    final error = GrpcError.deadlineExceeded('Deadline exceeded');
    _finishTimelineWithError(error, _requestTimeline);
    _responses.addError(error);
    _safeTerminate();
  }

  /// Subscribe to incoming response messages, once [_stream] is available, and
  /// the caller has subscribed to the [_responses] stream.
  void _onResponseListen() {
    if (_stream != null &&
        _responses.hasListener &&
        _responseSubscription == null) {
      // ignore: cancel_subscriptions
      final subscription = _stream!.incomingMessages.listen(_onResponseData,
          onError: _onResponseError,
          onDone: _onResponseDone,
          cancelOnError: true);
      if (_responses.isPaused) {
        subscription.pause();
      }
      _responses.onPause = subscription.pause;
      _responses.onResume = subscription.resume;
      _responses.onCancel = cancel;

      _responseSubscription = subscription;
    }
  }

  /// Emit an error response to the user, and tear down this call.
  void _responseError(GrpcError error, [StackTrace? stackTrace]) {
    _finishTimelineWithError(error, _responseTimeline);
    _responses.addError(error, stackTrace);
    _timeoutTimer?.cancel();
    _requestSubscription?.cancel();
    _responseSubscription!.cancel();
    _responses.close();
    _stream!.terminate();
  }

  /// If there's an error status then process it as a response error.
  void _checkForErrorStatus(Map<String, String> trailers) {
    final error = grpcErrorDetailsFromTrailers(trailers);
    if (error != null) {
      _responseError(error);
    }
  }

  /// Data handler for responses coming from the server. Handles header/trailer
  /// metadata, and forwards response objects to [_responses].
  void _onResponseData(GrpcMessage data) {
    if (data is GrpcData) {
      if (!_headers.isCompleted) {
        _responseError(GrpcError.unimplemented('Received data before headers'));
        return;
      }
      if (_trailers.isCompleted) {
        _responseError(GrpcError.unimplemented('Received data after trailers'));
        return;
      }
      try {
        final decodedData = _method.responseDeserializer(data.data);
        _responseTimeline?.instant('Data received', arguments: {
          'data': decodedData.toString(),
        });
        _responses.add(decodedData);
        _hasReceivedResponses = true;
      } catch (e, s) {
        _responseError(GrpcError.dataLoss('Error parsing response'), s);
      }
    } else if (data is GrpcMetadata) {
      if (!_headers.isCompleted) {
        _headerMetadata = data.metadata;
        if (_requestTimeline != null) {
          _responseTimeline = timelineTaskFactory(
              parent: _requestTimeline, filterKey: clientTimelineFilterKey);
        }
        _responseTimeline?.start('gRPC Response');
        _responseTimeline?.instant('Metadata received', arguments: {
          'headers': _headerMetadata.toString(),
        });
        _headers.complete(_headerMetadata);
        return;
      }
      if (_trailers.isCompleted) {
        _responseError(GrpcError.unimplemented('Received multiple trailers'));
        return;
      }
      final metadata = data.metadata;
      _responseTimeline?.instant('Metadata received', arguments: {
        'trailers': metadata.toString(),
      });
      _trailers.complete(metadata);

      /// Process status error if necessary
      _checkForErrorStatus(metadata);
    } else {
      _responseError(GrpcError.unimplemented('Unexpected frame received'));
    }
  }

  /// Handler for response errors. Forward the error to the [_responses] stream,
  /// wrapped if necessary.
  void _onResponseError(error, StackTrace stackTrace) {
    if (error is GrpcError) {
      _responseError(error, stackTrace);
      return;
    }
    _responseError(GrpcError.unknown(error.toString()), stackTrace);
  }

  /// Handles closure of the response stream. Verifies that server has sent
  /// response messages and header/trailer metadata, as necessary.
  void _onResponseDone() {
    if (!_headers.isCompleted) {
      _responseError(GrpcError.unavailable('Did not receive anything'));
      return;
    }
    if (!_trailers.isCompleted) {
      if (_hasReceivedResponses) {
        // Trailers are required after receiving data.
        _responseError(GrpcError.unavailable('Missing trailers'));
        return;
      }

      // Only received a header frame and no data frames, so the header
      // should contain "trailers" as well (Trailers-Only).
      _trailers.complete(_headerMetadata);

      /// Process status error if necessary
      _checkForErrorStatus(_headerMetadata);
    }
    _responseTimeline?.finish();
    _timeoutTimer?.cancel();
    _responses.close();
    _responseSubscription!.cancel();
  }

  /// Error handler for the requests stream. Something went wrong while trying
  /// to send the request to the server. Abort the request, and forward the
  /// error to the user code on the [_responses] stream.
  void _onRequestError(error, StackTrace stackTrace) {
    if (error is! GrpcError) {
      error = GrpcError.unknown(error.toString());
    }

    _finishTimelineWithError(error, _requestTimeline);
    _responses.addError(error, stackTrace);
    _timeoutTimer?.cancel();
    _responses.close();
    _requestSubscription?.cancel();
    _responseSubscription?.cancel();
    _stream!.terminate();
  }

  Stream<R> get response => _responses.stream;

  @override
  Future<Map<String, String>> get headers => _headers.future;

  @override
  Future<Map<String, String>> get trailers => _trailers.future;

  @override
  Future<void> cancel() {
    if (!_responses.isClosed) {
      final error = GrpcError.cancelled('Cancelled by client.');
      _responses.addError(error);
      _finishTimelineWithError(error, _requestTimeline);
    }
    return _terminate();
  }

  Future<void> _terminate() async {
    isCancelled = true;
    _timeoutTimer?.cancel();
    // Don't await _responses.close() here. It'll only complete once the done
    // event has been delivered, and it's the caller of this function that is
    // reading from responses as well, so we might end up deadlocked.
    _responses.close();
    _stream?.terminate();
    final futures = <Future>[];
    if (_requestSubscription != null) {
      futures.add(_requestSubscription!.cancel());
    }
    if (_responseSubscription != null) {
      futures.add(_responseSubscription!.cancel());
    }
    await Future.wait(futures);
  }

  Future<void> _safeTerminate() async {
    try {
      await _terminate();
    } catch (_) {}
  }
}
