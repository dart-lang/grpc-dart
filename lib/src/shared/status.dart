// Copyright (c) 2017, the gRPC project authors. Please see the AUTHORS file
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

import 'dart:convert';
import 'dart:io' show HttpStatus;

import 'package:meta/meta.dart';
import 'package:protobuf/protobuf.dart';

import '../generated/google/protobuf/any.pb.dart';
import '../generated/google/rpc/code.pbenum.dart';
import '../generated/google/rpc/error_details.pb.dart';
import '../generated/google/rpc/status.pb.dart';

class StatusCode {
  /// The operation completed successfully.
  static const ok = 0;

  /// The operation was cancelled (typically by the caller).
  static const cancelled = 1;

  /// Unknown error. An example of where this error may be returned is if a
  /// Status value received from another address space belongs to an error-space
  /// that is not known in this address space. Also errors raised by APIs that
  /// do not return enough error information may be converted to this error.
  static const unknown = 2;

  /// Client specified an invalid argument. Note that this differs from
  /// [failedPrecondition]. [invalidArgument] indicates arguments that are
  /// problematic regardless of the state of the system (e.g., a malformed file
  /// name).
  static const invalidArgument = 3;

  /// Deadline expired before operation could complete. For operations that
  /// change the state of the system, this error may be returned even if the
  /// operation has completed successfully. For example, a successful response
  /// from a server could have been delayed long enough for the deadline to
  /// expire.
  static const deadlineExceeded = 4;

  /// Some requested entity (e.g., file or directory) was not found.
  static const notFound = 5;

  /// Some entity that we attempted to create (e.g., file or directory) already
  /// exists.
  static const alreadyExists = 6;

  /// The caller does not have permission to execute the specified operation.
  /// [permissionDenied] must not be used for rejections caused by exhausting
  /// some resource (use [resourceExhausted] instead for those errors).
  /// [permissionDenied] must not be used if the caller cannot be identified
  /// (use [unauthenticated] instead for those errors).
  static const permissionDenied = 7;

  /// Some resource has been exhausted, perhaps a per-user quota, or perhaps the
  /// entire file system is out of space.
  static const resourceExhausted = 8;

  /// Operation was rejected because the system is not in a state required for
  /// the operation's execution. For example, directory to be deleted may be
  /// non-empty, an rmdir operation is applied to a non-directory, etc.
  ///
  /// A litmus test that may help a service implementor in deciding between
  /// [failedPrecondition], [aborted], and [unavailable]:
  /// (a) Use [unavailable] if the client can retry just the failing call.
  /// (b) Use [aborted] if the client should retry at a higher-level (e.g.,
  ///     restarting a read-modify-write sequence).
  /// (c) Use [failedPrecondition] if the client should not retry until the
  ///     system state has been explicitly fixed.  E.g., if an "rmdir" fails
  ///     because the directory is non-empty, [failedPrecondition] should be
  ///     returned since the client should not retry unless they have first
  ///     fixed up the directory by deleting files from it.
  static const failedPrecondition = 9;

  /// The operation was aborted, typically due to a concurrency issue like
  /// sequencer check failures, transaction aborts, etc.
  ///
  /// See litmus test above for deciding between [failedPrecondition],
  /// [aborted], and [unavailable].
  static const aborted = 10;

  /// Operation was attempted past the valid range. E.g., seeking or reading
  /// past end of file.
  ///
  /// Unlike invalidArgument, this error indicates a problem that may be fixed
  /// if the system state changes. For example, a 32-bit file system will
  /// generate invalidArgument if asked to read at an offset that is not in the
  /// range [0,2^32-1], but it will generate [outOfRange] if asked to read from
  /// an offset past the current file size.
  ///
  /// There is a fair bit of overlap between [failedPrecondition] and
  /// [outOfRange]. We recommend using [outOfRange] (the more specific error)
  /// when it applies so that callers who are iterating through a space can
  /// easily look for an [outOfRange] error to detect when they are done.
  static const outOfRange = 11;

  /// Operation is not implemented or not supported/enabled in this service.
  static const unimplemented = 12;

  /// Internal errors. Means some invariants expected by underlying system has
  /// been broken. If you see one of these errors, something is very broken.
  static const internal = 13;

  /// The service is currently unavailable.  This is a most likely a transient
  /// condition and may be corrected by retrying with a backoff.
  ///
  /// See litmus test above for deciding between [failedPrecondition],
  /// [aborted], and [unavailable].
  static const unavailable = 14;

  /// Unrecoverable data loss or corruption.
  static const dataLoss = 15;

  /// The request does not have valid authentication credentials for the
  /// operation.
  static const unauthenticated = 16;

  /// Mapping taken from gRPC-Web JS implementation:
  /// https://github.com/grpc/grpc-web/blob/master/javascript/net/grpc/web/statuscode.js
  static const _httpStatusToGrpcStatus = <int, int>{
    HttpStatus.ok: StatusCode.ok,
    HttpStatus.badRequest: StatusCode.invalidArgument,
    HttpStatus.unauthorized: StatusCode.unauthenticated,
    HttpStatus.forbidden: StatusCode.permissionDenied,
    HttpStatus.notFound: StatusCode.notFound,
    HttpStatus.conflict: StatusCode.aborted,
    HttpStatus.preconditionFailed: StatusCode.failedPrecondition,
    HttpStatus.tooManyRequests: StatusCode.resourceExhausted,
    HttpStatus.clientClosedRequest: StatusCode.cancelled,
    HttpStatus.internalServerError: StatusCode.unknown,
    HttpStatus.notImplemented: StatusCode.unimplemented,
    HttpStatus.serviceUnavailable: StatusCode.unavailable,
    HttpStatus.gatewayTimeout: StatusCode.deadlineExceeded,
  };

  /// Creates a gRPC Status code from a HTTP Status code
  static int fromHttpStatus(int status) {
    return _httpStatusToGrpcStatus[status] ?? StatusCode.unknown;
  }
}

class GrpcError implements Exception {
  final int code;
  final String? message;
  final Object? rawResponse;
  final Map<String, String>? trailers;
  final List<GeneratedMessage>? details;

  /// Custom error code.
  GrpcError.custom(this.code,
      {this.message, this.details, this.rawResponse, this.trailers = const {}});

  /// The operation completed successfully.
  GrpcError.ok([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.ok;

  /// The operation was cancelled (typically by the caller).
  GrpcError.cancelled([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.cancelled;

  /// Unknown error. An example of where this error may be returned is if a
  /// Status value received from another address space belongs to an error-space
  /// that is not known in this address space. Also errors raised by APIs that
  /// do not return enough error information may be converted to this error.
  GrpcError.unknown([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.unknown;

  /// Client specified an invalid argument. Note that this differs from
  /// [failedPrecondition]. [invalidArgument] indicates arguments that are
  /// problematic regardless of the state of the system (e.g., a malformed file
  /// name).
  GrpcError.invalidArgument([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.invalidArgument;

  /// Deadline expired before operation could complete. For operations that
  /// change the state of the system, this error may be returned even if the
  /// operation has completed successfully. For example, a successful response
  /// from a server could have been delayed long enough for the deadline to
  /// expire.
  GrpcError.deadlineExceeded([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.deadlineExceeded;

  /// Some requested entity (e.g., file or directory) was not found.
  GrpcError.notFound([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.notFound;

  /// Some entity that we attempted to create (e.g., file or directory) already
  /// exists.
  GrpcError.alreadyExists([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.alreadyExists;

  /// The caller does not have permission to execute the specified operation.
  /// [permissionDenied] must not be used for rejections caused by exhausting
  /// some resource (use [resourceExhausted] instead for those errors).
  /// [permissionDenied] must not be used if the caller cannot be identified
  /// (use [unauthenticated] instead for those errors).
  GrpcError.permissionDenied([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.permissionDenied;

  /// Some resource has been exhausted, perhaps a per-user quota, or perhaps the
  /// entire file system is out of space.
  GrpcError.resourceExhausted([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.resourceExhausted;

  /// Operation was rejected because the system is not in a state required for
  /// the operation's execution. For example, directory to be deleted may be
  /// non-empty, an rmdir operation is applied to a non-directory, etc.
  ///
  /// A litmus test that may help a service implementor in deciding between
  /// [failedPrecondition], [aborted], and [unavailable]:
  /// (a) Use [unavailable] if the client can retry just the failing call.
  /// (b) Use [aborted] if the client should retry at a higher-level (e.g.,
  ///     restarting a read-modify-write sequence).
  /// (c) Use [failedPrecondition] if the client should not retry until the
  ///     system state has been explicitly fixed.  E.g., if an "rmdir" fails
  ///     because the directory is non-empty, [failedPrecondition] should be
  ///     returned since the client should not retry unless they have first
  ///     fixed up the directory by deleting files from it.
  GrpcError.failedPrecondition([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.failedPrecondition;

  /// The operation was aborted, typically due to a concurrency issue like
  /// sequencer check failures, transaction aborts, etc.
  ///
  /// See litmus test above for deciding between [failedPrecondition],
  /// [aborted], and [unavailable].
  GrpcError.aborted([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.aborted;

  /// Operation was attempted past the valid range. E.g., seeking or reading
  /// past end of file.
  ///
  /// Unlike invalidArgument, this error indicates a problem that may be fixed
  /// if the system state changes. For example, a 32-bit file system will
  /// generate invalidArgument if asked to read at an offset that is not in the
  /// range [0,2^32-1], but it will generate [outOfRange] if asked to read from
  /// an offset past the current file size.
  ///
  /// There is a fair bit of overlap between [failedPrecondition] and
  /// [outOfRange]. We recommend using [outOfRange] (the more specific error)
  /// when it applies so that callers who are iterating through a space can
  /// easily look for an [outOfRange] error to detect when they are done.
  GrpcError.outOfRange([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.outOfRange;

  /// Operation is not implemented or not supported/enabled in this service.
  GrpcError.unimplemented([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.unimplemented;

  /// Internal errors. Means some invariants expected by underlying system has
  /// been broken. If you see one of these errors, something is very broken.
  // TODO(sigurdm): This should probably not be an [Exception].
  GrpcError.internal(
      [this.message, this.details, this.rawResponse, this.trailers])
      : code = StatusCode.internal;

  /// The service is currently unavailable.  This is a most likely a transient
  /// condition and may be corrected by retrying with a backoff.
  ///
  /// See litmus test above for deciding between [failedPrecondition],
  /// [aborted], and [unavailable].
  GrpcError.unavailable([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.unavailable;

  /// Unrecoverable data loss or corruption.
  GrpcError.dataLoss([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.dataLoss;

  /// The request does not have valid authentication credentials for the
  /// operation.
  GrpcError.unauthenticated([this.message, this.details, this.rawResponse])
      : trailers = const {},
        code = StatusCode.unauthenticated;

  /// Given a status code, return the name
  String get codeName => (Code.valueOf(code) ?? Code.UNKNOWN).name;

  @override
  bool operator ==(other) {
    if (other is! GrpcError) return false;
    return code == other.code && message == other.message;
  }

  @override
  int get hashCode => code.hashCode ^ (message?.hashCode ?? 17);

  @override
  String toString() =>
      'gRPC Error (code: $code, codeName: $codeName, message: $message, '
      'details: $details, rawResponse: $rawResponse, trailers: $trailers)';
}

/// Parse error details `Any` object into the right kind of `GeneratedMessage`.
///
/// This list comes from `error_details.proto`. If any new error detail types are
/// added to the protbuf definition, this function should be updated accordingly to
/// support them.
GeneratedMessage parseErrorDetailsFromAny(Any any) {
  switch (any.typeUrl) {
    case 'type.googleapis.com/google.rpc.RetryInfo':
      return RetryInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.DebugInfo':
      return DebugInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.QuotaFailure':
      return QuotaFailure.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.ErrorInfo':
      return ErrorInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.PreconditionFailure':
      return PreconditionFailure.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.BadRequest':
      return BadRequest.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.RequestInfo':
      return RequestInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.ResourceInfo':
      return ResourceInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.Help':
      return Help.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.LocalizedMessage':
      return LocalizedMessage.fromBuffer(any.value);

    default:
      return any;
  }
}

/// Validate HTTP status and Content-Type which arrived with the response:
/// reject reponses with non-ok (200) status or unsupported Content-Type.
///
/// Note that grpc-status arrives in trailers and will be handled by
/// [ClientCall._onResponseData].
///
/// gRPC over HTTP2 protocol specification mandates the following:
///
///     Implementations should expect broken deployments to send non-200 HTTP
///     status codes in responses as well as a variety of non-GRPC content-types
///     and to omit Status & Status-Message. Implementations must synthesize a
///     Status & Status-Message to propagate to the application layer when this
///     occurs.
///
void validateHttpStatusAndContentType(
    int? httpStatus, Map<String, String> headers,
    {Object? rawResponse}) {
  if (httpStatus == null) {
    throw GrpcError.unknown(
        'HTTP response status is unknown', null, rawResponse);
  }

  if (httpStatus == 0) {
    throw GrpcError.unknown(
        'HTTP request completed without a status (potential CORS issue)',
        null,
        rawResponse);
  }

  final status = StatusCode.fromHttpStatus(httpStatus);
  if (status != StatusCode.ok) {
    // [httpStatus] itself already indicates an error. Check if we also
    // received grpc-status/message (i.e. this is a Trailers-Only response)
    // and use this information to report a better error to the application
    // layer. However prefer to use status code derived from HTTP status
    // if grpc-status itself does not provide an informative error.
    final error = grpcErrorDetailsFromTrailers(headers);
    if (error == null || error.code == StatusCode.unknown) {
      throw GrpcError.custom(
        status,
        message: error?.message ??
            'HTTP connection completed with ${httpStatus} instead of 200',
        details: error?.details,
        rawResponse: rawResponse,
        trailers: error?.trailers ?? toCustomTrailers(headers),
      );
    }
    throw error;
  }

  final contentType = headers['content-type'];
  if (contentType == null) {
    throw GrpcError.unknown('missing content-type header', null, rawResponse);
  }

  // Check if content-type header indicates a supported format.
  if (!_validContentTypePrefix.any(contentType.startsWith)) {
    throw GrpcError.unknown(
        'unsupported content-type (${contentType})', null, rawResponse);
  }
}

GrpcError? grpcErrorDetailsFromTrailers(Map<String, String> trailers) {
  final status = trailers['grpc-status'];
  final statusCode = status != null ? int.parse(status) : StatusCode.unknown;

  if (statusCode != StatusCode.ok) {
    final message = _tryDecodeStatusMessage(trailers['grpc-message']);
    final statusDetails = trailers[_statusDetailsHeader];
    return GrpcError.custom(
      statusCode,
      message: message,
      trailers: toCustomTrailers(trailers),
      details: statusDetails == null
          ? const <GeneratedMessage>[]
          : decodeStatusDetails(statusDetails),
    );
  }

  return null;
}

Map<String, String> toCustomTrailers(Map<String, String> trailers) {
  return Map.from(trailers)
    ..remove(':status')
    ..remove('content-type')
    ..remove('grpc-status')
    ..remove('grpc-message');
}

const _statusDetailsHeader = 'grpc-status-details-bin';

/// All accepted content-type header's prefix. We are being more permissive
/// then gRPC and gRPC-Web specifications because some of the services
/// return slightly different content-types.
const _validContentTypePrefix = [
  'application/grpc',
  'application/json+protobuf',
  'application/x-protobuf'
];

/// Given a string of base64url data, attempt to parse a Status object from it.
/// Once parsed, it will then map each detail item and attempt to parse it into
/// its respective GeneratedMessage type, returning the list of parsed detail items
/// as a `List<GeneratedMessage>`.
///
/// Prior to creating the Status object we pad the data to ensure its length is
/// an even multiple of 4, which is a requirement in Dart when decoding base64url data.
///
/// If any errors are thrown during decoding/parsing, it will return an empty list.
@visibleForTesting
List<GeneratedMessage> decodeStatusDetails(String data) {
  try {
    final parsedStatus = Status.fromBuffer(
        base64Url.decode(data.padRight((data.length + 3) & ~3, '=')));
    return parsedStatus.details.map(parseErrorDetailsFromAny).toList();
  } catch (e) {
    return <GeneratedMessage>[];
  }
}

/// Decode percent encoded status message contained in 'grpc-message' trailer.
String? _tryDecodeStatusMessage(String? statusMessage) {
  if (statusMessage == null) {
    return statusMessage;
  }

  try {
    return Uri.decodeFull(statusMessage);
  } catch (_) {
    // gRPC over HTTP2 protocol specification mandates:
    //
    //    When decoding invalid values, implementations MUST NOT error or throw
    //    away the message. At worst, the implementation can abort decoding the
    //    status message altogether such that the user would received the raw
    //    percent-encoded form.
    //
    return statusMessage;
  }
}
