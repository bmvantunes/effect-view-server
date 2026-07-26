import { describe, expect, it } from "@effect/vitest";
import * as Contract from "./contract";
import {
  GrpcAdapterFailure,
  GrpcLeasedMetrics,
  GrpcMaterializedMetrics,
  GrpcRejectionLocation,
  GrpcSourceAdapter,
  GrpcSourceConfigurationError,
  grpc,
} from "./model";

describe("gRPC public contract entrypoint", () => {
  it("exports exactly the approved runtime contract", () => {
    expect(Object.keys(Contract).sort()).toStrictEqual([
      "GrpcAdapterFailure",
      "GrpcLeasedMetrics",
      "GrpcMaterializedMetrics",
      "GrpcRejectionLocation",
      "GrpcSourceAdapter",
      "GrpcSourceConfigurationError",
      "grpc",
    ]);
    expect({
      GrpcAdapterFailure: Contract.GrpcAdapterFailure,
      GrpcLeasedMetrics: Contract.GrpcLeasedMetrics,
      GrpcMaterializedMetrics: Contract.GrpcMaterializedMetrics,
      GrpcRejectionLocation: Contract.GrpcRejectionLocation,
      GrpcSourceAdapter: Contract.GrpcSourceAdapter,
      GrpcSourceConfigurationError: Contract.GrpcSourceConfigurationError,
      grpc: Contract.grpc,
    }).toStrictEqual({
      GrpcAdapterFailure,
      GrpcLeasedMetrics,
      GrpcMaterializedMetrics,
      GrpcRejectionLocation,
      GrpcSourceAdapter,
      GrpcSourceConfigurationError,
      grpc,
    });
  });
});
