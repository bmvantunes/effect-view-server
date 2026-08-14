import { create, toBinary, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "@effect/vitest";
import {
  grpc,
  GrpcSourceConfigurationError,
  isGrpcServiceDescriptor,
  isGrpcSourceDefinitionOptions,
  selectedGrpcMethod,
} from "./model";

type RequestMessage = Message<"grpc.model.Request"> & {
  readonly region: string;
};

type EventMessage = Message<"grpc.model.Event"> & {
  readonly id: string;
  readonly region: string;
};

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/model.proto",
          package: "grpc.model",
          syntax: "proto3",
          messageType: [
            {
              name: "Request",
              field: [
                {
                  name: "region",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "Event",
              field: [
                {
                  name: "id",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "region",
                  number: 2,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
          ],
          service: [
            {
              name: "Orders",
              method: [
                {
                  name: "Stream",
                  inputType: ".grpc.model.Request",
                  outputType: ".grpc.model.Event",
                  serverStreaming: true,
                },
                {
                  name: "Get",
                  inputType: ".grpc.model.Request",
                  outputType: ".grpc.model.Event",
                },
              ],
            },
            {
              name: "Inventory",
              method: [
                {
                  name: "Watch",
                  inputType: ".grpc.model.Request",
                  outputType: ".grpc.model.Event",
                  serverStreaming: true,
                },
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
);

const RequestSchema = messageDesc<RequestMessage>(descriptorFile, 0);
const EventSchema = messageDesc<EventMessage>(descriptorFile, 1);
const OrdersService = serviceDesc<{
  readonly stream: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
  readonly get: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "unary";
  };
}>(descriptorFile, 0);
const InventoryService = serviceDesc<{
  readonly watch: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
}>(descriptorFile, 1);

const invoke = <Owner extends object, Input>(
  owner: Owner,
  method: string,
  input: Input,
): unknown => {
  const selected = Reflect.get(owner, method);
  if (typeof selected !== "function") {
    throw new TypeError(`Expected ${method} to be a function.`);
  }
  return Reflect.apply(selected, owner, [input]);
};

describe("gRPC Source Definition contract", () => {
  it("captures exact generated descriptors and freezes pure definitions", () => {
    const descriptors = {
      orders: OrdersService,
    };
    const sources = grpc.topicSources(descriptors);
    let requestCalls = 0;
    const request = () => {
      requestCalls += 1;
      return { region: "eu" };
    };
    const map = ({ value }: { readonly value: EventMessage }) => ({
      id: value.id,
      region: value.region,
    });
    const materialized = sources.materialized({
      client: "orders",
      method: "stream",
      request,
      map,
    });
    const callerRouteBy: ["region"] = ["region"];
    const leased = sources.leased({
      client: "orders",
      method: "stream",
      routeBy: callerRouteBy,
      request: (route) => ({ region: route.region }),
      map: ({ value }) => ({
        id: value.id,
        region: value.region,
      }),
    });

    descriptors.orders = OrdersService;
    callerRouteBy.push("region");

    expect({
      callerRouteBy,
      callerRouteByFrozen: Object.isFrozen(callerRouteBy),
      helperFrozen: Object.isFrozen(sources),
      grpcFrozen: Object.isFrozen(grpc),
      materializedFrozen: Object.isFrozen(materialized),
      leasedFrozen: Object.isFrozen(leased),
      materializedLifecycle: materialized.lifecycle,
      leasedLifecycle: leased.lifecycle,
      routeBy: leased.routeBy,
      requestIdentity: Reflect.get(materialized.options, "request") === request,
      mapped: Reflect.apply(Reflect.get(materialized.options, "mapValue"), undefined, [
        { id: "one", region: "eu" },
      ]),
      requestCalls,
    }).toStrictEqual({
      callerRouteBy: ["region", "region"],
      callerRouteByFrozen: false,
      helperFrozen: true,
      grpcFrozen: true,
      materializedFrozen: true,
      leasedFrozen: true,
      materializedLifecycle: "materialized",
      leasedLifecycle: "leased",
      routeBy: ["region"],
      requestIdentity: false,
      mapped: { id: "one", region: "eu" },
      requestCalls: 0,
    });
    expect(
      Reflect.apply(Reflect.get(materialized.options, "request"), undefined, []),
    ).toStrictEqual({ region: "eu" });
    expect(requestCalls).toBe(1);
  });

  it("rejects malformed descriptor records and lifecycle inputs", () => {
    let accessorCalls = 0;
    const accessorDescriptors = {};
    Object.defineProperty(accessorDescriptors, "orders", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return OrdersService;
      },
    });
    const invalidDescriptorInputs: ReadonlyArray<unknown> = [
      {},
      [],
      null,
      new (class DescriptorRecord {
        readonly orders = OrdersService;
      })(),
      accessorDescriptors,
      new Proxy(
        {},
        {
          ownKeys: () => {
            throw new Error("descriptor proxy failure");
          },
        },
      ),
      { orders: {} },
      {
        orders: {
          kind: "service",
          name: "Orders",
          typeName: "grpc.model.Orders",
          file: descriptorFile,
          methods: OrdersService.methods,
          method: OrdersService.method,
          toString: OrdersService.toString,
        },
      },
      { "": OrdersService },
      { [Symbol.for("orders")]: OrdersService },
    ];
    for (const input of invalidDescriptorInputs) {
      expect(() => Reflect.apply(grpc.topicSources, grpc, [input])).toThrow(
        GrpcSourceConfigurationError,
      );
    }

    const sources = grpc.topicSources({
      inventory: InventoryService,
      orders: OrdersService,
    });
    const throwingLifecycleInput = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error("materialized input proxy failure");
        },
      },
    );
    const invalidMaterializedInputs: ReadonlyArray<unknown> = [
      null,
      {},
      throwingLifecycleInput,
      {
        client: "orders",
        method: "stream",
        request: () => ({ region: "eu" }),
        map: () => ({ id: "one", region: "eu" }),
        extra: true,
      },
      {
        client: "missing",
        method: "stream",
        request: () => ({ region: "eu" }),
        map: () => ({ id: "one", region: "eu" }),
      },
      {
        client: "orders",
        method: "get",
        request: () => ({ region: "eu" }),
        map: () => ({ id: "one", region: "eu" }),
      },
      {
        client: "orders",
        method: "watch",
        request: () => ({ region: "eu" }),
        map: () => ({ id: "one", region: "eu" }),
      },
      {
        client: "orders",
        method: "stream",
        request: "not-a-function",
        map: () => ({ id: "one", region: "eu" }),
      },
      {
        client: "orders",
        method: 1,
        request: () => ({ region: "eu" }),
        map: () => ({ id: "one", region: "eu" }),
      },
    ];
    for (const input of invalidMaterializedInputs) {
      expect(() => invoke(sources, "materialized", input)).toThrow(GrpcSourceConfigurationError);
    }
    const invalidLeasedInputs = invalidMaterializedInputs.map((input) =>
      typeof input === "object" && input !== null && input !== throwingLifecycleInput
        ? { ...input, routeBy: ["region"] }
        : input,
    );
    for (const input of invalidLeasedInputs) {
      expect(() => invoke(sources, "leased", input)).toThrow(GrpcSourceConfigurationError);
    }

    const leasedBase = {
      client: "orders",
      method: "stream",
      request: () => ({ region: "eu" }),
      map: () => ({ id: "one", region: "eu" }),
    };
    const invalidLengthRouteBy = new Proxy(["region"], {
      getOwnPropertyDescriptor: (target, key) =>
        key === "length"
          ? {
              configurable: false,
              enumerable: false,
              value: 1.5,
              writable: true,
            }
          : Reflect.getOwnPropertyDescriptor(target, key),
    });
    const extraRouteBy = ["region"];
    Object.defineProperty(extraRouteBy, "extra", {
      enumerable: true,
      value: "extra",
    });
    const throwingRouteBy = new Proxy(["region"], {
      ownKeys: () => {
        throw new Error("route proxy failure");
      },
    });
    for (const routeBy of [
      "region",
      [],
      [""],
      ["region", "region"],
      invalidLengthRouteBy,
      extraRouteBy,
      throwingRouteBy,
    ]) {
      expect(() => invoke(sources, "leased", { ...leasedBase, routeBy })).toThrow(
        GrpcSourceConfigurationError,
      );
    }
    const accessorRouteBy: Array<string> = [];
    Object.defineProperty(accessorRouteBy, "0", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return "region";
      },
    });
    Object.defineProperty(accessorRouteBy, "length", {
      value: 1,
    });
    expect(() => invoke(sources, "leased", { ...leasedBase, routeBy: accessorRouteBy })).toThrow(
      GrpcSourceConfigurationError,
    );
    expect(accessorCalls).toBe(0);

    let lifecyclePropertyReads = 0;
    const materializedInput = {
      client: "orders",
      method: "stream",
      request: () => ({ region: "eu" }),
      map: () => ({ id: "captured-materialized", region: "eu" }),
    };
    const materializedProxy = new Proxy(materializedInput, {
      get: () => {
        lifecyclePropertyReads += 1;
        throw new Error("validated Materialized input must not be read through property access");
      },
    });
    const capturedMaterialized = invoke(sources, "materialized", materializedProxy);
    const leasedRouteBy = ["region"];
    const leasedInput = {
      client: "orders",
      method: "stream",
      routeBy: leasedRouteBy,
      request: () => ({ region: "eu" }),
      map: () => ({ id: "captured-leased", region: "eu" }),
    };
    const leasedProxy = new Proxy(leasedInput, {
      get: () => {
        lifecyclePropertyReads += 1;
        throw new Error("validated Leased input must not be read through property access");
      },
    });
    const capturedLeased = invoke(sources, "leased", leasedProxy);
    leasedRouteBy[0] = "mutated";
    const capturedMaterializedOptions = Reflect.get(Object(capturedMaterialized), "options");
    const capturedLeasedOptions = Reflect.get(Object(capturedLeased), "options");
    expect({
      lifecyclePropertyReads,
      materializedClient: Reflect.get(Object(capturedMaterializedOptions), "client"),
      materializedMapped: Reflect.apply(
        Reflect.get(Object(capturedMaterializedOptions), "mapValue"),
        undefined,
        [{}],
      ),
      leasedClient: Reflect.get(Object(capturedLeasedOptions), "client"),
      leasedMapped: Reflect.apply(
        Reflect.get(Object(capturedLeasedOptions), "mapValue"),
        undefined,
        [{}, { region: "eu" }],
      ),
      leasedRouteBy: Reflect.get(Object(capturedLeased), "routeBy"),
    }).toStrictEqual({
      lifecyclePropertyReads: 0,
      materializedClient: "orders",
      materializedMapped: { id: "captured-materialized", region: "eu" },
      leasedClient: "orders",
      leasedMapped: { id: "captured-leased", region: "eu" },
      leasedRouteBy: ["region"],
    });

    const protoDescriptors = {};
    Object.defineProperty(protoDescriptors, "__proto__", {
      enumerable: true,
      value: OrdersService,
    });
    const protoSources = invoke(grpc, "topicSources", protoDescriptors);
    if (typeof protoSources !== "object" || protoSources === null) {
      throw new TypeError("Expected __proto__ client Source helpers.");
    }
    const protoSource = invoke(protoSources, "materialized", {
      client: "__proto__",
      method: "stream",
      request: () => ({ region: "eu" }),
      map: () => ({ id: "one", region: "eu" }),
    });
    if (typeof protoSource !== "object" || protoSource === null) {
      throw new TypeError("Expected __proto__ client Source Definition.");
    }
    const protoOptions = Reflect.get(protoSource, "options");
    expect({
      client:
        typeof protoOptions === "object" && protoOptions !== null
          ? Reflect.get(protoOptions, "client")
          : undefined,
      method:
        typeof protoOptions === "object" && protoOptions !== null
          ? Reflect.get(protoOptions, "method")
          : undefined,
    }).toStrictEqual({
      client: "__proto__",
      method: "stream",
    });
  });

  it("recognizes only exact generated services, methods, and definition options", () => {
    const malformedMethodService: Record<string, unknown> = {};
    const malformedMethod = {
      ...OrdersService.methods[0],
      parent: malformedMethodService,
      input: null,
    };
    Object.assign(malformedMethodService, OrdersService, {
      file: {
        ...descriptorFile,
        services: [malformedMethodService],
      },
      methods: [malformedMethod],
      method: {
        stream: malformedMethod,
      },
    });

    expect({
      service: isGrpcServiceDescriptor(OrdersService),
      nullService: isGrpcServiceDescriptor(null),
      objectService: isGrpcServiceDescriptor({ kind: "service", method: null }),
      malformedMethod: isGrpcServiceDescriptor(malformedMethodService),
      detachedService: isGrpcServiceDescriptor({
        ...OrdersService,
        file: descriptorFile,
      }),
      mismatchedMethods: isGrpcServiceDescriptor({
        ...OrdersService,
        methods: [],
      }),
      primitiveMethod: isGrpcServiceDescriptor({
        ...OrdersService,
        methods: [42],
        method: { stream: 42 },
      }),
      stream: selectedGrpcMethod(OrdersService, "stream")?.methodKind,
      unary: selectedGrpcMethod(OrdersService, "get"),
      missing: selectedGrpcMethod(OrdersService, "missing"),
      materializedOptions: isGrpcSourceDefinitionOptions(
        grpc.topicSources({ orders: OrdersService }).materialized({
          client: "orders",
          method: "stream",
          request: () => ({ region: "eu" }),
          map: ({ value }) => ({ id: value.id, region: value.region }),
        }).options,
      ),
      invalidOptions: isGrpcSourceDefinitionOptions({
        client: "orders",
        method: "stream",
        request: () => ({}),
        mapValue: () => ({}),
        service: () => OrdersService,
        extra: true,
      }),
      invalidOptionFields: [
        {
          client: null,
          mapValue: () => ({}),
          method: "stream",
          request: () => ({}),
          service: () => OrdersService,
        },
        {
          client: "orders",
          mapValue: null,
          method: "stream",
          request: () => ({}),
          service: () => OrdersService,
        },
        {
          client: "orders",
          mapValue: () => ({}),
          method: null,
          request: () => ({}),
          service: () => OrdersService,
        },
        {
          client: "orders",
          mapValue: () => ({}),
          method: "stream",
          request: null,
          service: () => OrdersService,
        },
        {
          client: "orders",
          mapValue: () => ({}),
          method: "stream",
          request: () => ({}),
          service: null,
        },
      ].map(isGrpcSourceDefinitionOptions),
    }).toStrictEqual({
      service: true,
      nullService: false,
      objectService: false,
      malformedMethod: false,
      detachedService: false,
      mismatchedMethods: false,
      primitiveMethod: false,
      stream: "server_streaming",
      unary: undefined,
      missing: undefined,
      materializedOptions: true,
      invalidOptions: false,
      invalidOptionFields: [false, false, false, false, false],
    });
  });
});
