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

const invoke = (owner: object, method: string, input: unknown): unknown => {
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
    expect(typeof protoSource).toBe("object");
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
        "materialized",
      ),
      invalidOptions: isGrpcSourceDefinitionOptions(
        {
          client: "orders",
          method: "stream",
          request: () => ({}),
          mapValue: () => ({}),
          service: () => OrdersService,
          extra: true,
        },
        "leased",
      ),
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
      ].map((options) => isGrpcSourceDefinitionOptions(options, "materialized")),
    }).toStrictEqual({
      service: true,
      nullService: false,
      objectService: false,
      malformedMethod: false,
      detachedService: false,
      mismatchedMethods: false,
      stream: "server_streaming",
      unary: undefined,
      missing: undefined,
      materializedOptions: true,
      invalidOptions: false,
      invalidOptionFields: [false, false, false, false, false],
    });
  });
});
