import { defineRule } from "@oxlint/plugins";
import type { ESTree } from "@oxlint/plugins";

const FORBIDDEN_SYMBOL_NAME = "shape";

function containsForbiddenSymbolName(name: string): boolean {
  return name.toLowerCase().includes(FORBIDDEN_SYMBOL_NAME);
}

function staticPropertyName(key: ESTree.PropertyKey): string | null {
  if (key.type === "Identifier" || key.type === "PrivateIdentifier") return key.name;
  if (key.type === "Literal") return typeof key.value === "string" ? key.value : null;
  if (key.type !== "TemplateLiteral" || key.expressions.length !== 0) return null;
  const [quasi] = key.quasis;
  return quasi?.value.cooked ?? quasi?.value.raw ?? null;
}

type PropertyWithKey = ESTree.Node & { readonly key: ESTree.PropertyKey };

function isPropertyWithKey(node: ESTree.Node): node is PropertyWithKey {
  return (
    node.type === "Property" ||
    node.type === "MethodDefinition" ||
    node.type === "TSAbstractMethodDefinition" ||
    node.type === "PropertyDefinition" ||
    node.type === "TSAbstractPropertyDefinition" ||
    node.type === "TSPropertySignature" ||
    node.type === "TSMethodSignature"
  );
}

/** Ban the case-insensitive substring "shape" in every JavaScript and TypeScript symbol name. */
export const noForbiddenTermInSymbolNamesRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        'Disallow the case-insensitive substring "shape" in JavaScript, TypeScript, private, and JSX symbol names.',
    },
    messages: {
      forbiddenSymbolName:
        'Do not use the case-insensitive substring "shape" in symbol names (found "{{name}}").',
    },
  },
  create(context) {
    const reportForbiddenSymbolName = (node: ESTree.Node, name: string) => {
      if (!containsForbiddenSymbolName(name)) return;
      context.report({
        node,
        messageId: "forbiddenSymbolName",
        data: { name },
      });
    };

    const reportStaticPropertyName = (node: ESTree.Node) => {
      if (!isPropertyWithKey(node)) return;
      if (node.key.type === "Identifier" || node.key.type === "PrivateIdentifier") return;
      const name = staticPropertyName(node.key);
      if (name !== null) reportForbiddenSymbolName(node.key, name);
    };

    return {
      Identifier: (node) => reportForbiddenSymbolName(node, node.name),
      PrivateIdentifier: (node) => reportForbiddenSymbolName(node, node.name),
      JSXIdentifier: (node) => reportForbiddenSymbolName(node, node.name),
      Property: reportStaticPropertyName,
      MethodDefinition: reportStaticPropertyName,
      PropertyDefinition: reportStaticPropertyName,
      TSPropertySignature: reportStaticPropertyName,
      TSMethodSignature: reportStaticPropertyName,
    };
  },
});
