export const workflow = async (input: unknown) => ({
  input,
  kind: input === null ? "null" : Array.isArray(input) ? "array" : typeof input,
})
