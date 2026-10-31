export const workflow = async (input: unknown) => {
  if (input === null || Array.isArray(input) || typeof input !== "object")
    throw new Error("fixture input must be an object")
  return { accepted: true, input }
}
