const codes = new Set(["client_bundle_options_invalid", "client_bundle_output_invalid", "client_bundle_source_invalid", "client_bundle_tool_invalid", "client_bundle_package_invalid", "client_bundle_input_invalid", "client_bundle_compile_failed", "client_bundle_pin_invalid"])

export class ClientBundleError extends Error {
  constructor(code) {
    super(codes.has(code) ? code : "client_bundle_options_invalid")
    this.name = "ClientBundleError"
  }
}
