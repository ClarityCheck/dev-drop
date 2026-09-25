import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
	plugins: [
		cloudflareTest({
			wrangler: { configPath: "./wrangler.jsonc" },
			// Test-only secrets. Cron B refuses to upload without an API key (the
			// upload step itself is mocked), and the start routes refuse without
			// the operator token.
			miniflare: { bindings: { DROP_API_KEY: "test-key", DROP_OPERATOR_TOKEN: "test-operator" } },
		}),
	],
});
