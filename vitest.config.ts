import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
	plugins: [
		cloudflareTest({
			wrangler: { configPath: "./wrangler.jsonc" },
			// Cron B refuses to upload without a key. The upload step itself is
			// mocked in the tests; this only gets the run past that check.
			miniflare: { bindings: { DROP_API_KEY: "test-key" } },
		}),
	],
});
