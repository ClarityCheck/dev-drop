// Secrets are not declared in wrangler.jsonc, so `wrangler types` cannot see
// them. Declare them here so the Worker and Workflow type-check.
declare namespace Cloudflare {
	interface Env {
		CH_PASSWORD: string;
	}
}
