/**
 * Secrets are not declared in wrangler.jsonc, so `wrangler types` cannot
 * generate them. Declaration merging adds them to the global Env interface,
 * which means running `npm run cf-typegen` will not drop them again.
 *
 *   npx wrangler secret put CH_PASSWORD
 *   npx wrangler secret put SUPABASE_URL
 *   npx wrangler secret put SUPABASE_ANON_KEY
 *   npx wrangler secret put SUPABASE_JWT_SECRET
 */
interface Env {
	CH_PASSWORD: string;
	SUPABASE_URL: string;
	SUPABASE_ANON_KEY: string;
	SUPABASE_JWT_SECRET: string;
}
