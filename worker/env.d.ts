/**
 * Secrets are not declared in wrangler.jsonc, so `wrangler types` cannot
 * generate them. Declaration merging adds them to the global Env interface,
 * which means running `npm run cf-typegen` will not drop them again.
 *
 *   npx wrangler secret put CH_PASSWORD
 *   npx wrangler secret put SUPABASE_DB_URL
 *
 * SUPABASE_DB_URL is the fallback used until the DROP_DB Hyperdrive binding
 * exists; db.ts prefers the binding when it is there.
 */
interface Env {
	CH_PASSWORD: string;
	SUPABASE_DB_URL?: string;
}
