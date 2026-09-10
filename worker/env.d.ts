/**
 * Secrets are not declared in wrangler.jsonc, so `wrangler types` cannot
 * generate them. Declaration merging adds them to the global Env interface,
 * which means running `npm run cf-typegen` will not drop them again.
 *
 *   npx wrangler secret put CH_PASSWORD
 */
interface Env {
	CH_PASSWORD: string;
}
