# COA Directory Static Site

## Files
- `index.html`
- `styles.css`
- `app.js`
- `coas.json`

## How it works
This is a plain static site. `app.js` fetches `./coas.json`, flattens the nested category/product structure, and renders searchable product cards.

## Cloudflare Pages
Upload the contents of this folder as your static site.
If your PDF files live at paths like `/coas/flower/...` and `/coas/beverages/...`, the "Open COA" links will work as-is.

## Important
If you later regenerate your nested JSON, replace `coas.json` with the new file.
