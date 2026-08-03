# Process strip title/body left-align (v20260731o)

## Issue
Step 3 title “Better-Aligned Decision” looked centered relative to its body (“Aligned participants…”) because `.oh-eco-inner` sets `text-align: center`, which titles inherited while body was forced left.

## Fix
Force `.oh-eco-step-h` (and body) to `text-align: left` and `justify-self: start`. Also zero number `margin-right` (column-gap already spaces).

## Staging CDN
`https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cf374c02f5ea0e442932c_dealality-old-home-ecosystem.v20260731o.css`
