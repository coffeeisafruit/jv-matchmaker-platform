#!/usr/bin/env python
"""Export the Vadim Voss static hub (index.html) for GitHub Pages — client-facing only.

Generates index.html with links to Client Profile and Partner Outreach only.
Site Analytics is internal-only and must not appear on Vadim's page; it lives
on the internal architecture page (architecture_diagram.html) instead.

Deploying vadim-voss-profile:
  1. Run this script to generate index.html (no Site Analytics link).
  2. Run export_vadim_profile.py and export_static_outreach.py for profile.html and outreach.html.
  3. Do not deploy architecture.html (or any "Site Analytics" page) to the client-facing site.
  4. Remove any existing architecture.html from the vadim-voss-profile repo so Vadim cannot see analytics.

Usage:
    python scripts/export_vadim_hub.py --output /path/to/vadim-voss-profile/index.html
"""
import argparse
import os

HUB_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Vadim Voss · Partner Report</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=Playfair+Display:wght@500;600&display=swap" rel="stylesheet">
    <style>
        :root { --cream: #faf8f5; --ink: #1a1a1a; --forest: #1e3a2f; --gold: #c9a962; --muted: #8b8680; --card: #ffffff; }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'DM Sans', -apple-system, sans-serif; background: var(--cream); color: var(--ink); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; }
        .container { max-width: 400px; width: 100%; }
        h1 { font-family: 'Playfair Display', Georgia, serif; font-size: 28px; font-weight: 500; color: var(--forest); text-align: center; margin-bottom: 8px; }
        .subtitle { text-align: center; color: var(--muted); font-size: 14px; margin-bottom: 32px; }
        .nav-list { list-style: none; }
        .nav-card { background: var(--card); border-radius: 8px; padding: 20px 24px; margin-bottom: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); transition: box-shadow 0.2s, transform 0.2s; }
        .nav-card:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.08); transform: translateY(-2px); }
        .nav-card a { text-decoration: none; color: inherit; display: block; }
        .nav-card .name { font-size: 18px; font-weight: 600; color: var(--forest); margin-bottom: 4px; }
        .nav-card .desc { font-size: 14px; color: var(--muted); }
    </style>
</head>
<body>
    <div class="container">
        <h1>Vadim Voss</h1>
        <p class="subtitle">Vadim Voss · Partner report</p>
        <ul class="nav-list">
            <li class="nav-card">
                <a href="profile.html">
                    <div class="name">Client Profile</div>
                    <div class="desc">Offer details and matching criteria</div>
                </a>
            </li>
            <li class="nav-card">
                <a href="outreach.html">
                    <div class="name">Partner Outreach</div>
                    <div class="desc">Your top JV partner matches with contact info</div>
                </a>
            </li>
        </ul>
    </div>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(
        description="Export Vadim's static hub (index.html) without Site Analytics"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path for index.html (e.g. path/to/vadim-voss-profile/index.html)",
    )
    args = parser.parse_args()

    out_path = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as f:
        f.write(HUB_HTML)

    print(f"Exported hub to {out_path}")
    print("  Cards: Client Profile, Partner Outreach (no Site Analytics — internal only)")


if __name__ == "__main__":
    main()
