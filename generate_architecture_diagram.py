import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Set up figure
fig, ax = plt.subplots(figsize=(24, 13.5), dpi=300)
fig.patch.set_facecolor('#0A1119')
ax.set_facecolor('#0A1119')
ax.set_xlim(0, 24)
ax.set_ylim(0, 13.5)
ax.axis('off')

# Color palette
C_BG = '#0A1119'
C_CARD = '#111B26'
C_CARD_ALT = '#14202E'
C_BORDER = '#243243'
C_BORDER_ACTIVE = '#3A4D66'
C_BLUE = '#1F6FEB'
C_GREEN = '#3FCF8E'
C_AMBER = '#E3A13A'
C_RED = '#FF6B5E'
C_PURPLE = '#A78BFA'
C_TEXT_HEAD = '#EEF3F8'
C_TEXT_BODY = '#CDD7E3'
C_TEXT_MUTED = '#93A0B4'

def draw_card(ax, x, y, w, h, title='', subtitle='', bg=C_CARD, border=C_BORDER, radius=0.3):
    box = patches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        facecolor=bg, edgecolor=border, linewidth=1.5, zorder=2
    )
    ax.add_patch(box)
    if title:
        ax.text(x + 0.4, y + h - 0.5, title, fontsize=12, fontweight='bold', color=C_TEXT_HEAD, zorder=3)
    if subtitle:
        ax.text(x + 0.4, y + h - 0.85, subtitle, fontsize=9.5, color=C_TEXT_MUTED, zorder=3)

def draw_pill(ax, x, y, w, h, text, color=C_BLUE, bg_alpha=0.15):
    pill = patches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0,rounding_size=0.15",
        facecolor=color, alpha=bg_alpha, edgecolor=color, linewidth=1, zorder=3
    )
    ax.add_patch(pill)
    ax.text(x + w/2, y + h/2, text, fontsize=9, fontweight='bold', color=color,
            ha='center', va='center', zorder=4)

def draw_arrow(ax, x1, y1, x2, y2, label='', color=C_BLUE, curve=0.0, dashed=False, label_offset=(0, 0.25)):
    style = '->'
    linestyle = '--' if dashed else '-'
    conn = f"arc3,rad={curve}"
    ax.annotate(
        '', xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(
            arrowstyle="-|>", color=color, lw=2,
            linestyle=linestyle,
            connectionstyle=conn,
            mutation_scale=15
        ), zorder=5
    )
    if label:
        mx = (x1 + x2) / 2 + label_offset[0]
        my = (y1 + y2) / 2 + label_offset[1]
        ax.text(
            mx, my, label, fontsize=8.5, fontweight='bold', color=color,
            ha='center', va='center',
            bbox=dict(boxstyle="round,pad=0.2", facecolor=C_BG, edgecolor=color, alpha=0.9, lw=0.8),
            zorder=6
        )

# ==================== HEADER ====================
ax.text(1.0, 12.7, "Multi-Tenant Aggregation Layer", fontsize=22, fontweight='bold', color=C_TEXT_HEAD)
ax.text(1.0, 12.3, "High-Throughput Sub-Millisecond Architecture & Real-Time Incremental Sync", fontsize=11, color=C_TEXT_MUTED)

# Status badges
draw_pill(ax, 16.5, 12.4, 2.2, 0.45, "LATENCY: < 2ms", C_GREEN)
draw_pill(ax, 19.0, 12.4, 2.3, 0.45, "FRESHNESS: < 10s", C_BLUE)
draw_pill(ax, 21.6, 12.4, 1.8, 0.45, "ZERO LEAKAGE", C_PURPLE)

# Header separator
ax.plot([1.0, 23.4], [12.0, 12.0], color=C_BORDER, lw=1)

# ==================== LAYER 1: CLIENTS & TRAFFIC ====================
draw_card(ax, 0.8, 4.0, 3.4, 7.5, "1. INGESTION & CLIENTS", "Continuous Multi-Tenant Traffic", bg='#0E1724')

# Simulator block
draw_card(ax, 1.1, 8.5, 2.8, 2.6, "", "", bg=C_CARD_ALT, border='#2E4057')
ax.text(1.3, 10.6, "Live Traffic Simulator", fontsize=10.5, fontweight='bold', color=C_AMBER)
ax.text(1.3, 10.2, "app.simulate --rate 50", fontsize=9, fontfamily='monospace', color=C_TEXT_MUTED)
ax.text(1.3, 9.6, "• 50 new orders/sec\n• ~25 shipped status updates\n• ~10 cancel updates\n• Monotonic load generator", fontsize=8.5, color=C_TEXT_BODY)

# Web & Store clients
draw_card(ax, 1.1, 4.5, 2.8, 3.5, "", "", bg=C_CARD_ALT, border='#2E4057')
ax.text(1.3, 7.5, "Multi-Tenant Sources", fontsize=10.5, fontweight='bold', color=C_BLUE)
ax.text(1.3, 6.9, "• Tenant 1 ... Tenant 10\n• Raw CSV Seed Data (1M-10M)\n• API Webhooks & POS\n• High concurrent writes", fontsize=8.5, color=C_TEXT_BODY)
draw_pill(ax, 1.3, 4.8, 2.4, 0.4, "TENANTS 1 TO 10", C_BLUE)

# ==================== LAYER 2: PRIMARY DATABASE ====================
draw_card(ax, 4.8, 4.0, 4.4, 7.5, "2. POSTGRES BASE STORE", "Source of Truth (10M+ Raw Orders)", bg='#0E1724')

# Orders table
draw_card(ax, 5.1, 7.8, 3.8, 3.3, "", "", bg=C_CARD_ALT, border='#2E4057')
ax.text(5.3, 10.6, "orders Base Table", fontsize=11, fontweight='bold', color=C_TEXT_HEAD)
ax.text(5.3, 10.2, "Raw unaggregated transaction store", fontsize=8.5, color=C_TEXT_MUTED)
ax.text(5.3, 9.6, "Columns:\n• order_id, tenant_id, customer_id\n• product_id, category, quantity, amount\n• status (placed / shipped / cancelled)\n• created_at, updated_at", fontsize=8, color=C_TEXT_BODY)
draw_pill(ax, 5.3, 8.1, 3.4, 0.4, "NEVER ACCESSED BY QUERY API", C_RED)

# Trigger box
draw_card(ax, 5.1, 4.5, 3.8, 2.8, "", "", bg='#18212D', border=C_AMBER)
ax.text(5.3, 6.8, "PostgreSQL Monotonic Trigger", fontsize=10.5, fontweight='bold', color=C_AMBER)
ax.text(5.3, 6.4, "Clock-Independent Change Tracking", fontsize=8.5, color=C_TEXT_MUTED)
ax.text(5.3, 5.8, "• Stamps change_id sequence on each write\n• Survives clock skews / TZ differences\n• 100% loss-less watermark tracking", fontsize=8, color=C_TEXT_BODY)
draw_pill(ax, 5.3, 4.8, 3.4, 0.4, "change_id > seq (Monotonic)", C_AMBER)

# ==================== LAYER 3: REFRESH WORKER ====================
draw_card(ax, 9.8, 7.0, 4.4, 4.5, "3. INCREMENTAL WORKER", "Rollup Engine (app.refresh)", bg='#0E1724')

draw_card(ax, 10.1, 7.3, 3.8, 3.8, "", "", bg='#12241F', border=C_GREEN)
ax.text(10.3, 10.6, "Incremental Refresh Loop", fontsize=11, fontweight='bold', color=C_GREEN)
ax.text(10.3, 10.2, "Runs every REFRESH_INTERVAL_SEC (10s)", fontsize=8.5, color=C_TEXT_MUTED)
ax.text(10.3, 9.4, "1. Reads change_id > seq watermark\n2. Identifies dirty (tenant_id, day) sets\n3. Full recompute of ONLY dirty buckets\n4. Zero delta-drift on late status updates\n5. Updates refresh_state cursor\n6. Invalidates Redis cache generation", fontsize=8.5, color=C_TEXT_BODY)
draw_pill(ax, 10.3, 7.6, 3.4, 0.4, "COST = O(changed rows)", C_GREEN)

# Refresh state cursor table
draw_card(ax, 9.8, 4.0, 4.4, 2.6, "refresh_state Cursor", "Single-Row Atomic Bookkeeping", bg=C_CARD_ALT)
ax.text(10.1, 5.8, "• id: 1 (singleton lock-safe)\n• seq: last consumed change_id\n• rows_last_run & max_updated\n• Powers GET /freshness live telemetry", fontsize=8.5, color=C_TEXT_BODY)
draw_pill(ax, 10.1, 4.3, 3.8, 0.4, "EXPOSES LIVE FRESHNESS STATUS", C_BLUE)

# ==================== LAYER 4: PRE-COMPUTED ROLLUPS ====================
draw_card(ax, 14.8, 4.0, 4.2, 7.5, "4. ROLLUP STORE (DB SAFE)", "Primary Query Target (< 2ms)", bg='#0E1724')

# Daily rollup
draw_card(ax, 15.1, 8.8, 3.6, 2.3, "", "", bg=C_CARD_ALT, border='#2E4057')
ax.text(15.3, 10.6, "rollup_daily", fontsize=11, fontweight='bold', color=C_BLUE)
ax.text(15.3, 10.2, "Grain: (tenant_id, day, status, category)", fontsize=8, color=C_TEXT_MUTED)
ax.text(15.3, 9.5, "• order_count, total_amount, total_qty\n• Powers FR-1 (counts) & FR-2 (trend)\n• Powers FR-4 (status/category)", fontsize=8, color=C_TEXT_BODY)

# Product rollup
draw_card(ax, 15.1, 6.2, 3.6, 2.2, "", "", bg=C_CARD_ALT, border='#2E4057')
ax.text(15.3, 8.0, "rollup_product_daily", fontsize=11, fontweight='bold', color=C_BLUE)
ax.text(15.3, 7.6, "Grain: (tenant_id, day, product_id)", fontsize=8, color=C_TEXT_MUTED)
ax.text(15.3, 6.9, "• product_name, revenue, units, orders\n• Powers FR-3 Top-N Products", fontsize=8, color=C_TEXT_BODY)

# Metric config
draw_card(ax, 15.1, 4.3, 3.6, 1.6, "", "", bg=C_CARD_ALT, border=C_PURPLE)
ax.text(15.3, 5.4, "metric_config (Bonus B-4)", fontsize=10, fontweight='bold', color=C_PURPLE)
ax.text(15.3, 4.7, "Config-driven SQL expressions:\navg_order_value, cancelled_rate", fontsize=8, color=C_TEXT_BODY)

# ==================== LAYER 5: SERVING & DASHBOARD ====================
draw_card(ax, 19.6, 4.0, 3.8, 7.5, "5. API & DASHBOARD", "FastAPI + Real-Time UI", bg='#0E1724')

# Redis cache
draw_card(ax, 19.9, 9.8, 3.2, 1.3, "", "", bg='#261517', border=C_RED)
ax.text(20.1, 10.7, "Redis 7 Cache (TTL 30s)", fontsize=10, fontweight='bold', color=C_RED)
ax.text(20.1, 10.1, "Key: {cache_gen}:cnt:{tenant}:{from}:{to}\nInstant invalidation via rcache.incr()", fontsize=7.5, color=C_TEXT_BODY)

# FastAPI
draw_card(ax, 19.9, 7.0, 3.2, 2.5, "", "", bg=C_CARD_ALT, border='#2E4057')
ax.text(20.1, 9.1, "FastAPI Service (:8000)", fontsize=10.5, fontweight='bold', color=C_BLUE)
ax.text(20.1, 8.6, "• GET /tenants/{id}/orders/count\n• GET /tenants/{id}/orders/trend\n• GET /tenants/{id}/products/top\n• GET /tenants/{id}/orders/breakdown\n• GET /tenants/{id}/metrics/{name}\n• GET /freshness & GET /stats", fontsize=7.5, color=C_TEXT_BODY)

# Dashboard UI
draw_card(ax, 19.9, 4.3, 3.2, 2.4, "", "", bg='#122030', border=C_BLUE)
ax.text(20.1, 6.3, "Dashboard (/dashboard)", fontsize=10.5, fontweight='bold', color=C_TEXT_HEAD)
ax.text(20.1, 5.8, "• 4 Big KPI Cards (₹, Orders, Units, AOV)\n• Chart.js Trend, Top-10 & Breakdown\n• Full Dark & Full Light Theme Toggle\n• 2-Second Live Auto-Sync on writes", fontsize=7.5, color=C_TEXT_BODY)
draw_pill(ax, 20.1, 4.6, 2.8, 0.35, "LIVE AUTO-SYNC ACTIVE", C_GREEN)

# ==================== CONNECTING ARROWS ====================
# 1. Ingestion -> orders table
draw_arrow(ax, 4.2, 9.5, 5.1, 9.5, "Writes (INSERT/UPDATE/CANCEL)", C_AMBER, curve=0.0, label_offset=(0, 0.25))

# 2. Orders table -> Trigger
draw_arrow(ax, 7.0, 7.8, 7.0, 7.3, "Trigger Fire", C_AMBER)

# 3. Trigger -> Worker
draw_arrow(ax, 8.9, 5.8, 10.1, 8.5, "change_id > seq", C_AMBER, curve=-0.15, label_offset=(0.2, 0.4))

# 4. Worker reads base table dirty buckets
draw_arrow(ax, 10.1, 9.5, 8.9, 9.5, "Recompute Dirty Buckets", C_GREEN, curve=0.0, dashed=True, label_offset=(0, 0.25))

# 5. Worker -> Rollups
draw_arrow(ax, 13.9, 9.0, 15.1, 9.0, "Atomic Upsert", C_GREEN)

# 6. Worker -> refresh_state
draw_arrow(ax, 12.0, 7.3, 12.0, 6.6, "Advance seq Watermark", C_GREEN)

# 7. Worker -> Redis
draw_arrow(ax, 14.2, 10.5, 19.9, 10.5, "Incr cache_gen (Instant Invalidation)", C_RED, curve=-0.15, label_offset=(-0.8, 0.3))

# 8. API -> Rollups
draw_arrow(ax, 19.9, 8.0, 18.7, 8.0, "Sub-ms Rollup Queries", C_BLUE, dashed=False, label_offset=(0, 0.25))

# 9. API -> Redis
draw_arrow(ax, 21.5, 9.5, 21.5, 9.8, "Cache Lookup", C_RED)

# 10. API -> Dashboard
draw_arrow(ax, 21.5, 7.0, 21.5, 6.7, "JSON Response", C_BLUE)

# 11. Dashboard -> Freshness Poller
draw_arrow(ax, 19.9, 5.0, 14.2, 5.0, "Poll /freshness every 2s", C_GREEN, curve=0.0, dashed=True, label_offset=(0, 0.25))

# ==================== BOTTOM FEATURE HIGHLIGHT FOOTER ====================
draw_card(ax, 0.8, 0.5, 22.6, 3.0, "PRODUCTION HIGHLIGHTS & ARCHITECTURAL GUARANTEES", "", bg=C_CARD, border=C_BORDER)

cols = [
    ("1. SUB-SECOND SPEED", "Reads ONLY pre-aggregated rollups.\nNever touches base orders table.\nMeasured latency: 1-2 ms consistently\neven with 10M+ rows loaded."),
    ("2. CONTINUOUS FRESHNESS", "Incremental worker runs every 10s.\nTrigger stamps monotonic change_id.\nZero dependency on system clock.\nDashboard syncs live in 2 seconds."),
    ("3. 100% CORRECTNESS", "Dirty buckets recomputed in full.\nHandles placed -> shipped -> cancelled\nwithout delta drift or double counts.\nValidated by ./verify.sh harness."),
    ("4. STRICT ISOLATION", "Tenant ID scoped in URL path parameter.\nRollups partitioned by tenant_id.\nMulti-tenant concurrency limiter (B-3)\nprevents noisy-neighbor starvation.")
]

for idx, (title, desc) in enumerate(cols):
    bx = 1.2 + idx * 5.6
    draw_card(ax, bx, 0.7, 5.3, 2.2, "", "", bg='#0D1520', border='#243447')
    ax.text(bx + 0.3, 2.4, title, fontsize=10, fontweight='bold', color=C_BLUE)
    ax.text(bx + 0.3, 1.5, desc, fontsize=8, color=C_TEXT_BODY, linespacing=1.35)

plt.subplots_adjust(left=0.01, right=0.99, top=0.98, bottom=0.02)
plt.savefig("system_architecture.png", facecolor='#0A1119', edgecolor='none', dpi=300)
plt.close(fig)
print("System architecture diagram saved as system_architecture.png successfully!")
