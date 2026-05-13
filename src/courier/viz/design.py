"""Design tokens and color palette for the Courier Viz TUI."""

# User-provided color palette
STEEL_BLUE = "#2081c3"
FROSTED_BLUE = "#84e6f8"
YELLOW = "#ffff39"
POWDER_BLUSH = "#f8ad9d"
PEACH_FUZZ = "#fbc4ab"

# Semantic color assignments
COLOR_HEADER = STEEL_BLUE
COLOR_SECONDARY = FROSTED_BLUE
COLOR_HIGHLIGHT = YELLOW
COLOR_WARNING = POWDER_BLUSH
COLOR_BACKGROUND = PEACH_FUZZ
COLOR_HEALTHY = "#00ff00"
COLOR_UNHEALTHY = "#ff3333"
COLOR_NEUTRAL = "#888888"

# Textual CSS color variables (used in app TCSS)
CSS_VARIABLES = {
    "steel-blue": STEEL_BLUE,
    "frosted-blue": FROSTED_BLUE,
    "yellow": YELLOW,
    "powder-blush": POWDER_BLUSH,
    "peach-fuzz": PEACH_FUZZ,
}

# Refresh rate presets (seconds)
REFRESH_RATES = [1, 2, 5, 10, 30]
DEFAULT_REFRESH = 5

# Layout dimensions
HEADER_HEIGHT = 3
FOOTER_HEIGHT = 1
