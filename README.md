# Lazy Lemon Documentation Setup

This directory contains the complete documentation for Lazy Lemon,
written in **reStructuredText (RST)** and rendered using **Sphinx**.

## Quick Start

### Prerequisites

-   Python 3.11 or later
-   pip or poetry

### Installation

    # Install dependencies
    cd docs
    pip install -r requirements.txt

    # Or with make
    make install

### Build Documentation

    # Build HTML documentation
    make html

    # View in browser
    open _build/html/index.html

    # Or on Linux
    xdg-open _build/html/index.html

### Development

    # Clean build directory
    make clean

    # Rebuild
    make html

    # Auto-rebuild on changes (requires sphinx-autobuild)
    pip install sphinx-autobuild
    make watch

## Documentation Structure

    docs/
    ├── conf.py                          # Sphinx configuration
    ├── index.rst                        # Main documentation page
    ├── requirements.txt                 # Python dependencies
    ├── Makefile                         # Build commands
    │
    ├── getting-started/                 # Getting started guide
    │   ├── installation.rst
    │   ├── quick-start.rst
    │   ├── configuration-basics.rst
    │   └── concepts.rst
    │
    ├── tutorials/                       # Step-by-step tutorials
    │   ├── 01-simple-file-watcher.rst
    │   ├── 02-adding-metadata.rst
    │   ├── 03-custom-job-builder.rst
    │   ├── 04-bash-dispatcher.rst
    │   ├── 05-geoips-workflow-dispatcher.rst
    │   ├── 06-multi-satellite-monitor.rst
    │   ├── 07-monitoring-with-prometheus.rst
    │   ├── 08-production-deployment.rst
    │   ├── 09-error-handling.rst
    │   └── 10-testing-plugins.rst
    │
    ├── user-guide/                      # User documentation
    │   ├── architecture.rst
    │   ├── services.rst
    │   ├── plugins.rst
    │   ├── configuration.rst
    │   ├── metadata-matching.rst
    │   ├── monitoring.rst
    │   ├── deployment.rst
    │   └── troubleshooting.rst
    │
    ├── developer-guide/                 # Developer documentation
    │   ├── architecture-deep-dive.rst
    │   ├── plugin-development.rst
    │   ├── testing.rst
    │   ├── contributing.rst
    │   ├── code-style.rst
    │   └── extending-interfaces.rst
    │
    ├── api-reference/                   # API documentation
    │   ├── service.rst
    │   ├── plugins.rst
    │   ├── types.rst
    │   ├── utils.rst
    │   └── interfaces.rst
    │
    └── reference/                       # Reference materials
        ├── configuration-schema.rst
        ├── plugin-catalog.rst
        ├── metrics-reference.rst
        ├── queue-reference.rst
        └── faq.rst

## Writing Documentation

### reStructuredText Format

All documentation is written in **reStructuredText (RST)**, the standard
format for Sphinx.

#### Headers

    Title (H1)
    ==========

    Section (H2)
    ------------

    Subsection (H3)
    ~~~~~~~~~~~~~~~

    Subsubsection (H4)
    ^^^^^^^^^^^^^^^^^^

#### Code Blocks

    .. code-block:: python

       def example():
           return "Hello, World!"

    .. code-block:: yaml

       apiVersion: geoips_driver/v1
       kind: Service

#### Admonitions

    .. note::

       This is a note admonition.

    .. warning::

       This is a warning.

    .. tip::

       This is a helpful tip.

#### Cross-References

    :doc:`path/to/document`           # Link to another document
    :ref:`section-label`              # Link to a section

#### Links

    `Link text <https://example.com>`_
    :doc:`installation`

#### Lists

    * Unordered item 1
    * Unordered item 2

    1. Ordered item 1
    2. Ordered item 2

#### Tables

    +---------+---------+---------+
    | Header1 | Header2 | Header3 |
    +=========+=========+=========+
    | Cell 1  | Cell 2  | Cell 3  |
    +---------+---------+---------+
    | Cell 4  | Cell 5  | Cell 6  |
    +---------+---------+---------+

#### Inline Code

    Use ``inline code`` for commands and variable names.

#### Table of Contents

    .. toctree::
       :maxdepth: 2
       :caption: Section Name

       file1
       file2
       file3

## Sphinx Theme

This documentation uses the **Read the Docs** theme
(`sphinx_rtd_theme`).

Theme options are configured in `conf.py`:

    html_theme_options = {
        'navigation_depth': 4,
        'collapse_navigation': False,
        'sticky_navigation': True,
    }

## Extensions

Installed Sphinx extensions:

-   **sphinx.ext.autodoc** - Auto-generate from docstrings
-   **sphinx.ext.napoleon** - Google/NumPy style docstrings
-   **sphinx.ext.viewcode** - Source code links
-   **sphinx\_copybutton** - Copy buttons on code blocks

## Custom Styling

Custom CSS is in `_static/custom.css` and includes:

-   Enhanced code block styling
-   Better admonition colors
-   Improved table formatting
-   Consistent heading styles

## Building for GitHub Pages

    # Build HTML
    make html

    # The output is in _build/html/

    # Deploy with ghp-import (install first: pip install ghp-import)
    ghp-import -n -p _build/html

## Troubleshooting

### Build Errors

**"No module named 'sphinx\_rtd\_theme'"**

    pip install -r requirements.txt

**"WARNING: document isn't included in any toctree"**

Make sure files are referenced in a `toctree` directive somewhere.

### Links Not Working

Make sure cross-references use proper RST syntax:

    :doc:`path/to/file`   # Correct

### Formatting Issues

Run the build with verbose output:

    sphinx-build -v . _build/html

## Best Practices

1.  **Use meaningful file names** - `installation.rst` not `install.rst`
2.  **Keep line length reasonable** - ~100 characters for better diffs
3.  **One sentence per line** - Makes diffs cleaner
4.  **Use relative links** - `` :doc:`relative/path ``\` not absolute
5.  **Include code examples** - Real, working examples
6.  **Test code blocks** - Ensure all examples actually work
7.  **Use admonitions** - Highlight important information
8.  **Cross-reference liberally** - Link to related documentation

## Contributing

When adding new documentation:

1.  Create the `.rst` file in the appropriate directory
2.  Add it to the relevant `toctree` in `index.rst` or parent file
3.  Build locally to verify: `make html`
4.  Check for warnings in the build output
5.  Preview in browser to ensure formatting is correct

## Support

-   **Sphinx Documentation**: <https://www.sphinx-doc.org/>
-   **RST Primer**:
    <https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html>
-   **RST Reference**: <https://docutils.sourceforge.io/rst.html>
