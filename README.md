# PROJECT


## Contributing

I welcome contributions to PROJECT!

Please follow these general guidelines:

1.  **Fork the repository** on GitHub.
2.  **Create a new branch** for your feature or bug fix. (eg. `git checkout -b feature/your-feature-name` or `fix/issue-number-description`)
3.  **Make your changes** and **ensure all linters and tests pass** (the CI will also check for you, but it's helpful to run them locally).
4.  **Commit your changes** using [Conventional Commits](https://www.conventionalcommits.org/) (see "Commit Messages" below).
5.  **Push your branch** to your fork.
6.  **Open a Pull Request** against the `main` (or `dev`) branch of the `biosafetylvl5/PROJECT` repository.
7.  Clearly describe your changes in the PR. If it fixes an open issue, please link to it (e.g., "Fixes #123").

If you're planning a larger contribution, it's a good idea to open an issue first to discuss your ideas so your work isn't duplicated/done in vain.

### Setting up Your Development Environment

This project uses [Poetry](https://python-poetry.org/) for dependency management and packaging.

#### Using Dev Containers (Recommended)

This repository is configured to use [VS Code Dev Containers](https://code.visualstudio.com/docs/devcontainers/containers). This is the recommended way to set up your development environment, as it ensures consistency and comes pre-configured with all necessary tools.

1.  Ensure you have Docker Desktop and the "Dev Containers" extension installed in VS Code.
2.  Clone the repository: `git clone https://github.com/biosafetylvl5/PROJECT.git`
3.  Open the cloned repository folder in VS Code. (You can also do this [without VS code](https://github.com/devcontainers/cli), but that's up to you to figure out.)
4.  VS Code should prompt you to "Reopen in Container". Click it.
    *   If it doesn't prompt, open the command palette (`Ctrl+Shift+P` or `Cmd+Shift+P`) and search for "Dev Containers: Reopen in Container".
5.  The dev container will build (this might take a few minutes the first time). Once built, VS Code will be connected to the containerized environment, with Python, Poetry, and all project dependencies (including development tools and plugins) already installed and configured.

#### Manual Setup

If you prefer not to use Dev Containers:

1.  **Install Python**: Ensure you have Python >=3.11 installed.
2.  **Install Poetry**: Follow the [official Poetry installation guide](https://python-poetry.org/docs/#installation).
3.  **Clone the repository**:
    ```bash
    git clone https://github.com/biosafetylvl5/PROJECT.git
    cd PROJECT
    ```
4.  **Install dependencies**: This command installs the project dependencies and all development tools (linters, test runners, etc.).
    ```bash
    poetry install --all-extras
    ```
    Alternatively, if you want more granular control, you can install specific groups:
    ```bash
    poetry install --with dev,lint,test,doc # For newer Poetry versions
    ```
5.  **Activate a virtual environment**:
    ```bash
    poetry shell
    ```

### Linters and Formatters

I use several tools to ensure code quality, consistency, and to catch potential errors early.

*   **Ruff**: An rust-based fast Python linter and formatter. It replaces Flake8, isort, pydocstyle, pyupgrade, and more (like wayy more). It's also used for formatting Python code and is Black-compatible.
*   **MyPy**: A static type checker for Python.
*   **Prettier**: For formatting YAML, JSON, and Markdown files.
*   **doc8 / pinkrst**: For linting and formatting reStructuredText (`.rst`) files for documentation.

#### Pre-commit Hooks (Highly Recommended)

The easiest way to ensure your code meets standards is by using the pre-commit hooks. These hooks will automatically run the linters and formatters on staged files before you make a commit.

1.  **Install pre-commit hooks** (if you're not using the Dev Container, which does this automatically):
    ```bash
    poetry run pre-commit install
    ```
    Now, `pre-commit` will run automatically on `git commit`. If any linters or formatters make changes, you'll need to `git add` those changes and commit again.

2.  **Run on all files manually** (optional):
    ```bash
    poetry run pre-commit run --all-files
    ```

#### Running Tools Manually

You can also run the tools manually:

*   **Ruff (Linting & Formatting Python)**:
    *   To format all Python files:
        ```bash
        poetry run ruff format .
        ```
    *   To check for linting errors (and auto-fix where possible):
        ```bash
        poetry run ruff check . --fix
        ```
*   **MyPy (Type Checking)**:
    ```bash
    poetry run mypy src tests
    ```
*   **Prettier (Formatting YAML, JSON, MD, etc.)**:
    *   To check formatting:
        ```bash
        poetry run prettier --check .
        ```
    *   To apply formatting:
        ```bash
        poetry run prettier --write .
        ```
*   **Doc Linting (reStructuredText)**:
    *   To lint `.rst` files (e.g., in the `docs` directory):
        ```bash
        poetry run doc8 docs
        ```
    *   To auto-format `.rst` files with `pinkrst` (e.g., in the `docs` directory):
        ```bash
        poetry run pinkrst docs
        ```

### Code Style

 *  **Line Length**: 88 characters (Black-compatible).
 *  **Quotes**: Double quotes (`"`) for strings.
 *  **Docstrings**: NumPy style docstrings (enforced by `ruff` via `pydocstyle`).
 *  **Type Hinting**: Please add type hints to your code. MyPy is configured with strict checks.

Refer to the `[tool.ruff]` and `[tool.mypy]` sections in `pyproject.toml` for detailed configurations.

### Running Tests

I use [pytest](https://docs.pytest.org/) for testing.

1.  **Run all tests**:
    ```bash
    poetry run pytest
    ```
    This will also generate a coverage report in the terminal and an XML report (`coverage.xml`).

2.  **Run tests with specific markers**:
    For example, to skip slow tests:
    ```bash
    poetry run pytest -m "not slow"
    ```

### Commit Messages

This project uses [Commitizen](https://commitizen-tools.github.io/commitizen/) with the `cz_conventional_commits` style. This helps create a more readable and structured commit history.

* **If using the Dev Container or after `poetry install --all-extras`**:
    You can use `cz c` or `git cz` (if hooks are set up) to be interactively guided through creating a commit message:
    ```bash
    poetry run cz commit
    # or simply if commitizen is on your PATH / using dev container
    # cz c
    ```
* **Manually**:
    Ensure your commit messages follow the [Conventional Commits specification](https://www.conventionalcommits.org/en/v1.0.0/).
    Example: `feat: add user authentication endpoint`
    Common types: `feat`, `fix`, `build`, `chore`, `ci`, `docs`, `style`, `refactor`, `perf`, `test`.

### Documentation

All substantial changes must be accompanied by a changelog release file. These can be created with [`brassy`](https://biosafetylvl5.github.io/brassy/) in `docs/source/release/latest` with `brassy -t $filename` or `brassy -t` to name the file after your current branch.

If your changes affect documentation or add new features that need documenting, please update the other documentation in the `docs/` directory. Documentation is built using Sphinx and brassy.

To build the documentation locally:
```bash
poetry install --extras doc # Ensure doc dependencies are installed
cd docs
poetry run make html
```

Then open `docs/_build/html/index.html` in your browser.
