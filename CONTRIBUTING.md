# Contributing to Igloo

We welcome contributions to Igloo! Whether it's bug fixes, feature enhancements, or documentation improvements, your help is appreciated.

## Getting Started

- If you have a larger feature in mind, please open an issue first to discuss the design and approach.
- For smaller changes, feel free to submit a pull request directly.

## Development Workflow

1.  Fork the repository.
2.  Create a new branch for your changes (e.g., `feat/my-new-feature` or `fix/issue-123`).
3.  Make your changes.
4.  Ensure your code adheres to the project's style and quality standards (see below).
5.  Commit your changes with clear and descriptive commit messages.
6.  Push your branch to your fork.
7.  Open a pull request against the main Igloo repository.

## Code Style and Quality

To maintain code quality and consistency, we use Rust's standard tooling:

- **Formatting:** Code is formatted using `rustfmt`. Before submitting, please ensure your code is formatted by running `cargo fmt --all`.
- **Linting:** We use `clippy` for linting. Please ensure your code is free of clippy warnings by running `cargo clippy --all-targets --all-features -- -D warnings`.

Our Continuous Integration (CI) pipeline automatically checks for formatting and linting issues. Pull requests that do not pass these checks will not be merged.

## Questions?

Feel free to open an issue if you have any questions or need clarification.
