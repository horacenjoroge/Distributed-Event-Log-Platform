# Contributing to DistributedLog

Thank you for your interest in contributing to DistributedLog! This document provides guidelines and workflows for contributing.

## Development Setup

### Prerequisites

- Python 3.10 or higher
- Docker and Docker Compose
- Git

### Initial Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd "Distributed Event Log Platform"
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies:**
   ```bash
   make install-dev
   ```

4. **Generate Protocol Buffer files:**
   ```bash
   make proto
   ```

5. **Install pre-commit hooks:**
   ```bash
   pre-commit install
   ```

## Development Workflow

### Branching Strategy

We follow a feature branch workflow:

- `main` - Production-ready code
- `develop` - Integration branch for features
- `feat/*` - Feature branches
- `fix/*` - Bug fix branches
- `docs/*` - Documentation updates

### Creating a Feature Branch

```bash
git checkout -b feat/your-feature-name
```

### Making Changes

1. **Write code** following the project's coding standards
2. **Add tests** for new functionality
3. **Update documentation** as needed
4. **Run tests and linters:**
   ```bash
   make test
   make lint
   make format
   ```

### Committing Changes

We use conventional commits for clear, semantic commit messages:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```bash
git commit -m "feat(broker): implement basic broker server"
git commit -m "fix(replication): handle network partition scenarios"
git commit -m "docs(readme): add installation instructions"
```

### Code Quality Standards

#### Python Style Guide

- Follow PEP 8
- Use type hints for all function signatures
- Line length: 100 characters
- Use Black for formatting
- Use Ruff for linting

#### Testing

- Write unit tests for all new code
- Maintain test coverage above 80%
- Use pytest for testing
- Use hypothesis for property-based testing

#### Documentation

- Add docstrings to all public functions and classes
- Use Google-style docstrings
- Update README.md for user-facing changes
- Add inline comments for complex logic

### Running Tests

```bash
# Run all tests
make test

# Run specific test file
pytest distributedlog/tests/test_file.py

# Run with coverage
pytest --cov=distributedlog --cov-report=html
```

### Running Linters

```bash
# Run all linters
make lint

# Run specific linter
ruff check distributedlog/
mypy distributedlog/
```

### Formatting Code

```bash
# Format all code
make format

# Format specific file
black distributedlog/path/to/file.py
```

## Protocol Buffers

When modifying `.proto` files:

1. Update the proto definition
2. Regenerate Python code:
   ```bash
   make proto
   ```
3. Update related Python code
4. Test the changes

## Docker Development

### Building Images

```bash
make docker-build
```

### Starting Services

```bash
make docker-up
```

### Viewing Logs

```bash
make docker-logs
```

### Stopping Services

```bash
make docker-down
```

## Pull Request Process

1. **Update your branch** with the latest changes from `develop`:
   ```bash
   git fetch origin
   git rebase origin/develop
   ```

2. **Push your branch:**
   ```bash
   git push origin feat/your-feature-name
   ```

3. **Create a Pull Request** on GitHub with:
   - Clear title describing the change
   - Description of what changed and why
   - Link to related issues
   - Screenshots (if applicable)

4. **Address review feedback:**
   - Make requested changes
   - Push new commits
   - Respond to comments

5. **Merge** once approved (squash and merge preferred)

## Issue Reporting

### Bug Reports

Include:
- Clear description of the bug
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Python version, etc.)
- Error messages and logs

### Feature Requests

Include:
- Clear description of the feature
- Use case and motivation
- Proposed implementation (if any)
- Alternative solutions considered

## Code Review Guidelines

When reviewing code:

- Be constructive and respectful
- Focus on code quality, not personal preferences
- Test the changes locally when possible
- Check for edge cases and error handling
- Verify tests are adequate
- Ensure documentation is updated

## Questions or Help?

- Open an issue for discussion
- Check existing issues and documentation first
- Be specific about your question or problem

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT License).
