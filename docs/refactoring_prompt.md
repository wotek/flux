# Refactoring Prompt: Rename "Saga" to "Workflow"

Please execute a complete, repository-wide refactoring to rename the concept of "Sagas" to "Workflows" in the `flux` framework.

Follow these exact steps in order, ensuring all unit tests and linting checks pass at the end.

## 1. Directory & File Renames
Move and rename the following files and directories using `git mv` (or your file system tools):
- Rename the root directory `saga/` to `workflow/`.
- Rename `workflow/saga.go` to `workflow/workflow.go`.
- Rename `example/e-commerce/internal/workflows/payment/saga.go` to `workflow.go`.
- Rename `example/e-commerce/internal/workflows/payment/saga_test.go` to `workflow_test.go`.

## 2. Codebase Find and Replace
Perform the following case-sensitive replacements across all `.go`, `.md`, and `.rules` files in the repository (excluding `.git` and `.github` folders):

**Target Strings:**
- `github.com/wotek/flux/saga` -> `github.com/wotek/flux/workflow`
- `SagaStore` -> `WorkflowStore`
- `PaymentSaga` -> `PaymentWorkflow`
- `OnboardingSaga` -> `OnboardingWorkflow`
- `dummySaga` -> `dummyWorkflow`
- `sagaInstance` -> `workflowInstance`
- `sagaState` -> `workflowState`
- `sagaCtx` -> `workflowCtx`
- `mySagaStore` -> `myWorkflowStore`

**Core Terms (Apply carefully to respect casing):**
- `Saga` -> `Workflow`
- `saga` -> `workflow`
- `Sagas` -> `Workflows`
- `sagas` -> `workflows`

*(Note: For the core terms, ensure you are matching word boundaries so you do not accidentally overwrite unrelated text, though "saga" is fairly unique).*

## 3. Package Declarations
Ensure that all Go files inside the newly renamed `workflow/` directory (and its `store/` subdirectories) now declare `package workflow` (or `package workflow_test` / `package store`) rather than `package saga`.

## 4. Documentation & Rules
Verify that the `README.md`, `docs/ARCHITECTURE.md`, `docs/PROJECT_LAYOUT.md`, and `.rules` files have had their terminology updated. 
- "Process Manager" / "Saga" should now read as "Workflow".

## 5. Verification
Once the refactoring is applied, run the following commands to ensure the codebase remains completely green:
```bash
go mod tidy
go test -v -race ./...
golangci-lint run
```
If any tests fail or there are compiler errors due to casing (e.g. `s *PaymentWorkflow` receiver variables), fix them before committing.
